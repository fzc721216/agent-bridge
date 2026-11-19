// Package managed owns provider processes and their documented streaming transports.
// It never attaches to, resumes, or kills an independently started agent session.
package managed

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	bridge "github.com/fuzicheng/agent-bridge"
)

const maxText = 256 << 10

type Options struct {
	CodexBin  string
	ClaudeBin string
	StateDir  string
}

type StartRequest struct {
	Provider bridge.Provider `json:"provider"`
	CWD      string          `json:"cwd"`
	Name     string          `json:"name"`
	Prompt   string          `json:"prompt"`
}

// Event contains public agent output or transport state, never hidden reasoning.
type Event struct {
	SessionID string        `json:"session_id"`
	Type      string        `json:"type"`
	Text      string        `json:"text,omitempty"`
	Status    bridge.Status `json:"status,omitempty"`
	At        time.Time     `json:"at"`
}

type Manager struct {
	mu        sync.Mutex
	opts      Options
	ctx       context.Context
	cancel    context.CancelFunc
	closed    bool
	processes map[string]*process
	events    chan Event
}

type process struct {
	manager     *Manager
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	writerMu    sync.Mutex
	mu          sync.Mutex
	session     bridge.Session
	readyFlag   bool
	startupOnce sync.Once
	startup     chan error
	done        chan struct{}
	queue       chan string
	turnDone    chan struct{}
	awaiting    bool
	turnID      string
	requestID   int
	nextID      int
	turnBlocked bool
}

type wireMessage struct {
	ID                json.RawMessage   `json:"id"`
	Method            string            `json:"method"`
	Type              string            `json:"type"`
	Subtype           string            `json:"subtype"`
	Result            json.RawMessage   `json:"result"`
	Error             json.RawMessage   `json:"error"`
	Params            json.RawMessage   `json:"params"`
	Message           json.RawMessage   `json:"message"`
	IsError           bool              `json:"is_error"`
	SessionID         string            `json:"session_id"`
	ParentToolUseID   json.RawMessage   `json:"parent_tool_use_id"`
	PermissionDenials []json.RawMessage `json:"permission_denials"`
}

func New(opts Options) *Manager {
	if opts.CodexBin == "" {
		opts.CodexBin = "codex"
	}
	if opts.ClaudeBin == "" {
		opts.ClaudeBin = "claude"
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{opts: opts, ctx: ctx, cancel: cancel, processes: make(map[string]*process), events: make(chan Event, 128)}
}

// Events is best-effort public progress. Sessions remains authoritative if a consumer falls behind.
func (m *Manager) Events() <-chan Event { return m.events }

func (m *Manager) Start(ctx context.Context, req StartRequest) (bridge.Session, error) {
	if err := ctx.Err(); err != nil {
		return bridge.Session{}, err
	}
	if req.Provider != bridge.Codex && req.Provider != bridge.Claude {
		return bridge.Session{}, fmt.Errorf("unsupported provider %q", req.Provider)
	}
	if len(req.Prompt) > maxText {
		return bridge.Session{}, fmt.Errorf("prompt exceeds %d bytes", maxText)
	}
	cwd := req.CWD
	if cwd == "" {
		var err error
		cwd, err = os.Getwd()
		if err != nil {
			return bridge.Session{}, err
		}
	}
	cwd, err := filepath.Abs(cwd)
	if err != nil {
		return bridge.Session{}, err
	}
	info, err := os.Stat(cwd)
	if err != nil {
		return bridge.Session{}, fmt.Errorf("working directory: %w", err)
	}
	if !info.IsDir() {
		return bridge.Session{}, fmt.Errorf("working directory is not a directory")
	}
	native := bridge.NewID()
	bin := m.opts.CodexBin
	args := []string{"app-server", "--stdio"}
	if req.Provider == bridge.Claude {
		bin = m.opts.ClaudeBin
		args = []string{"--print", "--verbose", "--input-format", "stream-json", "--output-format", "stream-json", "--session-id", native, "--permission-prompts", "none"}
		if req.Name != "" {
			args = append(args, "--name", req.Name)
		}
	}
	cmd := exec.CommandContext(m.ctx, bin, args...)
	cmd.Dir = cwd
	cmd.Stderr = io.Discard // Do not copy credentials or provider debug logs to the broker event log.
	configureProcess(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return bridge.Session{}, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return bridge.Session{}, err
	}
	p := &process{manager: m, cmd: cmd, stdin: stdin, startup: make(chan error, 1), done: make(chan struct{}), queue: make(chan string, 64), turnDone: make(chan struct{}, 1), nextID: 3}
	p.session = bridge.Session{ID: string(req.Provider) + ":" + native, Provider: req.Provider, NativeID: native, Name: req.Name, CWD: cwd, Active: true, Status: bridge.Unknown, LastSeen: time.Now().UTC(), Capabilities: bridge.Capabilities{CanSend: true, CanReceiveEvents: true, CanCaptureOutput: true, Transport: bridge.Managed}}
	key := bridge.NewID()
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = stdin.Close()
		_ = stdout.Close()
		return bridge.Session{}, fmt.Errorf("managed manager is closed")
	}
	if err = cmd.Start(); err == nil {
		p.session.PID = cmd.Process.Pid
		m.processes[key] = p
	}
	m.mu.Unlock()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return bridge.Session{}, fmt.Errorf("start %s: %w", req.Provider, err)
	}
	readerDone := make(chan struct{})
	go func() { defer close(readerDone); p.read(stdout) }()
	go func() {
		<-readerDone
		err := cmd.Wait()
		p.mu.Lock()
		p.session.Active = false
		p.session.Status = bridge.Stale
		p.session.Capabilities.CanSend = false
		p.session.LastSeen = time.Now().UTC()
		if err != nil {
			p.session.Note = "managed provider exited; inspect provider configuration or permissions"
		} else {
			p.session.Note = "managed provider exited"
		}
		p.mu.Unlock()
		_ = stdin.Close()
		p.completeStartup(fmt.Errorf("%s exited before initialization completed", req.Provider))
		close(p.done)
		p.emit("exited", "", bridge.Stale)
	}()
	go p.sendLoop()
	if req.Provider == bridge.Codex {
		err = p.write(map[string]any{"id": 1, "method": "initialize", "params": map[string]any{"clientInfo": map[string]string{"name": "agent_bridge", "title": "Agent Bridge", "version": "0.1.0"}}})
		if err != nil {
			p.stop()
			return bridge.Session{}, err
		}
	} else {
		// Claude's native id is provided explicitly. Its system/init event is emitted only
		// once the first user input arrives; starting an idle process must not invoke a model.
		p.mu.Lock()
		p.session.Note = "waiting for the first input and provider initialization"
		p.mu.Unlock()
		p.completeStartup(nil)
	}
	startupCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	select {
	case <-startupCtx.Done():
		p.stop()
		return bridge.Session{}, fmt.Errorf("initialize %s: %w", req.Provider, startupCtx.Err())
	case err := <-p.startup:
		if err != nil {
			p.stop()
			return bridge.Session{}, err
		}
	}
	if req.Prompt != "" {
		if err = p.enqueue(ctx, req.Prompt); err != nil {
			p.stop()
			return bridge.Session{}, err
		}
	}
	return p.snapshot(), nil
}

func (m *Manager) Sessions() []bridge.Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]bridge.Session, 0, len(m.processes))
	for _, p := range m.processes {
		p.mu.Lock()
		if p.readyFlag {
			out = append(out, p.session)
		}
		p.mu.Unlock()
	}
	return out
}

func (m *Manager) Send(ctx context.Context, session bridge.Session, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(text) == "" {
		return fmt.Errorf("message must not be empty")
	}
	if len(text) > maxText {
		return fmt.Errorf("message exceeds %d bytes", maxText)
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return fmt.Errorf("managed manager is closed")
	}
	var target *process
	for _, p := range m.processes {
		p.mu.Lock()
		match := p.readyFlag && p.session.ID == session.ID && p.session.Provider == session.Provider
		p.mu.Unlock()
		if match {
			target = p
			break
		}
	}
	m.mu.Unlock()
	if target == nil {
		return bridge.ErrNotFound
	}
	return target.enqueue(ctx, text)
}

func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	processes := make([]*process, 0, len(m.processes))
	for _, p := range m.processes {
		processes = append(processes, p)
	}
	m.mu.Unlock()
	for _, p := range processes {
		p.stop()
	}
	m.cancel()
	return nil
}

func (p *process) snapshot() bridge.Session { p.mu.Lock(); defer p.mu.Unlock(); return p.session }
func (p *process) emit(kind, text string, status bridge.Status) {
	s := p.snapshot()
	// Events are a bounded progress surface, not a complete transcript archive.
	if len(text) > 64<<10 {
		text = text[:64<<10]
	}
	select {
	case p.manager.events <- Event{SessionID: s.ID, Type: kind, Text: text, Status: status, At: time.Now().UTC()}:
	default:
	}
}
func (p *process) completeStartup(err error) {
	p.startupOnce.Do(func() { p.mu.Lock(); p.readyFlag = err == nil; p.mu.Unlock(); p.startup <- err })
}
func (p *process) write(v any) error {
	p.writerMu.Lock()
	defer p.writerMu.Unlock()
	return json.NewEncoder(p.stdin).Encode(v)
}
func (p *process) enqueue(ctx context.Context, text string) error {
	s := p.snapshot()
	if !s.Active {
		return fmt.Errorf("managed session is no longer active")
	}
	select {
	case <-p.done:
		return fmt.Errorf("managed session exited")
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return fmt.Errorf("managed session exited")
	case p.queue <- text:
		return nil
	default:
		return fmt.Errorf("managed session queue is full")
	}
}
func (p *process) stop() {
	select {
	case <-p.done:
		return
	default:
	}
	killProcess(p.cmd)
	select {
	case <-p.done:
	case <-time.After(3 * time.Second):
		_ = p.stdin.Close()
	}
}

func (p *process) sendLoop() {
	for {
		select {
		case <-p.done:
			return
		case <-p.manager.ctx.Done():
			return
		case text := <-p.queue:
			p.mu.Lock()
			if !p.session.Active {
				p.mu.Unlock()
				return
			}
			p.awaiting = true
			p.turnID = ""
			p.turnBlocked = false
			p.session.Status = bridge.Working
			p.session.LastSeen = time.Now().UTC()
			id := p.nextID
			p.nextID++
			p.requestID = id
			s := p.session
			p.mu.Unlock()
			p.emit("status", "", bridge.Working)
			var input any
			if s.Provider == bridge.Codex {
				input = map[string]any{"id": id, "method": "turn/start", "params": map[string]any{"threadId": s.NativeID, "input": []map[string]string{{"type": "text", "text": text}}}}
			} else {
				input = map[string]any{"type": "user", "message": map[string]any{"role": "user", "content": text}, "session_id": s.NativeID, "parent_tool_use_id": nil}
			}
			if err := p.write(input); err != nil {
				p.finishTurn(true, "failed to write to managed provider")
				p.stop()
				return
			}
			select {
			case <-p.done:
				return
			case <-p.manager.ctx.Done():
				return
			case <-p.turnDone:
			}
		}
	}
}

func (p *process) read(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), 8<<20)
	for scanner.Scan() {
		var msg wireMessage
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			p.completeStartup(fmt.Errorf("provider emitted invalid initialization JSON"))
			killProcess(p.cmd)
			return
		}
		p.mu.Lock()
		p.session.LastSeen = time.Now().UTC()
		provider := p.session.Provider
		p.mu.Unlock()
		if provider == bridge.Codex {
			p.handleCodex(msg)
		} else {
			p.handleClaude(msg)
		}
	}
	if scanner.Err() != nil {
		p.completeStartup(fmt.Errorf("provider stream exceeds limit or failed"))
		killProcess(p.cmd)
	}
}

func (p *process) handleCodex(msg wireMessage) {
	if msg.Method != "" && len(msg.ID) > 0 && string(msg.ID) != "null" {
		// Requests requiring user consent are never approved by the broker.
		var response any
		if msg.Method == "item/commandExecution/requestApproval" || msg.Method == "item/fileChange/requestApproval" {
			response = map[string]any{"id": msg.ID, "result": map[string]string{"decision": "decline"}}
		} else {
			response = map[string]any{"id": msg.ID, "error": map[string]any{"code": -32601, "message": "Interactive requests are not supported by agent-bridge; request denied"}}
		}
		p.mu.Lock()
		p.turnBlocked = true
		p.session.Status = bridge.Blocked
		p.session.Note = "provider requested interactive approval; broker declined"
		p.mu.Unlock()
		_ = p.write(response)
		p.emit("status", "provider requested interactive approval; broker declined", bridge.Blocked)
		return
	}
	var id int
	_ = json.Unmarshal(msg.ID, &id)
	if id == 1 {
		if hasError(msg.Error) {
			p.completeStartup(fmt.Errorf("Codex initialize failed"))
			return
		}
		if err := p.write(map[string]any{"method": "initialized", "params": map[string]any{}}); err != nil {
			p.completeStartup(err)
			return
		}
		s := p.snapshot()
		if err := p.write(map[string]any{"id": 2, "method": "thread/start", "params": map[string]any{"cwd": s.CWD, "sandbox": "workspace-write", "approvalPolicy": "on-request"}}); err != nil {
			p.completeStartup(err)
		}
		return
	}
	if id == 2 {
		if hasError(msg.Error) {
			p.completeStartup(fmt.Errorf("Codex thread/start failed"))
			return
		}
		var r struct {
			Thread struct {
				ID string `json:"id"`
			} `json:"thread"`
		}
		if json.Unmarshal(msg.Result, &r) != nil || r.Thread.ID == "" {
			p.completeStartup(fmt.Errorf("Codex thread/start did not return a thread id"))
			return
		}
		p.mu.Lock()
		p.session.NativeID = r.Thread.ID
		p.session.ID = string(bridge.Codex) + ":" + r.Thread.ID
		p.session.Status = bridge.Idle
		p.mu.Unlock()
		p.completeStartup(nil)
		return
	}
	p.mu.Lock()
	requestID := p.requestID
	awaiting := p.awaiting
	p.mu.Unlock()
	if id != 0 && id == requestID && awaiting {
		if hasError(msg.Error) {
			p.finishTurn(true, "Codex turn/start failed")
			return
		}
		var r struct {
			Turn struct {
				ID string `json:"id"`
			} `json:"turn"`
		}
		if json.Unmarshal(msg.Result, &r) != nil || r.Turn.ID == "" {
			p.finishTurn(true, "Codex turn/start did not return a turn id")
			return
		}
		p.mu.Lock()
		p.turnID = r.Turn.ID
		p.mu.Unlock()
	}
	if msg.Method == "turn/completed" {
		var params struct {
			ThreadID string `json:"threadId"`
			Turn     struct {
				ID     string `json:"id"`
				Status string `json:"status"`
			} `json:"turn"`
		}
		if json.Unmarshal(msg.Params, &params) != nil {
			return
		}
		p.mu.Lock()
		matches := p.awaiting && params.ThreadID == p.session.NativeID && p.turnID != "" && p.turnID == params.Turn.ID
		p.mu.Unlock()
		if matches {
			p.finishTurn(params.Turn.Status != "completed", "")
		}
	}
	if msg.Method == "item/agentMessage/delta" {
		var params struct {
			ThreadID string `json:"threadId"`
			Delta    string `json:"delta"`
		}
		if json.Unmarshal(msg.Params, &params) == nil && params.ThreadID == p.snapshot().NativeID {
			p.emit("output", params.Delta, "")
		}
	}
	if msg.Method == "item/completed" {
		var params struct {
			ThreadID string `json:"threadId"`
			Item     struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"item"`
		}
		if json.Unmarshal(msg.Params, &params) == nil && params.ThreadID == p.snapshot().NativeID && params.Item.Type == "agentMessage" {
			p.emit("output.final", params.Item.Text, "")
		}
	}
}

func (p *process) handleClaude(msg wireMessage) {
	// A forwarded subagent message or a different session cannot finish this turn.
	if (msg.SessionID != "" && msg.SessionID != p.snapshot().NativeID) || (len(msg.ParentToolUseID) > 0 && string(msg.ParentToolUseID) != "null") {
		return
	}
	if msg.Type == "system" && msg.Subtype == "init" {
		p.mu.Lock()
		if !p.awaiting {
			p.session.Status = bridge.Idle
		}
		p.session.Note = ""
		p.mu.Unlock()
		return
	}
	if msg.Type == "assistant" {
		var message struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		}
		if json.Unmarshal(msg.Message, &message) == nil {
			for _, part := range message.Content {
				if part.Type == "text" {
					p.emit("output", part.Text, "")
				}
			}
		}
	}
	if msg.Type == "result" {
		p.finishTurn(msg.IsError || strings.HasPrefix(msg.Subtype, "error") || len(msg.PermissionDenials) > 0, "Claude turn failed or required unavailable permissions")
	}
}

func (p *process) finishTurn(failed bool, note string) {
	p.mu.Lock()
	if !p.awaiting {
		p.mu.Unlock()
		return
	}
	p.awaiting = false
	if failed || p.turnBlocked {
		p.session.Status = bridge.Blocked
		if note != "" {
			p.session.Note = note
		}
	} else {
		p.session.Status = bridge.Idle
		p.session.Note = ""
	}
	p.session.LastSeen = time.Now().UTC()
	status := p.session.Status
	p.mu.Unlock()
	p.emit("status", "", status)
	select {
	case p.turnDone <- struct{}{}:
	default:
	}
}

func hasError(raw json.RawMessage) bool { return len(raw) > 0 && string(raw) != "null" }
