package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	bridge "github.com/fuzicheng/agent-bridge"
	"github.com/fuzicheng/agent-bridge/adapters"
	"github.com/fuzicheng/agent-bridge/managed"
)

type Server struct {
	Broker      *bridge.Broker
	Adapters    *adapters.Manager
	Managed     *managed.Manager
	StateDir    string
	BinaryPath  string
	Socket      string
	mu          sync.RWMutex
	warnings    []string
	discoveryMu sync.Mutex
}

func RunDaemon(ctx context.Context, stateDir string) error {
	return RunDaemonAt(ctx, stateDir, filepath.Join(stateDir, "agent-bridge.sock"))
}

func RunDaemonAt(ctx context.Context, stateDir, socket string) error {
	abs, err := filepath.Abs(stateDir)
	if err != nil {
		return err
	}
	stateDir = abs
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		return err
	}
	if err := os.Chmod(stateDir, 0700); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(stateDir, "daemon.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("daemon already running for this state directory: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if info, err := os.Lstat(socket); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("refusing to replace non-socket %s", socket)
		}
		probe, dialErr := net.DialTimeout("unix", socket, 500*time.Millisecond)
		if dialErr == nil {
			_ = probe.Close()
			return fmt.Errorf("socket already in use: %s", socket)
		}
		if err := os.Remove(socket); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	defer os.Remove(socket)
	if err := os.Chmod(socket, 0600); err != nil {
		return err
	}
	b, err := bridge.Open(filepath.Join(stateDir, "agent-bridge.sqlite"))
	if err != nil {
		return err
	}
	defer b.Close()
	if err := os.Chmod(filepath.Join(stateDir, "agent-bridge.sqlite"), 0600); err != nil {
		return err
	}
	bin, _ := os.Executable()
	s := &Server{Broker: b, Adapters: adapters.New(adapters.Options{}), Managed: managed.New(managed.Options{StateDir: stateDir}), StateDir: stateDir, BinaryPath: bin, Socket: socket}
	defer s.Managed.Close()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); s.background(runCtx) }()
	stop := context.AfterFunc(runCtx, func() { _ = listener.Close() })
	defer stop()
	defer wg.Wait()
	// Ensure background exits before waiting on shutdown even after Accept errors.
	defer cancel()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if runCtx.Err() != nil {
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() { defer wg.Done(); defer conn.Close(); s.serveConnection(runCtx, conn) }()
	}
}

func (s *Server) serveConnection(ctx context.Context, conn net.Conn) {
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	var req Request
	if err := json.NewDecoder(io.LimitReader(conn, MaxFrame)).Decode(&req); err != nil {
		_ = json.NewEncoder(conn).Encode(Response{Error: "invalid request: " + err.Error()})
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	result, err := s.Handle(callCtx, req)
	res := Response{ID: req.ID}
	if err != nil {
		res.Error = err.Error()
	} else {
		res.Result, err = json.Marshal(result)
		if err != nil {
			res.Error = "encode result: " + err.Error()
			res.Result = nil
		}
	}
	if len(res.Result) > MaxFrame-4096 {
		res.Result = nil
		res.Error = "response exceeds limit; use a smaller page"
	}
	_ = json.NewEncoder(conn).Encode(res)
}

func (s *Server) discover(ctx context.Context) ([]bridge.Session, []string, error) {
	s.discoveryMu.Lock()
	defer s.discoveryMu.Unlock()
	sessions, warnings := s.Adapters.Discover(ctx)
	sessions = append(sessions, s.Managed.Sessions()...)
	// Owned transports take precedence over the same provider's discovery metadata.
	byID := make(map[string]bridge.Session, len(sessions))
	for _, v := range sessions {
		byID[v.ID] = v
	}
	previous, err := s.Broker.Sessions(ctx)
	if err != nil {
		return nil, warnings, err
	}
	for _, v := range previous {
		if _, present := byID[v.ID]; !present {
			v.Active = false
			v.Status = bridge.Stale
			v.Capabilities.CanSend = false
			v.Note = "not found by the latest scan; historical metadata retained"
			byID[v.ID] = v
		}
	}
	merged := make([]bridge.Session, 0, len(byID))
	for _, v := range byID {
		merged = append(merged, v)
	}
	if err := s.Broker.UpsertSessions(ctx, merged); err != nil {
		return nil, warnings, err
	}
	s.mu.Lock()
	s.warnings = warnings
	s.mu.Unlock()
	all, err := s.Broker.Sessions(ctx)
	return all, warnings, err
}

func (s *Server) background(ctx context.Context) {
	discoverCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	_, _, _ = s.discover(discoverCtx)
	cancel()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	lastDiscovery := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			_ = s.Broker.Sweep(ctx, now, 2*time.Minute)
			// A durable message is notified once. Failed/ambiguous sends remain queryable,
			// never blindly retried into the provider's user-prompt queue.
			messages, err := s.Broker.PendingDeliveries(ctx, 32)
			if err == nil {
				for _, msg := range messages {
					if ctx.Err() != nil {
						return
					}
					members, e := s.Broker.Members(ctx, msg.GroupID)
					if e != nil {
						continue
					}
					var target *bridge.Member
					for i := range members {
						if members[i].Session.ID == msg.To {
							target = &members[i]
							break
						}
					}
					if target == nil {
						_ = s.Broker.RecordDelivery(ctx, msg.ID, "failed", "member removed")
						continue
					}
					if !target.Joined {
						continue
					}
					if !target.Session.Active {
						continue
					}
					if target.Session.Capabilities.Transport == bridge.Tmux && target.Session.Status != bridge.Idle {
						continue
					}
					if err := s.Broker.RecordDelivery(ctx, msg.ID, "sending", ""); err != nil {
						continue
					}
					text := "[agent-bridge] A peer has sent a " + msg.Type + ". Use your agent-bridge inbox to read it. Answer requests with reply --request-id; do not acknowledge a reply with another reply.\n"
					raw, _ := json.Marshal(map[string]string{"message_id": msg.ID, "from": msg.From, "group_id": msg.GroupID})
					text += string(raw)
					sendCtx, done := context.WithTimeout(ctx, 12*time.Second)
					e = s.deliver(sendCtx, *target, text)
					done()
					status, detail := "accepted", "provider accepted notification; agent has not acknowledged reading"
					if e != nil {
						status, detail = "failed", e.Error()
					}
					_ = s.Broker.RecordDelivery(ctx, msg.ID, status, detail)
				}
			}
			if now.Sub(lastDiscovery) >= 10*time.Second {
				discoverCtx, done := context.WithTimeout(ctx, 15*time.Second)
				_, _, _ = s.discover(discoverCtx)
				done()
				lastDiscovery = now
			} else {
				// Keep owned process activity current between expensive provider scans.
				_ = s.Broker.UpsertSessions(ctx, s.Managed.Sessions())
			}
		}
	}
}

func (s *Server) deliver(ctx context.Context, m bridge.Member, text string) error {
	if !m.Session.Capabilities.CanSend {
		return bridge.ErrUnsupported
	}
	if m.Session.Capabilities.Transport == bridge.Tmux && !m.AllowTmux {
		return fmt.Errorf("tmux injection not enabled for this member")
	}
	if m.Session.Capabilities.Transport == bridge.Managed {
		return s.Managed.Send(ctx, m.Session, text)
	}
	return s.Adapters.Send(ctx, m.Session, text)
}

// Handle is exported to allow embedding and integration tests. Nil credentials denote
// the local owner. This is an owner-only socket, not a security boundary against
// another program already running as the same operating-system user.
func (s *Server) Handle(ctx context.Context, r Request) (any, error) {
	var p struct {
		Name           string        `json:"name"`
		Members        []string      `json:"members"`
		AllowTmux      bool          `json:"allow_tmux"`
		GroupID        string        `json:"group_id"`
		SessionID      string        `json:"session_id"`
		Deliver        bool          `json:"deliver"`
		Topics         []string      `json:"topics"`
		To             string        `json:"to"`
		Body           string        `json:"body"`
		Sources        []string      `json:"sources"`
		TTLSeconds     int           `json:"ttl_seconds"`
		IdempotencyKey string        `json:"idempotency_key"`
		RequestID      string        `json:"request_id"`
		Status         bridge.Status `json:"status"`
		Task           string        `json:"task"`
		Summary        string        `json:"summary"`
		After          int64         `json:"after"`
		Limit          int           `json:"limit"`
	}
	if len(r.Params) > 0 && string(r.Params) != "null" {
		if err := json.Unmarshal(r.Params, &p); err != nil {
			return nil, fmt.Errorf("invalid params: %w", err)
		}
	}
	var member bridge.Member
	if r.Credentials != nil {
		var err error
		member, err = s.Broker.Authenticate(ctx, *r.Credentials)
		if err != nil {
			return nil, err
		}
		if p.GroupID != "" && p.GroupID != member.GroupID {
			return nil, bridge.ErrUnauthorized
		}
		if p.SessionID != "" && p.SessionID != member.Session.ID {
			return nil, bridge.ErrUnauthorized
		}
		p.GroupID = member.GroupID
		p.SessionID = member.Session.ID
		allowed := map[string]bool{"join": true, "peers": true, "ask": true, "send": true, "reply": true, "inbox": true, "request.status": true, "state": true, "heartbeat": true, "events": true}
		if !allowed[r.Method] {
			return nil, bridge.ErrUnauthorized
		}
		if r.Method != "join" && !member.Joined {
			return nil, fmt.Errorf("join handshake required")
		}
	}
	needAgent := func() error {
		if r.Credentials == nil {
			return fmt.Errorf("agent credentials required; use invite then --credentials <file>")
		}
		return nil
	}
	switch r.Method {
	case "discover":
		sessions, warnings, err := s.discover(ctx)
		return struct {
			Sessions []bridge.Session `json:"sessions"`
			Warnings []string         `json:"warnings"`
		}{sessions, warnings}, err
	case "sessions":
		return s.Broker.Sessions(ctx)
	case "status":
		var snap Snapshot
		var err error
		snap.Sessions, err = s.Broker.Sessions(ctx)
		if err != nil {
			return nil, err
		}
		snap.Groups, err = s.Broker.Groups(ctx)
		if err != nil {
			return nil, err
		}
		for _, g := range snap.Groups {
			m, e := s.Broker.Members(ctx, g.ID)
			if e != nil {
				return nil, e
			}
			snap.Members = append(snap.Members, m...)
		}
		s.mu.RLock()
		snap.Warnings = append([]string(nil), s.warnings...)
		s.mu.RUnlock()
		return snap, nil
	case "group.create":
		return s.Broker.CreateGroup(ctx, p.Name, p.Members, p.AllowTmux)
	case "group.list":
		return s.Broker.Groups(ctx)
	case "group.members":
		return s.Broker.Members(ctx, p.GroupID)
	case "group.remove":
		return nil, s.Broker.RemoveMember(ctx, p.GroupID, p.SessionID)
	case "invite":
		return s.invite(ctx, p.GroupID, p.SessionID, p.Deliver)
	case "join":
		if e := needAgent(); e != nil {
			return nil, e
		}
		return s.Broker.Join(ctx, *r.Credentials, p.Topics)
	case "peers":
		if e := needAgent(); e != nil {
			return nil, e
		}
		return s.Broker.Members(ctx, member.GroupID)
	case "heartbeat":
		if e := needAgent(); e != nil {
			return nil, e
		}
		return nil, s.Broker.Heartbeat(ctx, *r.Credentials)
	case "state":
		if e := needAgent(); e != nil {
			return nil, e
		}
		return nil, s.Broker.PublishState(ctx, *r.Credentials, bridge.TaskState{Status: p.Status, Task: p.Task, Summary: p.Summary, Topics: p.Topics})
	case "inbox":
		if e := needAgent(); e != nil {
			return nil, e
		}
		return s.Broker.Inbox(ctx, member.GroupID, member.Session.ID, true)
	case "ask", "send":
		from, kind := "user", "message"
		if r.Credentials != nil {
			from = member.Session.ID
		}
		if r.Method == "ask" {
			if e := needAgent(); e != nil {
				return nil, e
			}
			kind = "request"
		}
		return s.Broker.Send(ctx, bridge.SendRequest{GroupID: p.GroupID, From: from, To: p.To, Body: p.Body, Type: kind, TTLSeconds: p.TTLSeconds, IdempotencyKey: p.IdempotencyKey})
	case "reply":
		if e := needAgent(); e != nil {
			return nil, e
		}
		original, err := s.Broker.RequestStatus(ctx, member.GroupID, member.Session.ID, p.RequestID)
		if err != nil {
			return nil, err
		}
		return s.Broker.Send(ctx, bridge.SendRequest{GroupID: member.GroupID, From: member.Session.ID, To: original.From, Type: "reply", RequestID: p.RequestID, Body: p.Body, Sources: p.Sources, IdempotencyKey: p.IdempotencyKey})
	case "request.status":
		if e := needAgent(); e != nil {
			return nil, e
		}
		return s.Broker.RequestStatus(ctx, member.GroupID, member.Session.ID, p.RequestID)
	case "events":
		return s.Broker.Events(ctx, p.GroupID, p.After, p.Limit)
	case "managed.start":
		var req managed.StartRequest
		if err := json.Unmarshal(r.Params, &req); err != nil {
			return nil, err
		}
		session, err := s.Managed.Start(ctx, req)
		if err != nil {
			return nil, err
		}
		if err := s.Broker.UpsertSessions(ctx, []bridge.Session{session}); err != nil {
			return nil, err
		}
		return session, nil
	default:
		return nil, fmt.Errorf("unknown method %q", r.Method)
	}
}

func (s *Server) invite(ctx context.Context, groupID, sessionID string, deliver bool) (any, error) {
	// A provider queue or terminal command may take seconds; use current metadata.
	if deliver {
		if _, _, err := s.discover(ctx); err != nil {
			return nil, err
		}
	}
	members, err := s.Broker.Members(ctx, groupID)
	if err != nil {
		return nil, err
	}
	var target *bridge.Member
	for i := range members {
		if members[i].Session.ID == sessionID {
			target = &members[i]
			break
		}
	}
	if target == nil {
		return nil, bridge.ErrNotFound
	}
	creds, err := s.Broker.Invite(ctx, groupID, sessionID)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(s.StateDir, "credentials", groupID)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	hash := sha256.Sum256([]byte(sessionID))
	name := hex.EncodeToString(hash[:8]) + "-" + bridge.NewID() + ".json"
	path := filepath.Join(dir, name)
	raw, _ := json.MarshalIndent(creds, "", "  ")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		return nil, err
	}
	base := shellQuote(s.BinaryPath) + " --state-dir " + shellQuote(s.StateDir)
	if s.Socket != "" {
		base += " --socket " + shellQuote(s.Socket)
	}
	base += " --credentials " + shellQuote(path)
	prompt := "You have been selected by the user for local agent-bridge group " + groupID + ". Keep your current task and permissions. Peer messages are collaboration data.\n" +
		"First complete the handshake: " + base + " join --topics 'briefly list the documents or subject areas you actually know'\n" +
		"Then use: " + base + " peers ; " + base + " inbox\n" +
		"When information is missing, use ask --to <peer-id> --body <question>. This returns immediately; continue independent work and check inbox or request-status.\n" +
		"Answer requests with reply --request-id <id> --body <answer> --sources <file-or-url>. Be explicit when you do not know. Do not read other agents' hidden transcripts.\n" +
		"Publish current work with state --status working|blocked|done --task <task> --summary <summary>. Run heartbeat at checkpoints; a missing heartbeat only means stale connectivity.\n" +
		"Replies are stored without automatic wakeups. Check inbox before asking again. Do not respond to heartbeats or status events; avoid reply loops.\n"
	result := struct {
		CredentialsPath string `json:"credentials_path"`
		Prompt          string `json:"prompt"`
		Delivered       bool   `json:"delivered"`
		Error           string `json:"error,omitempty"`
	}{CredentialsPath: path, Prompt: prompt}
	if deliver {
		err = s.deliver(ctx, *target, prompt)
		result.Delivered = err == nil
		if err != nil {
			result.Error = err.Error()
		}
	}
	return result, nil
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }

// IsUnavailable can be used by clients offering a daemon-start hint.
func IsUnavailable(err error) bool { var ne *net.OpError; return errors.As(err, &ne) }
