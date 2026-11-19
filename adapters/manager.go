// Package adapters discovers current-user coding-agent sessions and sends through
// public provider interfaces. Discovery never reads conversation transcripts.
package adapters

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	bridge "github.com/fuzicheng/agent-bridge"
)

const maxOutput = 8 << 20

type Options struct {
	CodexHome  string
	ClaudeHome string
	CodexBin   string
	ClaudeBin  string
	TmuxBin    string
}

type Manager struct {
	opts      Options
	queueOnce sync.Once
	queueOK   bool
}

func New(opts Options) *Manager {
	home, _ := os.UserHomeDir()
	if opts.CodexHome == "" {
		opts.CodexHome = os.Getenv("CODEX_HOME")
	}
	if opts.CodexHome == "" {
		opts.CodexHome = filepath.Join(home, ".codex")
	}
	if opts.ClaudeHome == "" {
		opts.ClaudeHome = os.Getenv("CLAUDE_CONFIG_DIR")
	}
	if opts.ClaudeHome == "" {
		opts.ClaudeHome = filepath.Join(home, ".claude")
	}
	if opts.CodexBin == "" {
		opts.CodexBin = "codex"
	}
	if opts.ClaudeBin == "" {
		opts.ClaudeBin = "claude"
	}
	if opts.TmuxBin == "" {
		opts.TmuxBin = "tmux"
	}
	return &Manager{opts: opts}
}

// Discover returns historical sessions as observe-only and never equates recent
// file timestamps with a live process. Warnings are nonfatal provider diagnostics.
func (m *Manager) Discover(ctx context.Context) ([]bridge.Session, []string) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	var all []bridge.Session
	var warnings []string
	collect := func(fn func(context.Context) ([]bridge.Session, []string)) {
		defer wg.Done()
		s, w := fn(ctx)
		mu.Lock()
		all = append(all, s...)
		warnings = append(warnings, w...)
		mu.Unlock()
	}
	wg.Add(2)
	go collect(m.discoverCodex)
	go collect(m.discoverClaude)
	wg.Wait()
	panes := m.discoverPanes(ctx)
	for i := range all {
		if !all[i].Active {
			continue
		}
		if pane, ok := findPane(all[i], panes); ok {
			all[i].TmuxPane = pane
			all[i].Capabilities.CanCaptureOutput = true
			if all[i].Provider == bridge.Claude {
				all[i].Capabilities.Transport = bridge.Tmux
				all[i].Capabilities.CanSend = true
				all[i].Note = "tmux delivery requires explicit permission and a confirmed idle provider prompt"
			}
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Active != all[j].Active {
			return all[i].Active
		}
		if !all[i].LastSeen.Equal(all[j].LastSeen) {
			return all[i].LastSeen.After(all[j].LastSeen)
		}
		return all[i].ID < all[j].ID
	})
	sort.Strings(warnings)
	return all, warnings
}

func (m *Manager) queueAvailable(ctx context.Context) bool {
	m.queueOnce.Do(func() {
		out, err := m.run(ctx, m.opts.CodexBin, "queue", "--help")
		m.queueOK = err == nil && bytes.Contains(out, []byte("--thread")) && bytes.Contains(out, []byte("--message"))
	})
	return m.queueOK
}

// Send reports transport acceptance only. A successful command does not prove
// that the target agent read, understood, or answered the message.
func (m *Manager) Send(ctx context.Context, s bridge.Session, text string) error {
	if len(text) == 0 || len(text) > 64<<10 {
		return fmt.Errorf("message must contain 1..65536 bytes")
	}
	if strings.ContainsRune(text, 0) {
		return fmt.Errorf("message contains NUL")
	}
	if s.ID != string(s.Provider)+":"+s.NativeID || !validNativeID(s.NativeID) {
		return fmt.Errorf("invalid session identity")
	}
	if !s.Active || !s.Capabilities.CanSend {
		return fmt.Errorf("%w: session cannot receive provider messages", bridge.ErrUnsupported)
	}
	p, err := inspectProcess(s.PID)
	if err != nil || s.ProcessStart == "" || p.Start != s.ProcessStart || !providerProcess(p.Command, s.Provider) {
		return fmt.Errorf("%w: session process exited or its identity changed", bridge.ErrUnsupported)
	}
	switch s.Capabilities.Transport {
	case bridge.CodexQueue:
		if s.Provider != bridge.Codex || !m.queueAvailable(ctx) {
			return fmt.Errorf("%w: codex queue unavailable", bridge.ErrUnsupported)
		}
		_, err := m.run(ctx, m.opts.CodexBin, "queue", "--thread", s.NativeID, "--message", text)
		return err
	case bridge.Tmux:
		return m.sendTmux(ctx, s, text)
	default:
		return fmt.Errorf("%w: %s transport", bridge.ErrUnsupported, s.Capabilities.Transport)
	}
}

func providerProcess(command string, p bridge.Provider) bool {
	command = strings.ToLower(command)
	// Inspect the executable and common interpreter/wrapper slots only. A
	// provider name appearing in a task's arbitrary text is not process identity.
	for i, f := range strings.Fields(command) {
		if i > 2 {
			break
		}
		name := strings.Trim(filepath.Base(f), "\"'")
		if name == string(p) || strings.HasPrefix(name, string(p)+"-") || strings.HasPrefix(name, string(p)+".") {
			return true
		}
	}
	return false
}

func validNativeID(s string) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

type limitedBuffer struct {
	bytes.Buffer
	overflow bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	remaining := maxOutput - b.Len()
	if remaining <= 0 {
		b.overflow = true
		return n, nil
	}
	if len(p) > remaining {
		p = p[:remaining]
		b.overflow = true
	}
	_, _ = b.Buffer.Write(p)
	return n, nil
}

func (m *Manager) run(ctx context.Context, bin string, args ...string) ([]byte, error) {
	return m.runInput(ctx, nil, bin, args...)
}

func (m *Manager) runInput(ctx context.Context, input []byte, bin string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	// A child of the provider may inherit stdout/stderr. Bound waiting for such
	// pipe owners after the provider exits or its command context is canceled.
	cmd.WaitDelay = time.Second
	cmd.Stdin = bytes.NewReader(input)
	// Override only provider-home variables. Never rewrite HOME or CODEX_HOME in
	// the host process, nor emit command arguments (messages may contain secrets).
	cmd.Env = overrideEnv(os.Environ(), "CODEX_HOME", m.opts.CodexHome)
	cmd.Env = overrideEnv(cmd.Env, "CLAUDE_CONFIG_DIR", m.opts.ClaudeHome)
	var out, stderr limitedBuffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Base(bin), ctx.Err())
	}
	if out.overflow || stderr.overflow {
		return nil, fmt.Errorf("%s output exceeded %d bytes", filepath.Base(bin), maxOutput)
	}
	if err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return nil, fmt.Errorf("%s not installed", filepath.Base(bin))
		}
		return nil, fmt.Errorf("%s command failed: %w", filepath.Base(bin), err)
	}
	return out.Bytes(), nil
}

func overrideEnv(env []string, key, value string) []string {
	result := make([]string, 0, len(env)+1)
	for _, s := range env {
		if !strings.HasPrefix(s, key+"=") {
			result = append(result, s)
		}
	}
	return append(result, key+"="+value)
}
