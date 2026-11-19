package adapters

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	bridge "github.com/fuzicheng/agent-bridge"
)

type paneInfo struct {
	ID      string
	PID     int
	Command string
}

var panePattern = regexp.MustCompile(`^%[0-9]+$`)
var ansiPattern = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]`)

func (m *Manager) discoverPanes(ctx context.Context) []paneInfo {
	if m.opts.TmuxBin == "" {
		return nil
	}
	out, err := m.run(ctx, m.opts.TmuxBin, "list-panes", "-a", "-F", "#{pane_id}\t#{pane_pid}\t#{pane_current_command}")
	if err != nil {
		return nil
	}
	return parsePanes(string(out))
}

func parsePanes(text string) []paneInfo {
	panes := make([]paneInfo, 0)
	for _, line := range strings.Split(text, "\n") {
		fields := strings.SplitN(line, "\t", 3)
		if len(fields) != 3 || !panePattern.MatchString(fields[0]) {
			continue
		}
		pid, err := strconv.Atoi(fields[1])
		if err != nil || pid <= 0 {
			continue
		}
		panes = append(panes, paneInfo{ID: fields[0], PID: pid, Command: fields[2]})
	}
	return panes
}

func findPane(session bridge.Session, panes []paneInfo) (string, bool) {
	if session.PID <= 0 {
		return "", false
	}
	process, err := inspectProcess(session.PID)
	if err != nil || (session.ProcessStart != "" && session.ProcessStart != process.Start) {
		return "", false
	}
	ancestors := map[int]bool{}
	for count := 0; count < 64; count++ {
		if ancestors[process.PID] {
			break
		}
		ancestors[process.PID] = true
		if process.PPID <= 1 {
			break
		}
		process, err = inspectProcess(process.PPID)
		if err != nil {
			break
		}
	}
	for _, pane := range panes {
		if ancestors[pane.PID] {
			return pane.ID, true
		}
	}
	return "", false
}

func (m *Manager) sendTmux(ctx context.Context, session bridge.Session, text string) error {
	if !session.Active || session.Status != bridge.Idle {
		return fmt.Errorf("%w: tmux requires a confirmed idle agent", bridge.ErrUnsupported)
	}
	if !panePattern.MatchString(session.TmuxPane) {
		return fmt.Errorf("%w: missing exact tmux pane", bridge.ErrUnsupported)
	}
	if len(text) == 0 || len(text) > 65536 {
		return fmt.Errorf("invalid tmux message length")
	}
	for _, c := range text {
		if unicode.IsControl(c) && c != '\n' && c != '\t' {
			return fmt.Errorf("tmux message contains terminal control characters")
		}
	}
	process, err := inspectProcess(session.PID)
	if err != nil || (session.ProcessStart != "" && process.Start != session.ProcessStart) {
		return fmt.Errorf("%w: target process exited or changed", bridge.ErrUnsupported)
	}
	if !isAgentProcess(process.Command, session.Provider) {
		return fmt.Errorf("%w: target is not the expected agent process", bridge.ErrUnsupported)
	}
	panes := m.discoverPanes(ctx)
	pane, ok := findPane(session, panes)
	if !ok || pane != session.TmuxPane {
		return fmt.Errorf("%w: target no longer belongs to the selected pane", bridge.ErrUnsupported)
	}
	var foreground string
	for _, p := range panes {
		if p.ID == pane {
			foreground = strings.ToLower(filepath.Base(p.Command))
		}
	}
	if !strings.Contains(foreground, string(session.Provider)) {
		return fmt.Errorf("%w: pane foreground command is not the agent", bridge.ErrUnsupported)
	}
	output, err := m.run(ctx, m.opts.TmuxBin, "capture-pane", "-p", "-t", pane, "-S", "-25")
	if err != nil {
		return fmt.Errorf("inspect idle prompt: %w", err)
	}
	if !hasIdlePrompt(string(output)) {
		return fmt.Errorf("%w: could not confirm an empty idle agent prompt", bridge.ErrUnsupported)
	}
	buffer := "agent-bridge-" + bridge.NewID()
	if _, err := m.runInput(ctx, []byte(text), m.opts.TmuxBin, "load-buffer", "-b", buffer, "-"); err != nil {
		return fmt.Errorf("load tmux message: %w", err)
	}
	defer func() {
		// A cancellation must not leave an orphaned buffer containing group data.
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = m.run(cleanup, m.opts.TmuxBin, "delete-buffer", "-b", buffer)
	}()
	if _, err := m.run(ctx, m.opts.TmuxBin, "paste-buffer", "-b", buffer, "-p", "-t", pane); err != nil {
		return fmt.Errorf("paste tmux message: %w", err)
	}
	if _, err := m.run(ctx, m.opts.TmuxBin, "send-keys", "-t", pane, "Enter"); err != nil {
		return fmt.Errorf("submit tmux message: %w", err)
	}
	return nil
}

func isAgentProcess(command string, provider bridge.Provider) bool {
	for i, arg := range strings.Fields(command) {
		if i > 2 {
			break
		}
		base := strings.ToLower(filepath.Base(arg))
		if base == string(provider) || base == string(provider)+".exe" || base == string(provider)+".js" {
			return true
		}
	}
	return false
}

func hasIdlePrompt(text string) bool {
	text = ansiPattern.ReplaceAllString(text, "")
	lower := strings.ToLower(text)
	for _, marker := range []string{"esc to interrupt", "ctrl+c to interrupt", "do you want", "allow this", "permission", "trust this", "approve", "[y/n]", "continue?"} {
		if strings.Contains(lower, marker) {
			return false
		}
	}
	lines := strings.Split(text, "\n")
	// TUI status/footer lines may appear below an idle prompt. Only accept an
	// empty, characteristic coding-agent prompt near the end of the pane.
	start := len(lines) - 10
	if start < 0 {
		start = 0
	}
	for i := len(lines) - 1; i >= start; i-- {
		line := lines[i]
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "❯") || strings.HasPrefix(trimmed, "›") {
			// Prefer the latest prompt; an earlier empty prompt must not mask
			// text the user has since entered on the actual input line.
			return trimmed == "❯" || trimmed == "›"
		}
	}
	return false
}
