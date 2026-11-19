package adapters

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	bridge "github.com/fuzicheng/agent-bridge"
)

func TestParsePanesAndIdlePrompt(t *testing.T) {
	panes := parsePanes("%7\t123\tclaude\nnot-a-pane\t22\tbash\n%8\tbad\tcodex\n%9\t12\tcodex\n")
	if len(panes) != 2 || panes[0].ID != "%7" || panes[1].PID != 12 {
		t.Fatalf("unexpected panes: %#v", panes)
	}
	for _, test := range []struct {
		text string
		idle bool
	}{
		{"Claude Code\n❯\n shift+tab to cycle modes", true},
		{"\x1b[31m›\x1b[0m\n", true},
		{"$\n", false},
		{">\n", false},
		{"❯ user is typing\n", false},
		{"❯\nprevious response\n❯ user is typing\n", false},
		{"❯\nDo you want to allow this tool?", false},
		{"❯\nEsc to interrupt", false},
	} {
		if got := hasIdlePrompt(test.text); got != test.idle {
			t.Errorf("hasIdlePrompt(%q)=%v, want %v", test.text, got, test.idle)
		}
	}
}

func testAgentProcess(t *testing.T) bridge.Session {
	t.Helper()
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("Unix process identity")
	}
	data, err := os.ReadFile("/bin/sleep")
	if err != nil {
		t.Skipf("sleep binary unavailable: %v", err)
	}
	bin := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(bin, data, 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	info, err := inspectProcess(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	return bridge.Session{ID: "claude:test", NativeID: "test", Provider: bridge.Claude, PID: info.PID, ProcessStart: info.Start, Active: true, Status: bridge.Idle, TmuxPane: "%7"}
}

func TestFindPaneValidatesProcessGeneration(t *testing.T) {
	session := testAgentProcess(t)
	panes := []paneInfo{{ID: "%7", PID: session.PID, Command: "claude"}}
	if id, ok := findPane(session, panes); !ok || id != "%7" {
		t.Fatalf("did not find live pane: %q, %v", id, ok)
	}
	session.ProcessStart = "different-generation"
	if _, ok := findPane(session, panes); ok {
		t.Fatal("accepted PID reuse")
	}
}

func TestSendTmuxPreservesLiteralMessageAndRefusesUnsafeTargets(t *testing.T) {
	session := testAgentProcess(t)
	dir := t.TempDir()
	log := filepath.Join(dir, "calls")
	input := filepath.Join(dir, "input")
	t.Setenv("FAKE_TMUX_LOG", log)
	t.Setenv("FAKE_TMUX_INPUT", input)
	bin := filepath.Join(dir, "tmux")
	script := fmt.Sprintf(`#!/bin/sh
printf 'CALL\n' >> "$FAKE_TMUX_LOG"
printf '%%s\n' "$@" >> "$FAKE_TMUX_LOG"
case "$1" in
  list-panes) printf '%%%%7\t%d\tclaude\n' ;;
  capture-pane) printf 'Claude Code\n❯\n' ;;
  load-buffer) cat > "$FAKE_TMUX_INPUT" ;;
esac
`, session.PID)
	if err := os.WriteFile(bin, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	m := New(Options{TmuxBin: bin})
	message := "literal $(touch /tmp/should-never-run) `date`\nsecond line"
	if err := m.sendTmux(context.Background(), session, message); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(input)
	if err != nil || string(data) != message {
		t.Fatalf("message changed: %q err=%v", data, err)
	}
	calls, _ := os.ReadFile(log)
	if !strings.Contains(string(calls), "paste-buffer\n-b\nagent-bridge-") || !strings.Contains(string(calls), "\n-p\n-t\n%7\n") || !strings.Contains(string(calls), "send-keys\n-t\n%7\nEnter\n") {
		t.Fatalf("missing bracketed paste and Enter: %s", calls)
	}
	for _, bad := range []bridge.Session{
		{Active: true, Status: bridge.Working},
		{Active: true, Status: bridge.Unknown},
		{Active: true, Status: bridge.Idle, TmuxPane: "-a"},
	} {
		if err := m.sendTmux(context.Background(), bad, "message"); !errors.Is(err, bridge.ErrUnsupported) {
			t.Errorf("unsafe target was not refused: %#v err=%v", bad, err)
		}
	}
	if err := m.sendTmux(context.Background(), session, "bad\x1b[0m"); err == nil {
		t.Fatal("accepted terminal escape sequences")
	}
}
