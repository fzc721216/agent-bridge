package adapters

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	bridge "github.com/fuzicheng/agent-bridge"
)

func TestCodexSQLiteReadOnlyAndSchemaFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state_5.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE threads(id TEXT, title TEXT, cwd TEXT, updated_at INTEGER, archived INTEGER); INSERT INTO threads VALUES('thread-1','saved task','/work',1700000000,0),('thread-2','archived','/work',1700000001,1)`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := readCodexDB(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Name != "saved task" || rows[0].Active || rows[0].Capabilities.CanSend || rows[0].Capabilities.Transport != bridge.ObserveOnly {
		t.Fatalf("unexpected rows: %#v", rows)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("discovery modified provider database")
	}
	if rows[0].LastSeen.Unix() != 1700000000 {
		t.Fatal(rows[0].LastSeen)
	}
}

func TestCodexIndexLatestAndMalformedLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session_index.jsonl")
	err := os.WriteFile(path, []byte("{\"id\":\"same-id\",\"thread_name\":\"old\",\"updated_at\":\"2026-01-01T00:00:00Z\"}\nnot json\n{\"id\":\"same-id\",\"thread_name\":\"new\",\"updated_at\":\"2026-02-01T00:00:00Z\"}\n{\"id\":\"../../invalid\",\"thread_name\":\"reject\"}\n"), 0600)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := readCodexIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows["same-id"].Name != "new" {
		t.Fatalf("unexpected index: %#v", rows)
	}
}

func TestHistoricalTimestampsNeverAuthorizeMessaging(t *testing.T) {
	home := t.TempDir()
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	data := `{"id":"very-recent","thread_name":"recent","updated_at":"` + stamp + `"}` + "\n"
	if err := os.WriteFile(filepath.Join(home, "session_index.jsonl"), []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	m := New(Options{CodexHome: home, ClaudeHome: t.TempDir(), CodexBin: "/no/such/codex", ClaudeBin: "/no/such/claude", TmuxBin: "/no/such/tmux"})
	rows, warnings := m.Discover(context.Background())
	if len(warnings) == 0 {
		t.Fatal("missing provider diagnostic")
	}
	if len(rows) != 1 || rows[0].Active || rows[0].Capabilities.CanSend {
		t.Fatalf("historical session became active: %#v", rows)
	}
}

func TestClaudeMetadataRejectsDeadAndReusedPID(t *testing.T) {
	dead, err := parseClaude([]byte(`{"sessionId":"dead-id","pid":2147483647,"status":"idle"}`))
	if err != nil {
		t.Fatal(err)
	}
	if dead.Active || dead.Capabilities.CanSend {
		t.Fatal("dead PID considered active")
	}
	p, err := inspectProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{"sessionId": "wrong-start", "pid": os.Getpid(), "procStart": p.Start + "-wrong", "status": "idle"})
	reused, err := parseClaude(data)
	if err != nil {
		t.Fatal(err)
	}
	if reused.Active || reused.Capabilities.CanSend {
		t.Fatal("reused/non-Claude PID considered active")
	}
}

func TestProviderMissingAndOutputBound(t *testing.T) {
	m := New(Options{})
	if _, err := m.run(context.Background(), "/definitely/not/an/executable"); err == nil {
		t.Fatal("missing executable accepted")
	}
	b := limitedBuffer{}
	input := make([]byte, maxOutput+100)
	n, err := b.Write(input)
	if err != nil || n != len(input) || b.Len() != maxOutput || !b.overflow {
		t.Fatalf("bad output bound n=%d len=%d overflow=%v err=%v", n, b.Len(), b.overflow, err)
	}
	if stateVersion("state_10.sqlite") <= stateVersion("state_9.sqlite") {
		t.Fatal("state versions sorted lexically")
	}
}

func TestCodexQueueLiteralArgumentsAndIdentity(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "codex")
	log := filepath.Join(dir, "args.json")
	// A long-lived shell named codex supplies an owned provider identity. The
	// separate queue branch records argv without evaluating message contents.
	script := `#!/bin/sh
if [ "$1" = "serve-test" ]; then
  read line
elif [ "$2" = "--help" ]; then
  printf '%s\n' '--thread --message'
else
  printf '%s\n' "$3" "$5" > "$ARGV_LOG"
fi
`
	if err := os.WriteFile(bin, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARGV_LOG", log)
	cmd := exec.Command(bin, "serve-test")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stdin.Close(); _ = cmd.Process.Kill(); _ = cmd.Wait() })
	var p processInfo
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		p, err = inspectProcess(cmd.Process.Pid)
		if err == nil && providerProcess(p.Command, bridge.Codex) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil || !providerProcess(p.Command, bridge.Codex) {
		t.Fatalf("provider identity not established: %v %#v", err, p)
	}
	m := New(Options{CodexBin: bin, CodexHome: t.TempDir(), ClaudeHome: t.TempDir()})
	s := bridge.Session{ID: "codex:target-id", Provider: bridge.Codex, NativeID: "target-id", PID: p.PID, ProcessStart: p.Start, Active: true, Capabilities: bridge.Capabilities{CanSend: true, Transport: bridge.CodexQueue}}
	body := "literal $(touch " + filepath.Join(dir, "injected") + ") `echo bad`\nnext line"
	if err := m.Send(context.Background(), s, body); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "target-id\n"+body+"\n" {
		t.Fatalf("message argv changed: %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "injected")); !os.IsNotExist(err) {
		t.Fatal("message was evaluated as shell")
	}
	s.ProcessStart += "wrong"
	if err := m.Send(context.Background(), s, "must refuse"); err == nil {
		t.Fatal("PID identity mismatch accepted")
	}
}

func TestMetadataFileBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "huge.json")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", (1<<20)+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSmallFile(path); err == nil {
		t.Fatal("oversized metadata accepted")
	}
}

func TestDaemonPIDPlainAndStructured(t *testing.T) {
	for _, tc := range []struct {
		input string
		pid   int
		start string
	}{
		{"123\n", 123, ""},
		{`{"pid":123,"processIdentity":{"startTicks":9507044}}`, 123, "9507044"},
		{`{"pid":123,"processIdentity":{"startTicks":"9507044"}}`, 123, "9507044"},
	} {
		pid, start, err := parseDaemonPID([]byte(tc.input))
		if err != nil || pid != tc.pid || start != tc.start {
			t.Fatalf("parseDaemonPID(%q)=(%d,%q,%v)", tc.input, pid, start, err)
		}
	}
	for _, input := range []string{"", "0", "-12", "not-json", `{"pid":0}`, `{"pid":"123"}`} {
		if _, _, err := parseDaemonPID([]byte(input)); err == nil {
			t.Errorf("invalid daemon metadata accepted: %q", input)
		}
	}
}

func TestLoadedCodexThreadsUsesStructuredPIDIdentity(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "codex")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nread line\n"), 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "app-server")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stdin.Close(); _ = cmd.Process.Kill(); _ = cmd.Wait() })
	var p processInfo
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		p, err = inspectProcess(cmd.Process.Pid)
		if err == nil && providerProcess(p.Command, bridge.Codex) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil || !providerProcess(p.Command, bridge.Codex) {
		t.Fatalf("provider identity missing: %#v err=%v", p, err)
	}
	home := t.TempDir()
	daemonDir := filepath.Join(home, "app-server-daemon")
	if err := os.MkdirAll(daemonDir, 0700); err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(daemonDir, "daemon.pid")
	identity := map[string]any{}
	if _, err := strconv.ParseUint(p.Start, 10, 64); err == nil {
		identity["startTicks"] = p.Start
	}
	pidData, _ := json.Marshal(map[string]any{"pid": p.PID, "processIdentity": identity})
	if err := os.WriteFile(pidFile, pidData, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(daemonDir, "loaded-threads.json"), []byte(`{"loadedThreadIds":["thread-live"]}`), 0600); err != nil {
		t.Fatal(err)
	}
	m := New(Options{CodexHome: home})
	if live := m.loadedCodexThreads(); len(live) != 1 || live["thread-live"].PID != p.PID {
		t.Fatalf("loaded thread not found: %#v", live)
	}
	if _, err := strconv.ParseUint(p.Start, 10, 64); err == nil {
		pidData, _ = json.Marshal(map[string]any{"pid": p.PID, "processIdentity": map[string]any{"startTicks": "0"}})
		if err := os.WriteFile(pidFile, pidData, 0600); err != nil {
			t.Fatal(err)
		}
		if live := m.loadedCodexThreads(); len(live) != 0 {
			t.Fatalf("reused daemon PID accepted: %#v", live)
		}
	}
}

func TestClaudeMissingGenerationCannotAuthorizeSavedMetadata(t *testing.T) {
	s := testAgentProcess(t)
	data, _ := json.Marshal(map[string]any{"sessionId": "live-id", "pid": s.PID, "status": "idle"})
	saved, err := parseClaude(data)
	if err != nil || saved.Active {
		t.Fatalf("saved generationless metadata considered live: %#v err=%v", saved, err)
	}
	registry, err := parseClaudeRecord(data, true)
	if err != nil || !registry.Active || registry.ProcessStart != s.ProcessStart {
		t.Fatalf("provider's fresh registry rejected: %#v err=%v", registry, err)
	}
	// Numeric procStart records should receive the same PID-reuse protection.
	start, err := strconv.ParseInt(s.ProcessStart, 10, 64)
	if err == nil {
		data, _ = json.Marshal(map[string]any{"sessionId": "live-id", "pid": s.PID, "status": "idle", "procStart": start})
		matched, err := parseClaude(data)
		if err != nil || !matched.Active {
			t.Fatalf("numeric process generation rejected: %#v err=%v", matched, err)
		}
		data, _ = json.Marshal(map[string]any{"sessionId": "live-id", "pid": s.PID, "status": "idle", "procStart": start + 1})
		mismatched, err := parseClaude(data)
		if err != nil || mismatched.Active {
			t.Fatalf("numeric PID reuse not rejected: %#v err=%v", mismatched, err)
		}
	}
}

func TestProviderNameInTaskTextIsNotProcessIdentity(t *testing.T) {
	if providerProcess("/usr/bin/bash -c echo unrelated-task mentions codex", bridge.Codex) {
		t.Fatal("arbitrary task text treated as provider executable")
	}
}
