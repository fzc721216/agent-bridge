package managed

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	bridge "github.com/fuzicheng/agent-bridge"
)

// These tests use an executable protocol simulator, never a real coding agent.
// In particular, no installed authentication, provider configuration or model
// endpoint is used while exercising subprocess and stream lifecycle behavior.
const fakeAgent = `#!/usr/bin/env python3
import json, os, sys, threading, time

mode = os.environ.get("AGENT_BRIDGE_FAKE_MODE", "normal")
log_path = os.environ["AGENT_BRIDGE_FAKE_LOG"]
guard = threading.Lock()
active = False
counter = 0

def log(kind, **fields):
    with guard:
        with open(log_path, "a") as f:
            f.write(json.dumps(dict(kind=kind, **fields)) + "\n")

def send(value):
    with guard:
        sys.stdout.write(json.dumps(value) + "\n")
        sys.stdout.flush()

log("start", pid=os.getpid(), args=sys.argv[1:])
if mode == "hang":
    time.sleep(60)
    sys.exit(0)
is_codex = "app-server" in sys.argv
if not is_codex:
    args = sys.argv[1:]
    sid = args[args.index("--session-id") + 1] if "--session-id" in args else "fake-claude-thread"
    send({"type":"system", "subtype":"init", "session_id":sid})
else:
    sid = "fake-codex-thread"

def complete(turn_id):
    global active
    time.sleep(0.12)
    active = False
    log("complete", turn_id=turn_id)
    if is_codex:
        send({"method":"item/completed", "params":{"threadId":sid,"item":{"type":"agentMessage","text":"fake answer"}}})
        send({"method":"turn/completed", "params":{"threadId":sid,"turn":{"id":turn_id,"status":"completed"}}})
    else:
        send({"type":"assistant", "session_id":sid,"message":{"role":"assistant","content":[{"type":"text","text":"fake answer"}]}})
        send({"type":"result", "subtype":"success", "is_error":False,"session_id":sid,"result":"fake answer"})

for line in sys.stdin:
    try:
        item = json.loads(line)
    except Exception:
        log("bad_input", line=line)
        continue
    log("input", value=item)
    if is_codex:
        method = item.get("method", "")
        ident = item.get("id")
        if method == "initialize":
            if mode == "startup-error":
                send({"id":ident,"error":{"code":-32000,"message":"simulated startup failure"}})
                break
            send({"id":ident,"result":{"userAgent":"fake"}})
        elif method == "thread/start":
            send({"id":ident,"result":{"thread":{"id":sid}}})
        elif method == "turn/start":
            if active:
                log("overlap")
                send({"id":ident,"error":{"code":-32001,"message":"already working"}})
                continue
            active = True
            counter += 1
            turn_id = "turn-" + str(counter)
            log("turn", text=item["params"]["input"][0]["text"])
            send({"id":ident,"result":{"turn":{"id":turn_id,"status":"inProgress"}}})
            if mode == "approval":
                send({"method":"item/commandExecution/requestApproval","id":900,"params":{"threadId":sid,"turnId":turn_id,"itemId":"command-1","command":"touch forbidden"}})
            else:
                threading.Thread(target=complete,args=(turn_id,),daemon=True).start()
        elif ident == 900:
            log("approval_response", value=item)
            threading.Thread(target=complete,args=("turn-1",),daemon=True).start()
    elif item.get("type") == "user":
        if active:
            log("overlap")
        active = True
        counter += 1
        log("turn", text=item.get("message",{}).get("content"))
        threading.Thread(target=complete,args=("turn-"+str(counter),),daemon=True).start()
`

type fakeRecord struct {
	Kind  string         `json:"kind"`
	PID   int            `json:"pid"`
	Args  []string       `json:"args"`
	Text  any            `json:"text"`
	Value map[string]any `json:"value"`
}

func fakeOptions(t *testing.T, mode string) (Options, string) {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-agent")
	if err := os.WriteFile(bin, []byte(fakeAgent), 0700); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "calls.jsonl")
	t.Setenv("AGENT_BRIDGE_FAKE_MODE", mode)
	t.Setenv("AGENT_BRIDGE_FAKE_LOG", logPath)
	return Options{CodexBin: bin, ClaudeBin: bin, StateDir: dir}, logPath
}

func fakeRecords(t *testing.T, name string) []fakeRecord {
	t.Helper()
	f, err := os.Open(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var records []fakeRecord
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var r fakeRecord
		// A writer can be in the middle of appending the final record.
		if json.Unmarshal(scanner.Bytes(), &r) == nil {
			records = append(records, r)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return records
}

func eventually(t *testing.T, why string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", why)
}

func countKind(records []fakeRecord, kind string) int {
	n := 0
	for _, r := range records {
		if r.Kind == kind {
			n++
		}
	}
	return n
}

func TestCodexManagedUsesHandshakeAndSerializesTurns(t *testing.T) {
	opts, logPath := fakeOptions(t, "normal")
	m := New(opts)
	t.Cleanup(func() { _ = m.Close() })
	s, err := m.Start(context.Background(), StartRequest{Provider: bridge.Codex, CWD: t.TempDir(), Name: "research"})
	if err != nil {
		t.Fatal(err)
	}
	if s.NativeID != "fake-codex-thread" || !s.Active || s.Capabilities.Transport != bridge.Managed {
		t.Fatalf("unexpected session: %+v", s)
	}
	texts := []string{"line one\nline two $(touch never) `literal`", "second task"}
	for _, text := range texts {
		if err := m.Send(context.Background(), s, text); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "two completed serial turns", func() bool { return countKind(fakeRecords(t, logPath), "complete") == 2 })
	records := fakeRecords(t, logPath)
	if countKind(records, "overlap") != 0 {
		t.Fatal("sent overlapping turns to a busy provider")
	}
	var methods []string
	var got []string
	for _, r := range records {
		if r.Kind == "input" {
			if method, ok := r.Value["method"].(string); ok {
				methods = append(methods, method)
			}
		}
		if r.Kind == "turn" {
			got = append(got, r.Text.(string))
		}
	}
	if len(methods) < 3 || methods[0] != "initialize" || methods[1] != "initialized" || methods[2] != "thread/start" {
		t.Fatalf("handshake methods: %v", methods)
	}
	if len(got) != 2 || got[0] != texts[0] || got[1] != texts[1] {
		t.Fatalf("messages changed: %#v", got)
	}
}

func TestClaudeManagedUsesStreamJSONAndSafePermissionMode(t *testing.T) {
	opts, logPath := fakeOptions(t, "normal")
	m := New(opts)
	t.Cleanup(func() { _ = m.Close() })
	s, err := m.Start(context.Background(), StartRequest{Provider: bridge.Claude, CWD: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	text := "Need schema\nquote \" and ; literal"
	if err := m.Send(context.Background(), s, text); err != nil {
		t.Fatal(err)
	}
	eventually(t, "Claude result", func() bool { return countKind(fakeRecords(t, logPath), "complete") == 1 })
	records := fakeRecords(t, logPath)
	var args []string
	var received any
	for _, r := range records {
		if r.Kind == "start" {
			args = r.Args
		}
		if r.Kind == "turn" {
			received = r.Text
		}
	}
	joined := " " + strings.Join(args, " ") + " "
	for _, flag := range []string{" --print ", " --verbose ", " --input-format stream-json ", " --output-format stream-json ", " --permission-prompts none "} {
		if !strings.Contains(joined, flag) {
			t.Fatalf("missing %q in %v", flag, args)
		}
	}
	if strings.Contains(joined, "bypass") || strings.Contains(joined, "skip-permissions") {
		t.Fatalf("permissions bypassed: %v", args)
	}
	if received != text {
		t.Fatalf("input changed: %#v", received)
	}
}

func TestManagedCallerCancellationDoesNotKillStartedSession(t *testing.T) {
	opts, logPath := fakeOptions(t, "normal")
	m := New(opts)
	t.Cleanup(func() { _ = m.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	s, err := m.Start(ctx, StartRequest{Provider: bridge.Codex, CWD: t.TempDir()})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	if err := m.Send(context.Background(), s, "after request cancellation"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "turn after caller cancellation", func() bool { return countKind(fakeRecords(t, logPath), "complete") == 1 })
	if got := m.Sessions(); len(got) != 1 || !got[0].Active {
		t.Fatalf("session died with caller: %+v", got)
	}
}

func TestManagedStartupCancellationReapsProcess(t *testing.T) {
	opts, logPath := fakeOptions(t, "hang")
	m := New(opts)
	t.Cleanup(func() { _ = m.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := m.Start(ctx, StartRequest{Provider: bridge.Codex, CWD: t.TempDir()})
	if err == nil {
		t.Fatal("startup unexpectedly succeeded")
	}
	var pid int
	for _, r := range fakeRecords(t, logPath) {
		if r.Kind == "start" {
			pid = r.PID
		}
	}
	if pid == 0 {
		t.Fatal("fake process did not start")
	}
	eventually(t, "failed startup process reaping", func() bool { return syscall.Kill(pid, 0) == syscall.ESRCH })
}

func TestManagedStartupErrorAndRepeatedClose(t *testing.T) {
	opts, _ := fakeOptions(t, "startup-error")
	m := New(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := m.Start(ctx, StartRequest{Provider: bridge.Codex, CWD: t.TempDir()})
	if err == nil {
		t.Fatal("expected startup protocol error")
	}
	for i := 0; i < 3; i++ {
		if err := m.Close(); err != nil {
			t.Fatalf("Close %d: %v", i, err)
		}
	}
	if _, err := m.Start(context.Background(), StartRequest{Provider: bridge.Codex, CWD: t.TempDir()}); err == nil {
		t.Fatal("closed manager accepted a new session")
	}
}

func TestManagedCodexNeverApprovesServerRequests(t *testing.T) {
	opts, logPath := fakeOptions(t, "approval")
	m := New(opts)
	t.Cleanup(func() { _ = m.Close() })
	s, err := m.Start(context.Background(), StartRequest{Provider: bridge.Codex, CWD: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Send(context.Background(), s, "run a permission-gated operation"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "approval rejection", func() bool { return countKind(fakeRecords(t, logPath), "approval_response") == 1 })
	for _, r := range fakeRecords(t, logPath) {
		if r.Kind != "approval_response" {
			continue
		}
		b, _ := json.Marshal(r.Value)
		text := strings.ToLower(string(b))
		if strings.Contains(text, "accept") || strings.Contains(text, "approve") || strings.Contains(text, `"allow":true`) {
			t.Fatalf("unsafe approval response: %s", text)
		}
		if !strings.Contains(text, "decline") && !strings.Contains(text, "deny") && !strings.Contains(text, "reject") && !strings.Contains(text, "error") && !strings.Contains(text, "abort") {
			t.Fatalf("no denial in response: %s", text)
		}
	}
}

func TestCodexTurnCompletionMustMatchThreadAndTurn(t *testing.T) {
	m := New(Options{})
	t.Cleanup(func() { _ = m.Close() })
	p := &process{manager: m, session: bridge.Session{ID: "codex:thread-1", NativeID: "thread-1", Provider: bridge.Codex, Active: true, Status: bridge.Working}, awaiting: true, turnID: "turn-1", turnDone: make(chan struct{}, 1)}
	for _, params := range []string{
		`{"threadId":"another-thread","turn":{"id":"turn-1","status":"completed"}}`,
		`{"threadId":"thread-1","turn":{"id":"another-turn","status":"completed"}}`,
		`{"threadId":"thread-1","turn":{"status":"completed"}}`,
	} {
		p.handleCodex(wireMessage{Method: "turn/completed", Params: json.RawMessage(params)})
		if !p.awaiting {
			t.Fatal("unrelated or incomplete completion released the active turn")
		}
	}
	p.handleCodex(wireMessage{Method: "turn/completed", Params: json.RawMessage(`{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}`)})
	if p.awaiting || p.snapshot().Status != bridge.Idle {
		t.Fatal("matching completion did not finish turn")
	}
}

func TestClaudeIgnoresOtherSessionsAndPreservesPermissionDenials(t *testing.T) {
	m := New(Options{})
	t.Cleanup(func() { _ = m.Close() })
	p := &process{manager: m, session: bridge.Session{ID: "claude:thread-1", NativeID: "thread-1", Provider: bridge.Claude, Active: true, Status: bridge.Working}, awaiting: true, turnDone: make(chan struct{}, 1)}
	p.handleClaude(wireMessage{Type: "result", Subtype: "success", SessionID: "another-thread"})
	if !p.awaiting {
		t.Fatal("another session result finished this session")
	}
	p.handleClaude(wireMessage{Type: "result", Subtype: "success", SessionID: "thread-1", ParentToolUseID: json.RawMessage(`"subagent-tool"`)})
	if !p.awaiting {
		t.Fatal("subagent result finished the parent session")
	}
	p.handleClaude(wireMessage{Type: "result", Subtype: "success", SessionID: "thread-1", PermissionDenials: []json.RawMessage{json.RawMessage(`{"tool_name":"Bash"}`)}})
	if p.awaiting || p.snapshot().Status != bridge.Blocked {
		t.Fatal("permission denial was falsely reported as successful idle")
	}
}
