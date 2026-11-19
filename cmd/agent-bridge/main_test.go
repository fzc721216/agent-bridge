package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	bridge "github.com/fuzicheng/agent-bridge"
)

type fakeRPC struct {
	method      string
	params      map[string]any
	credentials *bridge.Credentials
	calls       int
}

func (f *fakeRPC) Call(_ context.Context, method string, params any, c *bridge.Credentials, out any) error {
	f.calls++
	f.method = method
	f.credentials = c
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &f.params); err != nil {
		return err
	}
	if out != nil {
		return json.Unmarshal([]byte(`{"ok":true}`), out)
	}
	return nil
}

func testApp(t *testing.T, rpc *fakeRPC) (*app, *bytes.Buffer) {
	t.Helper()
	t.Setenv("AGENT_BRIDGE_CREDENTIALS", "")
	var out bytes.Buffer
	a := &app{in: strings.NewReader(""), out: &out, errOut: io.Discard, client: rpc, isTerminal: func() bool { return false }}
	return a, &out
}

func writeCredentials(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "member.json")
	if err := os.WriteFile(p, []byte(`{"group_id":"g","session_id":"codex:a","token":"secret"}`), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestGroupCreateRequiresConfirmation(t *testing.T) {
	rpc := &fakeRPC{}
	a, _ := testApp(t, rpc)
	err := a.run(context.Background(), []string{"group", "create", "team", "--members", "codex:a,claude:b", "--allow-tmux"})
	if err == nil || !strings.Contains(err.Error(), "--yes") {
		t.Fatalf("expected explicit confirmation requirement: %v", err)
	}
	if rpc.calls != 0 {
		t.Fatal("group was created before confirmation")
	}
}

func TestGroupCreateConfirmedSelection(t *testing.T) {
	rpc := &fakeRPC{}
	a, _ := testApp(t, rpc)
	if err := a.run(context.Background(), []string{"group", "create", "team", "--members", "codex:a, claude:b,codex:a", "--allow-tmux", "--yes"}); err != nil {
		t.Fatal(err)
	}
	if rpc.method != "group.create" || rpc.params["name"] != "team" || rpc.params["allow_tmux"] != true {
		t.Fatalf("wrong RPC: %#v", rpc)
	}
	ids := rpc.params["members"].([]any)
	if len(ids) != 2 || ids[0] != "codex:a" || ids[1] != "claude:b" {
		t.Fatalf("wrong selected members: %#v", ids)
	}
}

func TestInvitationDeliveryRequiresConfirmation(t *testing.T) {
	rpc := &fakeRPC{}
	a, _ := testApp(t, rpc)
	if err := a.run(context.Background(), []string{"invite", "g", "codex:a", "--deliver"}); err == nil {
		t.Fatal("missing delivery confirmation")
	}
	if rpc.calls != 0 {
		t.Fatal("invitation sent before confirmation")
	}
}

func TestAgentAskPreservesLiteralBodyAndIdentity(t *testing.T) {
	rpc := &fakeRPC{}
	a, _ := testApp(t, rpc)
	p := writeCredentials(t)
	body := "$HOME `printf hacked` $(touch /tmp/never-run)\n请给我表结构"
	if err := a.run(context.Background(), []string{"--credentials", p, "ask", "claude:b", "--body", body, "--ttl-seconds", "120", "--idempotency-key", "q1"}); err != nil {
		t.Fatal(err)
	}
	if rpc.method != "ask" || rpc.params["body"] != body || rpc.params["to"] != "claude:b" {
		t.Fatalf("wrong request %#v", rpc.params)
	}
	if rpc.params["ttl_seconds"] != float64(120) || rpc.params["group_id"] != "g" {
		t.Fatalf("wrong request %#v", rpc.params)
	}
	if rpc.credentials == nil || rpc.credentials.SessionID != "codex:a" {
		t.Fatal("member identity missing")
	}
}

func TestOwnerCannotUseAgentMethodsWithoutCredentials(t *testing.T) {
	for _, args := range [][]string{{"join"}, {"peers"}, {"inbox"}, {"heartbeat"}, {"ask", "claude:b", "--body", "schema?"}, {"reply", "r", "--body", "answer"}, {"request-status", "r"}, {"state", "--status", "working"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			rpc := &fakeRPC{}
			a, _ := testApp(t, rpc)
			if err := a.run(context.Background(), args); err == nil {
				t.Fatal("missing credentials accepted")
			}
			if rpc.calls != 0 {
				t.Fatal("RPC called without credentials")
			}
		})
	}
}

func TestCredentialsFileMustBePrivate(t *testing.T) {
	rpc := &fakeRPC{}
	a, _ := testApp(t, rpc)
	p := writeCredentials(t)
	if err := os.Chmod(p, 0644); err != nil {
		t.Fatal(err)
	}
	err := a.run(context.Background(), []string{"--credentials", p, "peers"})
	if err == nil || !strings.Contains(err.Error(), "private") {
		t.Fatalf("expected private credentials error: %v", err)
	}
	if rpc.calls != 0 {
		t.Fatal("insecure credentials used")
	}
}

func TestReplyIncludesSources(t *testing.T) {
	rpc := &fakeRPC{}
	a, _ := testApp(t, rpc)
	p := writeCredentials(t)
	if err := a.run(context.Background(), []string{"--credentials", p, "reply", "r1", "--body", "field is optional", "--sources", "/tmp/spec.md:3,https://example.test/schema"}); err != nil {
		t.Fatal(err)
	}
	if rpc.params["request_id"] != "r1" || len(rpc.params["sources"].([]any)) != 2 {
		t.Fatalf("wrong reply %#v", rpc.params)
	}
}

func TestOwnerSendExplicitGroup(t *testing.T) {
	rpc := &fakeRPC{}
	a, _ := testApp(t, rpc)
	if err := a.run(context.Background(), []string{"send", "--to", "codex:a", "--body", "hello"}); err == nil {
		t.Fatal("owner send accepted without a group")
	}
	if rpc.calls != 0 {
		t.Fatal("RPC called before validation")
	}
}

func TestParseFlagsAfterPositionalsAndLiteralValue(t *testing.T) {
	f := flag.NewFlagSet("test", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	var body string
	var yes bool
	f.StringVar(&body, "body", "", "")
	f.BoolVar(&yes, "yes", false, "")
	if err := parseFlags(f, []string{"target", "--body", "--literal-is-body", "--yes"}); err != nil {
		t.Fatal(err)
	}
	if body != "--literal-is-body" || !yes || len(f.Args()) != 1 || f.Args()[0] != "target" {
		t.Fatalf("bad parse body=%q yes=%t args=%v", body, yes, f.Args())
	}
}

func TestHelpStatesLocalProtocol(t *testing.T) {
	rpc := &fakeRPC{}
	a, out := testApp(t, rpc)
	if err := a.run(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "not A2A") || !strings.Contains(out.String(), "ask SESSION") {
		t.Fatalf("help missing actual protocol/collaboration commands: %s", out)
	}
}
