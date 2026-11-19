package local

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bridge "github.com/fuzicheng/agent-bridge"
)

func fixtureServer(t *testing.T) (*Server, bridge.Group, []bridge.Credentials) {
	t.Helper()
	dir := t.TempDir()
	b, err := bridge.Open(filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	s := &Server{Broker: b, StateDir: dir, BinaryPath: "/absolute/path with spaces/agent-bridge"}
	ctx := context.Background()
	ss := []bridge.Session{
		{ID: "codex:a", NativeID: "a", Provider: bridge.Codex, Active: true, Status: bridge.Idle, Capabilities: bridge.Capabilities{Transport: bridge.ObserveOnly}},
		{ID: "claude:b", NativeID: "b", Provider: bridge.Claude, Active: true, Status: bridge.Idle, Capabilities: bridge.Capabilities{Transport: bridge.ObserveOnly}},
	}
	if err := b.UpsertSessions(ctx, ss); err != nil {
		t.Fatal(err)
	}
	g, err := b.CreateGroup(ctx, "test", []string{ss[0].ID, ss[1].ID}, false)
	if err != nil {
		t.Fatal(err)
	}
	creds := make([]bridge.Credentials, 2)
	for i, v := range ss {
		creds[i], err = b.Invite(ctx, g.ID, v.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	return s, g, creds
}

func handle(t *testing.T, s *Server, method string, params any, c *bridge.Credentials) (any, error) {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return s.Handle(context.Background(), Request{ID: bridge.NewID(), Method: method, Params: raw, Credentials: c})
}

func TestGatewayHandshakeAskReplyAndCredentialScope(t *testing.T) {
	s, g, c := fixtureServer(t)
	if _, err := handle(t, s, "peers", nil, &c[0]); err == nil {
		t.Fatal("unjoined member used tools")
	}
	for i := range c {
		if _, err := handle(t, s, "join", map[string]any{"topics": []string{"schema"}}, &c[i]); err != nil {
			t.Fatal(err)
		}
	}
	for _, method := range []string{"status", "sessions", "discover", "invite", "group.create", "managed.start"} {
		if _, err := handle(t, s, method, nil, &c[0]); err == nil {
			t.Fatalf("member used owner method %s", method)
		}
	}
	if _, err := handle(t, s, "peers", map[string]string{"group_id": "other"}, &c[0]); err == nil {
		t.Fatal("cross group params allowed")
	}
	value, err := handle(t, s, "ask", map[string]string{"to": c[1].SessionID, "body": "What does the field mean?"}, &c[0])
	if err != nil {
		t.Fatal(err)
	}
	q := value.(bridge.Message)
	if q.From != c[0].SessionID || q.Type != "request" || q.GroupID != g.ID {
		t.Fatalf("bad identity: %+v", q)
	}
	if _, err := handle(t, s, "reply", map[string]string{"request_id": q.ID, "body": "forged answer"}, &c[0]); err == nil {
		t.Fatal("asker replied to its own request")
	}
	if _, err := handle(t, s, "inbox", nil, &c[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := handle(t, s, "reply", map[string]any{"request_id": q.ID, "body": "This is the order state", "sources": []string{"schema.sql:3"}}, &c[1]); err != nil {
		t.Fatal(err)
	}
	status, err := handle(t, s, "request.status", map[string]string{"request_id": q.ID}, &c[0])
	if err != nil {
		t.Fatal(err)
	}
	if status.(bridge.Message).State != "answered" {
		t.Fatal("request not answered")
	}
	value, err = handle(t, s, "inbox", nil, &c[0])
	if err != nil {
		t.Fatal(err)
	}
	answers := value.([]bridge.Message)
	if len(answers) != 1 || answers[0].RequestID != q.ID || len(answers[0].Sources) != 1 {
		t.Fatalf("bad answer: %+v", answers)
	}
}

func TestInvitationContainsPrivatePathAndRequiresActualJoin(t *testing.T) {
	s, g, c := fixtureServer(t)
	value, err := handle(t, s, "invite", map[string]any{"group_id": g.ID, "session_id": c[0].SessionID}, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(value)
	var invitation struct {
		CredentialsPath string `json:"credentials_path"`
		Prompt          string `json:"prompt"`
		Delivered       bool   `json:"delivered"`
	}
	if err := json.Unmarshal(raw, &invitation); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(invitation.CredentialsPath)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("private credentials: %v %v", info, err)
	}
	var fresh bridge.Credentials
	data, _ := os.ReadFile(invitation.CredentialsPath)
	_ = json.Unmarshal(data, &fresh)
	if invitation.Delivered || strings.Contains(invitation.Prompt, fresh.Token) {
		t.Fatal("invitation leaked a token or claimed delivery")
	}
	if !strings.Contains(invitation.Prompt, "'/absolute/path with spaces/agent-bridge'") {
		t.Fatal("command not shell quoted")
	}
	m, err := s.Broker.Authenticate(context.Background(), fresh)
	if err != nil || m.Joined {
		t.Fatalf("invite treated as join: %+v %v", m, err)
	}
	if _, err := s.Broker.Authenticate(context.Background(), c[0]); err == nil {
		t.Fatal("old credentials not invalidated")
	}
}

func TestUnixClientServerLifecycle(t *testing.T) {
	s, _, _ := fixtureServer(t)
	socket := filepath.Join(t.TempDir(), "test.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		s.serveConnection(context.Background(), conn)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var snap Snapshot
	if err := (Client{Socket: socket}).Call(ctx, "status", nil, nil, &snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.Sessions) != 2 || len(snap.Groups) != 1 {
		t.Fatalf("bad snapshot: %+v", snap)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("server did not finish")
	}
}

func TestDaemonPreservesNonSocketAndCancellation(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "agent-bridge.sock")
	if err := os.WriteFile(socket, []byte("keep me"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := RunDaemon(context.Background(), dir); err == nil {
		t.Fatal("replaced regular file")
	}
	raw, _ := os.ReadFile(socket)
	if string(raw) != "keep me" {
		t.Fatal("file overwritten")
	}
	if err := os.Remove(socket); err != nil {
		t.Fatal(err)
	}
	// Empty provider directories plus nonexistent provider executables ensure
	// this daemon lifecycle test never accesses an installed model or live session.
	t.Setenv("CODEX_HOME", filepath.Join(dir, "codex"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(dir, "claude"))
	t.Setenv("PATH", dir)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RunDaemon(ctx, dir) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("daemon never listened")
		}
		time.Sleep(10 * time.Millisecond)
	}
	probe, c := context.WithTimeout(context.Background(), time.Second)
	var snap Snapshot
	err := (Client{Socket: socket}).Call(probe, "status", nil, nil, &snap)
	c()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := RunDaemon(ctx, dir); err == nil {
		cancel()
		t.Fatal("duplicate daemon started")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("daemon failed to shut down")
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Fatal("stale socket remained")
	}
}
