package bridge

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testBroker(t *testing.T) (*Broker, Group, Credentials, Credentials) {
	t.Helper()
	b, err := Open(filepath.Join(t.TempDir(), "bridge.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	ctx := context.Background()
	if err = b.UpsertSessions(ctx, []Session{
		{ID: "codex:a", Provider: Codex, NativeID: "a", Active: true, Status: Idle, Capabilities: Capabilities{CanSend: true, Transport: CodexQueue}},
		{ID: "claude:b", Provider: Claude, NativeID: "b", Active: true, Status: Idle, Capabilities: Capabilities{CanSend: true, Transport: Managed}},
		{ID: "codex:c", Provider: Codex, NativeID: "c", Status: Idle},
	}); err != nil {
		t.Fatal(err)
	}
	g, err := b.CreateGroup(ctx, "development", []string{"codex:a", "claude:b", "codex:c"}, false)
	if err != nil {
		t.Fatal(err)
	}
	a, err := b.Invite(ctx, g.ID, "codex:a")
	if err != nil {
		t.Fatal(err)
	}
	c, err := b.Invite(ctx, g.ID, "claude:b")
	if err != nil {
		t.Fatal(err)
	}
	for _, creds := range []Credentials{a, c} {
		if _, err = b.Join(ctx, creds, []string{"requirements"}); err != nil {
			t.Fatal(err)
		}
	}
	return b, g, a, c
}

func TestAskReplyLifecycleAndScopedAuthentication(t *testing.T) {
	b, g, a, c := testBroker(t)
	ctx := context.Background()
	if _, err := b.Authenticate(ctx, Credentials{GroupID: g.ID, SessionID: c.SessionID, Token: a.Token}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("forged session token: %v", err)
	}
	if _, err := b.Authenticate(ctx, Credentials{GroupID: "another-group", SessionID: a.SessionID, Token: a.Token}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("cross-group token: %v", err)
	}
	q, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: a.SessionID, To: c.SessionID, Type: "request", Body: "What is the retry limit?"})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := b.PendingDeliveries(ctx, 100)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	if err = b.RecordDelivery(ctx, q.ID, "sending", ""); err != nil {
		t.Fatal(err)
	}
	if err = b.RecordDelivery(ctx, q.ID, "accepted", ""); err != nil {
		t.Fatal(err)
	}
	state, err := b.RequestStatus(ctx, g.ID, a.SessionID, q.ID)
	if err != nil || state.State != "queued" {
		t.Fatalf("queue acceptance must not imply agent read: %+v %v", state, err)
	}
	if _, err = b.Send(ctx, SendRequest{GroupID: g.ID, From: a.SessionID, Type: "reply", RequestID: q.ID, Body: "forged"}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("asker impersonated answerer: %v", err)
	}
	if _, err = b.RequestStatus(ctx, g.ID, "codex:c", q.ID); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unrelated member read request: %v", err)
	}
	inbox, err := b.Inbox(ctx, g.ID, c.SessionID, true)
	if err != nil || len(inbox) != 1 || inbox[0].State != "read" {
		t.Fatalf("inbox=%+v err=%v", inbox, err)
	}
	reply, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: c.SessionID, Type: "reply", RequestID: q.ID, Body: "3 retries", Sources: []string{"docs/retries.md:12"}})
	if err != nil {
		t.Fatal(err)
	}
	if reply.To != a.SessionID || reply.Delivery != "inbox" {
		t.Fatalf("reply route=%+v", reply)
	}
	pending, err = b.PendingDeliveries(ctx, 100)
	if err != nil || len(pending) != 0 {
		t.Fatalf("reply must not wake another turn: %+v %v", pending, err)
	}
	state, err = b.RequestStatus(ctx, g.ID, a.SessionID, q.ID)
	if err != nil || state.State != "answered" {
		t.Fatalf("request=%+v err=%v", state, err)
	}
	inbox, err = b.Inbox(ctx, g.ID, a.SessionID, true)
	if err != nil || len(inbox) != 1 || inbox[0].RequestID != q.ID || len(inbox[0].Sources) != 1 {
		t.Fatalf("reply inbox=%+v err=%v", inbox, err)
	}
	if _, err = b.Send(ctx, SendRequest{GroupID: g.ID, From: c.SessionID, Type: "reply", RequestID: q.ID, Body: "second answer"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate answer: %v", err)
	}
}

func TestMembershipInvitationAndOwnerBoundaries(t *testing.T) {
	b, g, a, c := testBroker(t)
	ctx := context.Background()
	if _, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: a.SessionID, To: "codex:c", Type: "request", Body: "question"}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unjoined target: %v", err)
	}
	if _, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: "user", To: c.SessionID, Type: "request", Body: "question"}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("owner impersonating an agent: %v", err)
	}
	rotated, err := b.Invite(ctx, g.ID, a.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Authenticate(ctx, a); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("rotated token still works: %v", err)
	}
	m, err := b.Authenticate(ctx, rotated)
	if err != nil || m.Joined {
		t.Fatalf("invite is not a handshake: %+v %v", m, err)
	}
	if _, err = b.Join(ctx, rotated, nil); err != nil {
		t.Fatal(err)
	}
	q, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: rotated.SessionID, To: c.SessionID, Type: "request", Body: "pending question"})
	if err != nil {
		t.Fatal(err)
	}
	if err = b.RemoveMember(ctx, g.ID, c.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Authenticate(ctx, c); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("removed credentials still work: %v", err)
	}
	state, err := b.RequestStatus(ctx, g.ID, a.SessionID, q.ID)
	if err != nil || state.State != "failed" {
		t.Fatalf("removed recipient request=%+v %v", state, err)
	}
}

func TestIdempotencyExpiryAndStaleState(t *testing.T) {
	b, g, a, c := testBroker(t)
	ctx := context.Background()
	r := SendRequest{GroupID: g.ID, From: a.SessionID, To: c.SessionID, Type: "request", Body: "field meaning", IdempotencyKey: "request-1", TTLSeconds: 1}
	first, err := b.Send(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	second, err := b.Send(ctx, r)
	if err != nil || first.ID != second.ID {
		t.Fatalf("not idempotent: %+v %v", second, err)
	}
	r.Body = "different question"
	if _, err = b.Send(ctx, r); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting reuse: %v", err)
	}
	if err = b.PublishState(ctx, a, TaskState{Status: Working, Task: "reviewing schema", Summary: "checking fields"}); err != nil {
		t.Fatal(err)
	}
	if err = b.Sweep(ctx, time.Now().Add(10*time.Minute), 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	state, err := b.RequestStatus(ctx, g.ID, a.SessionID, first.ID)
	if err != nil || state.State != "expired" {
		t.Fatalf("not expired: %+v %v", state, err)
	}
	inbox, err := b.Inbox(ctx, g.ID, c.SessionID, false)
	if err != nil || len(inbox) != 0 {
		t.Fatalf("expired leaked into inbox: %+v %v", inbox, err)
	}
	if err = b.Heartbeat(ctx, a); err != nil {
		t.Fatal(err)
	}
	members, err := b.Members(ctx, g.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range members {
		if m.Session.ID == a.SessionID && (m.State.Status != Stale || m.State.Task != "reviewing schema") {
			t.Fatalf("heartbeat changed semantic state: %+v", m)
		}
	}
}

func TestPersistenceConcurrentSendAndReadOnlyDiscoveryUpdates(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bridge.sqlite")
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if b != nil {
			b.Close()
		}
	}()
	sessions := []Session{{ID: "codex:a", Provider: Codex, NativeID: "a", Status: Idle}, {ID: "claude:b", Provider: Claude, NativeID: "b", Status: Idle}}
	if err = b.UpsertSessions(ctx, sessions); err != nil {
		t.Fatal(err)
	}
	g, err := b.CreateGroup(ctx, "persistence", []string{"codex:a", "claude:b"}, false)
	if err != nil {
		t.Fatal(err)
	}
	a, err := b.Invite(ctx, g.ID, "codex:a")
	if err != nil {
		t.Fatal(err)
	}
	c, err := b.Invite(ctx, g.ID, "claude:b")
	if err != nil {
		t.Fatal(err)
	}
	for _, creds := range []Credentials{a, c} {
		if _, err = b.Join(ctx, creds, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err = b.PublishState(ctx, a, TaskState{Status: Working, Task: "task survives discovery"}); err != nil {
		t.Fatal(err)
	}
	if err = b.UpsertSessions(ctx, sessions); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: a.SessionID, To: c.SessionID, Type: "request", Body: "concurrent question"})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	b = nil
	b, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Authenticate(ctx, a); err != nil {
		t.Fatalf("credentials not persisted: %v", err)
	}
	inbox, err := b.Inbox(ctx, g.ID, c.SessionID, false)
	if err != nil || len(inbox) != 20 {
		t.Fatalf("restored inbox count=%d err=%v", len(inbox), err)
	}
	members, err := b.Members(ctx, g.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range members {
		if m.Session.ID == a.SessionID && (!m.Joined || m.State.Task != "task survives discovery") {
			t.Fatalf("discovery reset task state: %+v", m)
		}
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("database permissions: %v %v", info, err)
	}
	events, err := b.Events(ctx, g.ID, 0, 100)
	if err != nil || len(events) < 20 {
		t.Fatalf("events not persisted: %d %v", len(events), err)
	}
}

func TestReadSuppressesWakeAndClaimCannotBeDuplicated(t *testing.T) {
	b, g, a, c := testBroker(t)
	ctx := context.Background()
	ask := func() Message {
		t.Helper()
		m, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: a.SessionID, To: c.SessionID, Type: "request", Body: "question"})
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	first := ask()
	if _, err := b.Inbox(ctx, g.ID, c.SessionID, true); err != nil {
		t.Fatal(err)
	}
	if pending, err := b.PendingDeliveries(ctx, 100); err != nil || len(pending) != 0 {
		t.Fatalf("already-read message would be notified: %+v %v", pending, err)
	}
	if err := b.RecordDelivery(ctx, first.ID, "sending", ""); !errors.Is(err, ErrConflict) {
		t.Fatalf("claimed a read message: %v", err)
	}
	second := ask()
	if err := b.RecordDelivery(ctx, second.ID, "sending", ""); err != nil {
		t.Fatal(err)
	}
	if err := b.RecordDelivery(ctx, second.ID, "sending", ""); !errors.Is(err, ErrConflict) {
		t.Fatalf("second dispatcher claimed same write: %v", err)
	}
	if _, err := b.Inbox(ctx, g.ID, c.SessionID, true); err != nil {
		t.Fatal(err)
	}
	if err := b.RecordDelivery(ctx, second.ID, "failed", "transport disconnected after write"); err != nil {
		t.Fatal(err)
	}
	status, err := b.RequestStatus(ctx, g.ID, a.SessionID, second.ID)
	if err != nil || status.State != "read" || status.ReadAt == nil {
		t.Fatalf("notification result regressed read acknowledgement: %+v %v", status, err)
	}
}

func TestRestartDoesNotRetryAmbiguousDelivery(t *testing.T) {
	b, g, a, c := testBroker(t)
	ctx := context.Background()
	var seq int
	var database, path string
	if err := b.db.QueryRowContext(ctx, `PRAGMA database_list`).Scan(&seq, &database, &path); err != nil {
		t.Fatal(err)
	}
	m, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: a.SessionID, To: c.SessionID, Type: "request", Body: "question"})
	if err != nil {
		t.Fatal(err)
	}
	if err = b.RecordDelivery(ctx, m.ID, "sending", ""); err != nil {
		t.Fatal(err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	status, err := reopened.RequestStatus(ctx, g.ID, a.SessionID, m.ID)
	if err != nil || status.Delivery != "unknown" || status.State != "queued" {
		t.Fatalf("ambiguous send recovery: %+v %v", status, err)
	}
	if pending, err := reopened.PendingDeliveries(ctx, 100); err != nil || len(pending) != 0 {
		t.Fatalf("ambiguous send would be retried: %+v %v", pending, err)
	}
}

func TestPendingRecipientsCannotStarveReadyMessages(t *testing.T) {
	b, g, a, c := testBroker(t)
	ctx := context.Background()
	_, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: a.SessionID, To: c.SessionID, Type: "request", Body: "offline recipient"})
	if err != nil {
		t.Fatal(err)
	}
	if err = b.UpsertSessions(ctx, []Session{{ID: c.SessionID, Provider: Claude, NativeID: "b", Active: false, Status: Stale, Capabilities: Capabilities{CanSend: true, Transport: Managed}}}); err != nil {
		t.Fatal(err)
	}
	ready, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: c.SessionID, To: a.SessionID, Type: "request", Body: "ready recipient"})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := b.PendingDeliveries(ctx, 1)
	if err != nil || len(pending) != 1 || pending[0].ID != ready.ID {
		t.Fatalf("offline queue starved ready recipient: %+v %v", pending, err)
	}
}

func TestInboxBoundsEscapedPayloadAndPrioritizesUnread(t *testing.T) {
	b, g, a, c := testBroker(t)
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		if _, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: a.SessionID, To: c.SessionID, Type: "request", Body: strings.Repeat("\"", maxBody)}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := b.Inbox(ctx, g.ID, c.SessionID, true)
	if err != nil || len(first) == 0 || len(first) >= 6 {
		t.Fatalf("unbounded first inbox page: %d %v", len(first), err)
	}
	second, err := b.Inbox(ctx, g.ID, c.SessionID, true)
	if err != nil || len(second) == 0 || first[0].ID == second[0].ID {
		t.Fatalf("read history hid unread messages: %+v %v", second, err)
	}
}

func TestExpiredRequestCannotBeAnsweredOrRead(t *testing.T) {
	b, g, a, c := testBroker(t)
	ctx := context.Background()
	request, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: a.SessionID, To: c.SessionID, Type: "request", Body: "expiring"})
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Sweep(ctx, request.ExpiresAt.Add(time.Second), time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Send(ctx, SendRequest{GroupID: g.ID, From: c.SessionID, Type: "reply", RequestID: request.ID, Body: "late answer"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("answered expired request: %v", err)
	}
	if messages, err := b.Inbox(ctx, g.ID, c.SessionID, true); err != nil || len(messages) != 0 {
		t.Fatalf("expired request still readable: %+v %v", messages, err)
	}
}

func TestMessageMustFitInboxAndEventsArePagedByBytes(t *testing.T) {
	b, g, a, c := testBroker(t)
	ctx := context.Background()
	sources := make([]string, 32)
	for i := range sources {
		sources[i] = strings.Repeat("\x00", 4096)
	}
	_, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: a.SessionID, To: c.SessionID, Type: "request", Body: strings.Repeat("\x00", maxBody), Sources: sources})
	if err == nil {
		t.Fatal("accepted a message whose escaped JSON can never fit the inbox page")
	}
	for i := 0; i < 10; i++ {
		if _, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: a.SessionID, To: c.SessionID, Type: "request", Body: strings.Repeat("x", maxBody)}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := b.Events(ctx, g.ID, 0, 1000)
	if err != nil || len(first) == 0 {
		t.Fatalf("first event page: %d %v", len(first), err)
	}
	last := first[len(first)-1].ID
	second, err := b.Events(ctx, g.ID, last, 1000)
	if err != nil || len(second) == 0 || second[0].ID <= last {
		t.Fatalf("byte paging lost events: %+v %v", second, err)
	}
}

func TestInboxCapacityDoesNotCountReadHistory(t *testing.T) {
	b, g, a, c := testBroker(t)
	ctx := context.Background()
	// Seed enough retained read messages to exercise the paging boundary without
	// generating thousands of provider notifications or expensive event writes.
	for i := 0; i < maxInbox+1; i++ {
		m, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: a.SessionID, To: c.SessionID, Type: "request", Body: "history"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = b.db.ExecContext(ctx, `UPDATE messages SET read_ns=?,state='read',delivery='inbox' WHERE id=?`, time.Now().UnixNano(), m.ID); err != nil {
			t.Fatal(err)
		}
	}
	fresh, err := b.Send(ctx, SendRequest{GroupID: g.ID, From: a.SessionID, To: c.SessionID, Type: "request", Body: "new information needed"})
	if err != nil {
		t.Fatalf("read history consumed inbox quota: %v", err)
	}
	inbox, err := b.Inbox(ctx, g.ID, c.SessionID, true)
	if err != nil || len(inbox) == 0 || inbox[0].ID != fresh.ID || inbox[0].ReadAt == nil {
		t.Fatalf("retained history starved unread request: %+v %v", inbox, err)
	}
}
