package bridge

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	maxBody  = 64 << 10
	maxInbox = 1000
)

func (b *Broker) UpsertSessions(ctx context.Context, sessions []Session) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, s := range sessions {
		if s.ID == "" || s.NativeID == "" || (s.Provider != Codex && s.Provider != Claude) {
			return fmt.Errorf("invalid session identity")
		}
		if s.Status == "" {
			s.Status = Unknown
		}
		if !s.Status.Valid() {
			return fmt.Errorf("invalid session status %q", s.Status)
		}
		data, err := json.Marshal(s)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO sessions(id,data) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data`, s.ID, string(data)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (b *Broker) Sessions(ctx context.Context) ([]Session, error) {
	rows, err := b.db.QueryContext(ctx, `SELECT data FROM sessions ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Session{}
	for rows.Next() {
		var s Session
		var data string
		if err = rows.Scan(&data); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(data), &s); err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

func (b *Broker) CreateGroup(ctx context.Context, name string, ids []string, allowTmux bool) (Group, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 256 || len(ids) == 0 || len(ids) > 100 {
		return Group{}, fmt.Errorf("group requires a name (1..256 bytes) and 1..100 members")
	}
	g := Group{ID: NewID(), Name: name, CreatedAt: time.Now().UTC()}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return Group{}, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO groups(id,name,created_ns) VALUES(?,?,?)`, g.ID, g.Name, g.CreatedAt.UnixNano()); err != nil {
		return Group{}, err
	}
	seen := map[string]bool{}
	state, _ := json.Marshal(TaskState{Status: Unknown})
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		if _, err = getSession(ctx, tx, id); err != nil {
			return Group{}, fmt.Errorf("member %s: %w", id, err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO members(group_id,session_id,generation,allow_tmux,state) VALUES(?,?,?,?,?)`, g.ID, id, NewID(), allowTmux, string(state)); err != nil {
			return Group{}, err
		}
	}
	if err = addEvent(ctx, tx, g.ID, "", "group.created", g, g.CreatedAt); err != nil {
		return Group{}, err
	}
	if err = tx.Commit(); err != nil {
		return Group{}, err
	}
	return g, nil
}

func (b *Broker) Groups(ctx context.Context) ([]Group, error) {
	rows, err := b.db.QueryContext(ctx, `SELECT id,name,created_ns FROM groups ORDER BY created_ns,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	groups := []Group{}
	for rows.Next() {
		var g Group
		var ns int64
		if err = rows.Scan(&g.ID, &g.Name, &ns); err != nil {
			return nil, err
		}
		g.CreatedAt = time.Unix(0, ns).UTC()
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

func groupExists(ctx context.Context, q sqlReader, id string) error {
	var n int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM groups WHERE id=?`, id).Scan(&n)
	if err == sql.ErrNoRows {
		return ErrNotFound
	}
	return err
}

func (b *Broker) Members(ctx context.Context, group string) ([]Member, error) {
	if err := groupExists(ctx, b.db, group); err != nil {
		return nil, err
	}
	rows, err := b.db.QueryContext(ctx, memberSelect+` WHERE m.group_id=? ORDER BY m.session_id`, group)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Member{}
	for rows.Next() {
		m, err := memberFromRow(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

func (b *Broker) RemoveMember(ctx context.Context, group, session string) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	r, err := tx.ExecContext(ctx, `DELETE FROM members WHERE group_id=? AND session_id=?`, group, session)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	// Removing a member cancels every outstanding delivery involving that member.
	if _, err = tx.ExecContext(ctx, `UPDATE messages SET state='failed',delivery=CASE WHEN delivery='pending' THEN 'failed' ELSE delivery END,delivery_error='member removed' WHERE group_id=? AND (sender=? OR recipient=?) AND state IN ('queued','read')`, group, session, session); err != nil {
		return err
	}
	if err = addEvent(ctx, tx, group, session, "member.removed", nil, time.Now().UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

func tokenHash(token string) string {
	v := sha256.Sum256([]byte(token))
	return hex.EncodeToString(v[:])
}

func (b *Broker) Invite(ctx context.Context, group, session string) (Credentials, error) {
	c := Credentials{GroupID: group, SessionID: session, Token: NewID() + NewID()}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return Credentials{}, err
	}
	defer tx.Rollback()
	if _, err = getMember(ctx, tx, group, session); err != nil {
		return Credentials{}, err
	}
	state, _ := json.Marshal(TaskState{Status: Unknown})
	if _, err = tx.ExecContext(ctx, `UPDATE members SET token_hash=?,generation=?,joined=0,heartbeat_ns=0,state=? WHERE group_id=? AND session_id=?`, tokenHash(c.Token), NewID(), string(state), group, session); err != nil {
		return Credentials{}, err
	}
	if err = addEvent(ctx, tx, group, session, "member.invited", nil, time.Now().UTC()); err != nil {
		return Credentials{}, err
	}
	if err = tx.Commit(); err != nil {
		return Credentials{}, err
	}
	return c, nil
}

func authenticate(ctx context.Context, q sqlReader, c Credentials) (Member, error) {
	if c.Token == "" || c.GroupID == "" || c.SessionID == "" {
		return Member{}, ErrUnauthorized
	}
	var hash string
	err := q.QueryRowContext(ctx, `SELECT token_hash FROM members WHERE group_id=? AND session_id=?`, c.GroupID, c.SessionID).Scan(&hash)
	if err == sql.ErrNoRows {
		return Member{}, ErrUnauthorized
	}
	if err != nil {
		return Member{}, err
	}
	if hash == "" || subtle.ConstantTimeCompare([]byte(hash), []byte(tokenHash(c.Token))) != 1 {
		return Member{}, ErrUnauthorized
	}
	return getMember(ctx, q, c.GroupID, c.SessionID)
}

func (b *Broker) Authenticate(ctx context.Context, c Credentials) (Member, error) {
	return authenticate(ctx, b.db, c)
}

func validTopics(topics []string) error {
	if len(topics) > 32 {
		return fmt.Errorf("at most 32 topics are allowed")
	}
	for _, topic := range topics {
		if len(topic) > 256 {
			return fmt.Errorf("topic exceeds 256 bytes")
		}
	}
	return nil
}

func (b *Broker) Join(ctx context.Context, c Credentials, topics []string) (Member, error) {
	if err := validTopics(topics); err != nil {
		return Member{}, err
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return Member{}, err
	}
	defer tx.Rollback()
	m, err := authenticate(ctx, tx, c)
	if err != nil {
		return Member{}, err
	}
	now := time.Now().UTC()
	m.Joined = true
	m.LastHeartbeat = now
	m.State.Topics = topics
	m.State.UpdatedAt = now
	state, _ := json.Marshal(m.State)
	if _, err = tx.ExecContext(ctx, `UPDATE members SET joined=1,heartbeat_ns=?,state=? WHERE group_id=? AND session_id=?`, now.UnixNano(), string(state), c.GroupID, c.SessionID); err != nil {
		return Member{}, err
	}
	if err = addEvent(ctx, tx, c.GroupID, c.SessionID, "member.joined", m, now); err != nil {
		return Member{}, err
	}
	if err = tx.Commit(); err != nil {
		return Member{}, err
	}
	return m, nil
}

func (b *Broker) Heartbeat(ctx context.Context, c Credentials) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	m, err := authenticate(ctx, tx, c)
	if err != nil {
		return err
	}
	if !m.Joined {
		return fmt.Errorf("join first: %w", ErrConflict)
	}
	// A heartbeat is connection liveness, not a semantic task-state report.
	if _, err = tx.ExecContext(ctx, `UPDATE members SET heartbeat_ns=? WHERE group_id=? AND session_id=?`, time.Now().UTC().UnixNano(), c.GroupID, c.SessionID); err != nil {
		return err
	}
	return tx.Commit()
}

func (b *Broker) PublishState(ctx context.Context, c Credentials, state TaskState) error {
	if !state.Status.Valid() {
		return fmt.Errorf("invalid task status %q", state.Status)
	}
	if len(state.Task) > maxBody || len(state.Summary) > maxBody {
		return fmt.Errorf("task state exceeds 64 KiB")
	}
	if err := validTopics(state.Topics); err != nil {
		return err
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	m, err := authenticate(ctx, tx, c)
	if err != nil {
		return err
	}
	if !m.Joined {
		return fmt.Errorf("join first: %w", ErrConflict)
	}
	now := time.Now().UTC()
	state.UpdatedAt = now
	if state.Topics == nil {
		state.Topics = m.State.Topics
	}
	encoded, _ := json.Marshal(state)
	if _, err = tx.ExecContext(ctx, `UPDATE members SET state=?,heartbeat_ns=? WHERE group_id=? AND session_id=?`, string(encoded), now.UnixNano(), c.GroupID, c.SessionID); err != nil {
		return err
	}
	if err = addEvent(ctx, tx, c.GroupID, c.SessionID, "state.updated", state, now); err != nil {
		return err
	}
	return tx.Commit()
}

// Send records an asynchronous message. A pending delivery is a request to wake
// an adapter, not proof of receipt. Replies are stored without waking the asker.
func (b *Broker) Send(ctx context.Context, r SendRequest) (Message, error) {
	if r.Type == "" {
		r.Type = "message"
	}
	if r.Type != "message" && r.Type != "request" && r.Type != "reply" {
		return Message{}, fmt.Errorf("invalid message type %q", r.Type)
	}
	if strings.TrimSpace(r.Body) == "" || len(r.Body) > maxBody {
		return Message{}, fmt.Errorf("body must contain 1..65536 bytes")
	}
	if len(r.Sources) > 32 {
		return Message{}, fmt.Errorf("at most 32 sources")
	}
	for _, s := range r.Sources {
		if len(s) > 4096 {
			return Message{}, fmt.Errorf("source exceeds 4096 bytes")
		}
	}
	if len(r.IdempotencyKey) > 128 {
		return Message{}, fmt.Errorf("idempotency key exceeds 128 bytes")
	}
	if r.TTLSeconds == 0 {
		r.TTLSeconds = 15 * 60
	}
	if r.TTLSeconds < 1 || r.TTLSeconds > 24*60*60 {
		return Message{}, fmt.Errorf("ttl_seconds must be between 1 and 86400")
	}
	if r.GroupID == "" || r.From == "" {
		return Message{}, fmt.Errorf("group and sender required")
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return Message{}, err
	}
	defer tx.Rollback()
	if r.From != "user" {
		member, err := getMember(ctx, tx, r.GroupID, r.From)
		if err != nil {
			return Message{}, err
		}
		if !member.Joined {
			return Message{}, fmt.Errorf("sender must join: %w", ErrConflict)
		}
	} else if r.Type != "message" {
		return Message{}, fmt.Errorf("owner can send only messages: %w", ErrUnauthorized)
	}
	// Resolve the reply route before hashing so implicit/explicit routes agree.
	var original Message
	if r.Type == "reply" {
		if r.RequestID == "" {
			return Message{}, fmt.Errorf("request_id required for reply")
		}
		original, err = getMessage(ctx, tx, r.RequestID)
		if err != nil {
			return Message{}, err
		}
		if original.GroupID != r.GroupID || original.Type != "request" || original.To != r.From {
			return Message{}, ErrUnauthorized
		}
		if r.To == "" {
			r.To = original.From
		}
		if r.To != original.From {
			return Message{}, ErrUnauthorized
		}
	} else if r.RequestID != "" {
		return Message{}, fmt.Errorf("request_id is valid only on replies")
	}
	if r.To == "" || r.To == r.From {
		return Message{}, fmt.Errorf("a different recipient is required")
	}
	target, err := getMember(ctx, tx, r.GroupID, r.To)
	if err != nil {
		return Message{}, err
	}
	if r.Type == "request" && !target.Joined {
		return Message{}, fmt.Errorf("recipient must join before receiving requests: %w", ErrUnsupported)
	}
	fingerprintData, _ := json.Marshal(r)
	sum := sha256.Sum256(fingerprintData)
	fingerprint := hex.EncodeToString(sum[:])
	if r.IdempotencyKey != "" {
		var id, previous string
		err = tx.QueryRowContext(ctx, `SELECT id,fingerprint FROM messages WHERE group_id=? AND sender=? AND idem_key=?`, r.GroupID, r.From, r.IdempotencyKey).Scan(&id, &previous)
		if err == nil {
			if previous != fingerprint {
				return Message{}, fmt.Errorf("idempotency key reused with different content: %w", ErrConflict)
			}
			return getMessage(ctx, tx, id)
		}
		if err != sql.ErrNoRows {
			return Message{}, err
		}
	}
	now := time.Now().UTC()
	if r.Type == "reply" {
		if !original.ExpiresAt.After(now) || original.State == "expired" {
			return Message{}, fmt.Errorf("request expired: %w", ErrConflict)
		}
		if original.State != "queued" && original.State != "read" {
			return Message{}, fmt.Errorf("request already terminal: %w", ErrConflict)
		}
	}
	var count int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE group_id=? AND recipient=? AND expires_ns>? AND read_ns=0 AND state IN ('queued','read')`, r.GroupID, r.To, now.UnixNano()).Scan(&count); err != nil {
		return Message{}, err
	}
	if count >= maxInbox {
		return Message{}, fmt.Errorf("recipient inbox full: %w", ErrConflict)
	}
	m := Message{Envelope: Envelope{Protocol: Protocol, Type: r.Type, ID: NewID(), RequestID: r.RequestID, GroupID: r.GroupID, From: r.From, To: r.To, Body: r.Body, Sources: r.Sources, CreatedAt: now, ExpiresAt: now.Add(time.Duration(r.TTLSeconds) * time.Second)}, State: "queued", Delivery: "pending"}
	if r.Type == "reply" {
		m.Delivery = "inbox"
	}
	encoded, _ := json.Marshal(m.Envelope)
	if len(encoded) > 480<<10 {
		return Message{}, fmt.Errorf("encoded message exceeds 480 KiB; reduce body or source references")
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO messages(id,group_id,sender,recipient,kind,request_id,envelope,state,delivery,created_ns,expires_ns,idem_key,fingerprint) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, m.ID, m.GroupID, m.From, m.To, m.Type, m.RequestID, string(encoded), m.State, m.Delivery, now.UnixNano(), m.ExpiresAt.UnixNano(), r.IdempotencyKey, fingerprint); err != nil {
		return Message{}, err
	}
	if r.Type == "reply" {
		if _, err = tx.ExecContext(ctx, `UPDATE messages SET state='answered' WHERE id=?`, r.RequestID); err != nil {
			return Message{}, err
		}
	}
	if err = addEvent(ctx, tx, m.GroupID, m.From, "message."+m.Type, m, now); err != nil {
		return Message{}, err
	}
	if err = tx.Commit(); err != nil {
		return Message{}, err
	}
	return m, nil
}

func (b *Broker) Inbox(ctx context.Context, group, session string, markRead bool) ([]Message, error) {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = getMember(ctx, tx, group, session); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	// Prefer unread messages so previously read history cannot hide a new request.
	// Limit the encoded payload as well as row count to fit the local RPC frame.
	rows, err := tx.QueryContext(ctx, messageSelect+` WHERE group_id=? AND recipient=? AND expires_ns>? AND state NOT IN ('failed','expired') ORDER BY (read_ns<>0),created_ns,id LIMIT 1000`, group, session, now.UnixNano())
	if err != nil {
		return nil, err
	}
	result := []Message{}
	encodedBytes := 2
	for rows.Next() {
		m, err := messageFromRow(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		encoded, err := json.Marshal(m)
		if err != nil {
			rows.Close()
			return nil, err
		}
		if encodedBytes+len(encoded)+1 > 512<<10 {
			break
		}
		encodedBytes += len(encoded) + 1
		result = append(result, m)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if markRead {
		for i := range result {
			m := &result[i]
			if m.ReadAt != nil {
				continue
			}
			m.ReadAt = &now
			if m.State == "queued" {
				m.State = "read"
			}
			if m.Delivery == "pending" {
				m.Delivery = "inbox"
			}
			if _, err = tx.ExecContext(ctx, `UPDATE messages SET read_ns=?,state=?,delivery=? WHERE id=?`, now.UnixNano(), m.State, m.Delivery, m.ID); err != nil {
				return nil, err
			}
			if err = addEvent(ctx, tx, group, session, "message.read", map[string]string{"message_id": m.ID}, now); err != nil {
				return nil, err
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (b *Broker) RequestStatus(ctx context.Context, group, session, requestID string) (Message, error) {
	if _, err := getMember(ctx, b.db, group, session); err != nil {
		return Message{}, err
	}
	m, err := getMessage(ctx, b.db, requestID)
	if err != nil {
		return m, err
	}
	if m.GroupID != group || (m.From != session && m.To != session) {
		return Message{}, ErrUnauthorized
	}
	if m.Type != "request" {
		return Message{}, fmt.Errorf("not a request: %w", ErrConflict)
	}
	if !m.ExpiresAt.After(time.Now()) && (m.State == "queued" || m.State == "read") {
		m.State = "expired"
	}
	return m, nil
}

func (b *Broker) Events(ctx context.Context, group string, after int64, limit int) ([]Event, error) {
	if group != "" {
		if err := groupExists(ctx, b.db, group); err != nil {
			return nil, err
		}
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	query := `SELECT id,group_id,session_id,kind,data,created_ns FROM events WHERE id>?`
	args := []any{after}
	if group != "" {
		query += ` AND group_id=?`
		args = append(args, group)
	}
	query += ` ORDER BY id LIMIT ?`
	args = append(args, limit)
	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Event{}
	encodedBytes := 2
	for rows.Next() {
		var event Event
		var data string
		var ns int64
		if err = rows.Scan(&event.ID, &event.GroupID, &event.SessionID, &event.Type, &data, &ns); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(data), &event.Data); err != nil {
			return nil, err
		}
		event.CreatedAt = time.Unix(0, ns).UTC()
		encoded, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		if encodedBytes+len(encoded)+1 > 512<<10 {
			break
		}
		encodedBytes += len(encoded) + 1
		result = append(result, event)
	}
	return result, rows.Err()
}

func (b *Broker) PendingDeliveries(ctx context.Context, limit int) ([]Message, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	// Skip targets that cannot currently be notified before applying LIMIT, so an
	// offline member's queue cannot starve unrelated active recipients.
	rows, err := b.db.QueryContext(ctx, messageSelect+` WHERE delivery='pending' AND state='queued' AND read_ns=0 AND expires_ns>?
		AND EXISTS (SELECT 1 FROM members m JOIN sessions s ON s.id=m.session_id
			WHERE m.group_id=messages.group_id AND m.session_id=messages.recipient AND m.joined=1
			AND json_extract(s.data,'$.active')=1 AND json_extract(s.data,'$.capabilities.can_send')=1
			AND (json_extract(s.data,'$.capabilities.transport')<>'tmux'
				OR (m.allow_tmux=1 AND json_extract(s.data,'$.status')='idle')))
		ORDER BY created_ns,id LIMIT ?`, time.Now().UnixNano(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Message{}
	for rows.Next() {
		m, err := messageFromRow(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

func (b *Broker) RecordDelivery(ctx context.Context, id, delivery, detail string) error {
	switch delivery {
	case "accepted", "unsupported", "failed", "unknown", "sending":
	default:
		return fmt.Errorf("invalid delivery state %q", delivery)
	}
	if len(detail) > 4096 {
		detail = detail[:4096]
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	m, err := getMessage(ctx, tx, id)
	if err != nil {
		return err
	}
	if m.Type == "reply" {
		return fmt.Errorf("replies are inbox-only: %w", ErrConflict)
	}
	if delivery == "sending" {
		if m.Delivery != "pending" || m.State != "queued" || m.ReadAt != nil || !m.ExpiresAt.After(time.Now()) {
			return fmt.Errorf("message is no longer eligible for notification: %w", ErrConflict)
		}
	}
	// Persist 'sending' before transport I/O. Crash recovery must not resend
	// an ambiguous write, as provider queues generally lack idempotency keys.
	if m.Delivery != "pending" && m.Delivery != "sending" {
		return fmt.Errorf("delivery already recorded: %w", ErrConflict)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE messages SET delivery=?,delivery_error=? WHERE id=?`, delivery, detail, id); err != nil {
		return err
	}
	if err = addEvent(ctx, tx, m.GroupID, m.To, "delivery."+delivery, map[string]string{"message_id": id, "detail": detail}, time.Now().UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

// Sweep expires requests and marks semantic state stale. A later heartbeat alone
// deliberately cannot restore an old task summary to 'working'.
func (b *Broker) Sweep(ctx context.Context, now time.Time, staleAfter time.Duration) error {
	if staleAfter <= 0 {
		return fmt.Errorf("staleAfter must be positive")
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, messageSelect+` WHERE expires_ns<=? AND state IN ('queued','read')`, now.UnixNano())
	if err != nil {
		return err
	}
	expired := []Message{}
	for rows.Next() {
		m, e := messageFromRow(rows)
		if e != nil {
			rows.Close()
			return e
		}
		expired = append(expired, m)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, m := range expired {
		if _, err = tx.ExecContext(ctx, `UPDATE messages SET state='expired',delivery=CASE WHEN delivery='pending' THEN 'expired' ELSE delivery END WHERE id=?`, m.ID); err != nil {
			return err
		}
		if err = addEvent(ctx, tx, m.GroupID, m.To, "message.expired", map[string]string{"message_id": m.ID}, now); err != nil {
			return err
		}
	}
	rows, err = tx.QueryContext(ctx, memberSelect+` WHERE m.joined=1`)
	if err != nil {
		return err
	}
	staleMembers := []Member{}
	for rows.Next() {
		m, e := memberFromRow(rows)
		if e != nil {
			rows.Close()
			return e
		}
		if m.State.Status != Stale && m.State.Status != Done && now.Sub(m.State.UpdatedAt) > staleAfter {
			staleMembers = append(staleMembers, m)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, m := range staleMembers {
		m.State.Status = Stale
		encoded, _ := json.Marshal(m.State)
		if _, err = tx.ExecContext(ctx, `UPDATE members SET state=? WHERE group_id=? AND session_id=?`, string(encoded), m.GroupID, m.Session.ID); err != nil {
			return err
		}
		if err = addEvent(ctx, tx, m.GroupID, m.Session.ID, "state.stale", m.State, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// IsNotFound reports the broker's stable not-found category.
func IsNotFound(err error) bool { return errors.Is(err, ErrNotFound) }
