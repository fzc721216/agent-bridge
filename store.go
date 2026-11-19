package bridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// Broker owns durable local coordination state. Public administrative methods are
// intended for the local owner; an IPC frontend must authenticate agent calls.
type Broker struct{ db *sql.DB }

// Open opens or creates a private coordination database. It never opens vendor
// databases for writing. All broker mutations use a single SQLite writer.
func Open(path string) (*Broker, error) {
	if path == "" {
		return nil, fmt.Errorf("database path is required")
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		if err := f.Chmod(0600); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var schemaVersion int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&schemaVersion); err != nil {
		db.Close()
		return nil, fmt.Errorf("read broker schema: %w", err)
	}
	if schemaVersion > 1 {
		db.Close()
		return nil, fmt.Errorf("broker schema version %d requires a newer agent-bridge", schemaVersion)
	}
	for _, statement := range []string{
		`PRAGMA busy_timeout=5000`, `PRAGMA foreign_keys=ON`, `PRAGMA journal_mode=WAL`,
		`CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, data TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS groups (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_ns INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS members (
			group_id TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
			session_id TEXT NOT NULL REFERENCES sessions(id), generation TEXT NOT NULL,
			token_hash TEXT NOT NULL DEFAULT '', joined INTEGER NOT NULL DEFAULT 0,
			allow_tmux INTEGER NOT NULL DEFAULT 0, heartbeat_ns INTEGER NOT NULL DEFAULT 0,
			state TEXT NOT NULL, PRIMARY KEY(group_id, session_id))`,
		`CREATE TABLE IF NOT EXISTS messages (
			id TEXT PRIMARY KEY, group_id TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
			sender TEXT NOT NULL, recipient TEXT NOT NULL, kind TEXT NOT NULL, request_id TEXT NOT NULL,
			envelope TEXT NOT NULL, state TEXT NOT NULL, delivery TEXT NOT NULL,
			delivery_error TEXT NOT NULL DEFAULT '', created_ns INTEGER NOT NULL, expires_ns INTEGER NOT NULL,
			read_ns INTEGER NOT NULL DEFAULT 0, idem_key TEXT NOT NULL DEFAULT '', fingerprint TEXT NOT NULL)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS messages_idem ON messages(group_id, sender, idem_key) WHERE idem_key <> ''`,
		`CREATE INDEX IF NOT EXISTS messages_inbox ON messages(group_id, recipient, created_ns)`,
		`CREATE INDEX IF NOT EXISTS messages_pending ON messages(delivery, created_ns)`,
		`CREATE INDEX IF NOT EXISTS messages_request ON messages(request_id)`,
		`CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT, group_id TEXT NOT NULL,
			session_id TEXT NOT NULL, kind TEXT NOT NULL, data TEXT NOT NULL, created_ns INTEGER NOT NULL)`,
		`PRAGMA user_version=1`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize broker database: %w", err)
		}
	}
	// Once transport I/O began, a daemon crash cannot establish whether the
	// provider accepted it. Never replay that write after opening the database.
	if _, err := db.ExecContext(ctx, `UPDATE messages SET delivery='unknown',delivery_error='daemon restarted during notification; not retried' WHERE delivery='sending'`); err != nil {
		db.Close()
		return nil, fmt.Errorf("recover broker deliveries: %w", err)
	}
	return &Broker{db: db}, nil
}

func (b *Broker) Close() error { return b.db.Close() }

type sqlReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func getSession(ctx context.Context, q sqlReader, id string) (Session, error) {
	var data string
	err := q.QueryRowContext(ctx, `SELECT data FROM sessions WHERE id=?`, id).Scan(&data)
	if err == sql.ErrNoRows {
		return Session{}, ErrNotFound
	}
	if err != nil {
		return Session{}, err
	}
	var s Session
	err = json.Unmarshal([]byte(data), &s)
	return s, err
}

func memberFromRow(row interface{ Scan(...any) error }) (Member, error) {
	var m Member
	var sessionData, stateData string
	var heartbeat int64
	err := row.Scan(&m.GroupID, &sessionData, &m.Joined, &m.Generation, &m.AllowTmux, &heartbeat, &stateData)
	if err == sql.ErrNoRows {
		return m, ErrNotFound
	}
	if err != nil {
		return m, err
	}
	if err = json.Unmarshal([]byte(sessionData), &m.Session); err != nil {
		return m, err
	}
	if err = json.Unmarshal([]byte(stateData), &m.State); err != nil {
		return m, err
	}
	if heartbeat > 0 {
		m.LastHeartbeat = time.Unix(0, heartbeat).UTC()
	}
	return m, nil
}

const memberSelect = `SELECT m.group_id,s.data,m.joined,m.generation,m.allow_tmux,m.heartbeat_ns,m.state FROM members m JOIN sessions s ON s.id=m.session_id`

func getMember(ctx context.Context, q sqlReader, group, session string) (Member, error) {
	return memberFromRow(q.QueryRowContext(ctx, memberSelect+` WHERE m.group_id=? AND m.session_id=?`, group, session))
}

const messageSelect = `SELECT envelope,state,delivery,delivery_error,read_ns FROM messages`

func messageFromRow(row interface{ Scan(...any) error }) (Message, error) {
	var m Message
	var data string
	var readNS int64
	err := row.Scan(&data, &m.State, &m.Delivery, &m.DeliveryError, &readNS)
	if err == sql.ErrNoRows {
		return m, ErrNotFound
	}
	if err != nil {
		return m, err
	}
	if err = json.Unmarshal([]byte(data), &m.Envelope); err != nil {
		return m, err
	}
	if readNS > 0 {
		t := time.Unix(0, readNS).UTC()
		m.ReadAt = &t
	}
	return m, nil
}

func getMessage(ctx context.Context, q sqlReader, id string) (Message, error) {
	return messageFromRow(q.QueryRowContext(ctx, messageSelect+` WHERE id=?`, id))
}

func addEvent(ctx context.Context, tx *sql.Tx, group, session, kind string, data any, now time.Time) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO events(group_id,session_id,kind,data,created_ns) VALUES(?,?,?,?,?)`, group, session, kind, string(encoded), now.UnixNano())
	return err
}
