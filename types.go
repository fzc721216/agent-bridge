// Package bridge connects selected local coding-agent sessions through a persistent broker.
// Its agent-bridge/1 protocol is a local protocol, not an A2A implementation.
package bridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

const Protocol = "agent-bridge/1"

var (
	ErrNotFound     = errors.New("not found")
	ErrUnauthorized = errors.New("unauthorized")
	ErrConflict     = errors.New("conflict")
	ErrUnsupported  = errors.New("unsupported capability")
)

type Provider string

const (
	Codex  Provider = "codex"
	Claude Provider = "claude"
)

type Transport string

const (
	ObserveOnly Transport = "observe-only"
	CodexQueue  Transport = "codex-queue"
	Tmux        Transport = "tmux"
	Managed     Transport = "managed"
)

type Status string

const (
	Unknown Status = "unknown"
	Idle    Status = "idle"
	Working Status = "working"
	Blocked Status = "blocked"
	Done    Status = "done"
	Stale   Status = "stale"
)

func (s Status) Valid() bool {
	switch s {
	case Unknown, Idle, Working, Blocked, Done, Stale:
		return true
	}
	return false
}

type Capabilities struct {
	CanSend          bool      `json:"can_send"`
	CanReceiveEvents bool      `json:"can_receive_events"`
	CanCaptureOutput bool      `json:"can_capture_output"`
	Transport        Transport `json:"transport"`
}

type Session struct {
	ID           string       `json:"id"`
	Provider     Provider     `json:"provider"`
	NativeID     string       `json:"native_id"`
	Name         string       `json:"name,omitempty"`
	CWD          string       `json:"cwd,omitempty"`
	PID          int          `json:"pid,omitempty"`
	ProcessStart string       `json:"process_start,omitempty"`
	Active       bool         `json:"active"`
	Status       Status       `json:"status"`
	Capabilities Capabilities `json:"capabilities"`
	LastSeen     time.Time    `json:"last_seen"`
	TmuxPane     string       `json:"tmux_pane,omitempty"`
	Note         string       `json:"note,omitempty"`
}

type TaskState struct {
	Status    Status    `json:"status"`
	Task      string    `json:"task,omitempty"`
	Summary   string    `json:"summary,omitempty"`
	Topics    []string  `json:"topics,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Group struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type Member struct {
	GroupID       string    `json:"group_id"`
	Session       Session   `json:"session"`
	Joined        bool      `json:"joined"`
	Generation    string    `json:"generation"`
	AllowTmux     bool      `json:"allow_tmux"`
	LastHeartbeat time.Time `json:"last_heartbeat,omitempty"`
	State         TaskState `json:"state"`
}

// Credentials are issued by the local owner. Tokens are group/session/generation scoped.
type Credentials struct {
	GroupID   string `json:"group_id"`
	SessionID string `json:"session_id"`
	Token     string `json:"token"`
}

type Envelope struct {
	Protocol  string    `json:"protocol"`
	Type      string    `json:"type"`
	ID        string    `json:"message_id"`
	RequestID string    `json:"request_id,omitempty"`
	GroupID   string    `json:"group_id"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Body      string    `json:"body"`
	Sources   []string  `json:"sources,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type Message struct {
	Envelope
	State         string     `json:"state"`
	Delivery      string     `json:"delivery"`
	DeliveryError string     `json:"delivery_error,omitempty"`
	ReadAt        *time.Time `json:"read_at,omitempty"`
}

type Event struct {
	ID        int64     `json:"id"`
	GroupID   string    `json:"group_id"`
	SessionID string    `json:"session_id,omitempty"`
	Type      string    `json:"type"`
	Data      any       `json:"data,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type SendRequest struct {
	GroupID        string   `json:"group_id"`
	From           string   `json:"from"`
	To             string   `json:"to"`
	Body           string   `json:"body"`
	Type           string   `json:"type,omitempty"`
	RequestID      string   `json:"request_id,omitempty"`
	Sources        []string `json:"sources,omitempty"`
	TTLSeconds     int      `json:"ttl_seconds,omitempty"`
	IdempotencyKey string   `json:"idempotency_key,omitempty"`
}

// Adapter implementations never read another agent's hidden reasoning or bypass its permissions.
type Adapter interface {
	Discover(context.Context) ([]Session, error)
	Send(context.Context, Session, string) error
}

func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
