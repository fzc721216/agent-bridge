// Package local exposes the broker over an owner-only Unix socket.
package local

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	bridge "github.com/fuzicheng/agent-bridge"
)

const MaxFrame = 1 << 20

type Request struct {
	ID          string              `json:"id"`
	Method      string              `json:"method"`
	Params      json.RawMessage     `json:"params,omitempty"`
	Credentials *bridge.Credentials `json:"credentials,omitempty"`
}

type Response struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type Snapshot struct {
	Sessions []bridge.Session `json:"sessions"`
	Groups   []bridge.Group   `json:"groups"`
	Members  []bridge.Member  `json:"members"`
	Warnings []string         `json:"warnings,omitempty"`
}

func DefaultStateDir() string {
	if v := os.Getenv("AGENT_BRIDGE_STATE_DIR"); v != "" {
		return v
	}
	if v := os.Getenv("XDG_STATE_HOME"); v != "" {
		return filepath.Join(v, "agent-bridge")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "state", "agent-bridge")
}

type Client struct{ Socket string }

// Call opens one short-lived connection. Context cancellation closes the connection.
func (c Client) Call(ctx context.Context, method string, params any, credentials *bridge.Credentials, out any) error {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", c.Socket)
	if err != nil {
		return fmt.Errorf("connect to daemon (run agent-bridge daemon): %w", err)
	}
	defer conn.Close()
	if d, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(d)
	} else {
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}
	req := Request{ID: bridge.NewID(), Method: method, Params: raw, Credentials: credentials}
	if len(raw) > MaxFrame/2 {
		return fmt.Errorf("request too large")
	}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return err
	}
	var response Response
	if err := json.NewDecoder(&limitedReader{r: conn, n: MaxFrame}).Decode(&response); err != nil {
		return err
	}
	if response.ID != req.ID {
		return fmt.Errorf("response id mismatch")
	}
	if response.Error != "" {
		return fmt.Errorf("%s", response.Error)
	}
	if out != nil && len(response.Result) > 0 {
		return json.Unmarshal(response.Result, out)
	}
	return nil
}

type limitedReader struct {
	r net.Conn
	n int
}

func (r *limitedReader) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, fmt.Errorf("frame exceeds limit")
	}
	if len(p) > r.n {
		p = p[:r.n]
	}
	n, e := r.r.Read(p)
	r.n -= n
	return n, e
}
