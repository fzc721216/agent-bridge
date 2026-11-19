// Package mcp exposes scoped agent-bridge operations as standard MCP stdio tools.
package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	bridge "github.com/fuzicheng/agent-bridge"
	"github.com/fuzicheng/agent-bridge/local"
)

const latestProtocol = "2025-03-26"

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

type textContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type toolResult struct {
	Content []textContent `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

// Serve implements MCP JSON-RPC over newline-delimited stdio. It writes only
// protocol messages to out. The caller owns the input stream and must close a
// blocking reader when canceling ctx. Credentials are never exposed as a tool.
func Serve(ctx context.Context, in io.Reader, out io.Writer, client local.Client, creds bridge.Credentials) error {
	if creds.GroupID == "" || creds.SessionID == "" || creds.Token == "" {
		return fmt.Errorf("MCP requires scoped group/session credentials")
	}
	s := server{client: client, creds: creds, encoder: json.NewEncoder(out)}
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 8192), local.MaxFrame)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if err := s.handle(ctx, line); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read MCP frame (maximum %d bytes): %w", local.MaxFrame, err)
	}
	return ctx.Err()
}

type server struct {
	client      local.Client
	creds       bridge.Credentials
	encoder     *json.Encoder
	initialized bool
	ready       bool
}

func (s *server) handle(ctx context.Context, line []byte) error {
	var r request
	if err := json.Unmarshal(line, &r); err != nil {
		code, message := -32700, "Parse error"
		if json.Valid(line) {
			code, message = -32600, "Invalid Request"
		}
		return s.fail(nil, code, message)
	}
	if r.JSONRPC != "2.0" || r.Method == "" || !validID(r.ID) {
		return s.fail(nil, -32600, "Invalid Request")
	}
	if len(r.ID) == 0 {
		if r.Method == "notifications/initialized" && s.initialized {
			s.ready = true
		}
		// Unknown notifications have no response under JSON-RPC.
		return nil
	}
	switch r.Method {
	case "initialize":
		if s.initialized {
			return s.fail(r.ID, -32600, "Already initialized")
		}
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		if err := json.Unmarshal(r.Params, &p); err != nil || p.ProtocolVersion == "" {
			return s.fail(r.ID, -32602, "initialize requires protocolVersion")
		}
		version := latestProtocol
		if p.ProtocolVersion == "2024-11-05" {
			version = p.ProtocolVersion
		}
		s.initialized = true
		return s.ok(r.ID, map[string]any{
			"protocolVersion": version,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]string{"name": "agent-bridge", "version": "0.1.0"},
			"instructions": "Local agent-bridge/1 collaboration tools, scoped to your selected group. " +
				"Join through the CLI before using tools. Publish your task and topics; use peers to find relevant knowledge. " +
				"ask queues a request and returns immediately. Read inbox to acknowledge requests and replies. " +
				"Answer requests with reply and cite sources. Never forward a request recursively or acknowledge replies with another reply. " +
				"A heartbeat reports liveness, not task progress; publish_state reports progress.",
		})
	case "ping":
		return s.ok(r.ID, map[string]any{})
	case "tools/list", "tools/call":
		if !s.ready {
			return s.fail(r.ID, -32002, "Not initialized; complete initialize and notifications/initialized")
		}
		if r.Method == "tools/list" {
			return s.ok(r.ID, map[string]any{"tools": tools()})
		}
		return s.call(ctx, r)
	default:
		return s.fail(r.ID, -32601, "Method not found")
	}
}

func validID(id json.RawMessage) bool {
	if len(id) == 0 {
		return true
	}
	var v any
	if err := json.Unmarshal(id, &v); err != nil {
		return false
	}
	switch v.(type) {
	case string, float64:
		return true
	default:
		return false
	}
}

func (s *server) call(ctx context.Context, r request) error {
	var p struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(r.Params, &p); err != nil || p.Name == "" {
		return s.fail(r.ID, -32602, "tools/call requires name")
	}
	method, known := map[string]string{
		"peers": "peers", "ask": "ask", "inbox": "inbox", "reply": "reply",
		"request_status": "request.status", "publish_state": "state", "heartbeat": "heartbeat",
	}[p.Name]
	if !known {
		return s.fail(r.ID, -32602, "Unknown tool")
	}
	args, err := validateArguments(p.Name, p.Arguments)
	if err != nil {
		return s.fail(r.ID, -32602, err.Error())
	}
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var result json.RawMessage
	if err := s.client.Call(callCtx, method, args, &s.creds, &result); err != nil {
		message := strings.ReplaceAll(err.Error(), s.creds.Token, "[redacted]")
		return s.ok(r.ID, toolResult{Content: []textContent{{Type: "text", Text: message}}, IsError: true})
	}
	if len(result) == 0 {
		result = json.RawMessage(`{}`)
	}
	return s.ok(r.ID, toolResult{Content: []textContent{{Type: "text", Text: string(result)}}})
}

func (s *server) ok(id json.RawMessage, result any) error {
	return s.encoder.Encode(response{JSONRPC: "2.0", ID: id, Result: result})
}

func (s *server) fail(id json.RawMessage, code int, message string) error {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	return s.encoder.Encode(response{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: message}})
}

func validateArguments(name string, raw json.RawMessage) (map[string]any, error) {
	args := map[string]any{}
	if len(raw) != 0 {
		if err := json.Unmarshal(raw, &args); err != nil || args == nil {
			return nil, errors.New("arguments must be an object")
		}
	}
	var spec tool
	for _, t := range tools() {
		if t.Name == name {
			spec = t
			break
		}
	}
	properties := spec.InputSchema["properties"].(map[string]any)
	for key, value := range args {
		definition, ok := properties[key]
		if !ok {
			return nil, fmt.Errorf("unknown argument %q", key)
		}
		property := definition.(map[string]any)
		switch property["type"] {
		case "string":
			str, ok := value.(string)
			if !ok || len(str) > 64<<10 {
				return nil, fmt.Errorf("%s must be a string of at most 65536 bytes", key)
			}
			if choices, ok := property["enum"].([]string); ok {
				match := false
				for _, choice := range choices {
					match = match || str == choice
				}
				if !match {
					return nil, fmt.Errorf("unsupported %s", key)
				}
			}
		case "integer":
			n, ok := value.(float64)
			if !ok || n < 1 || n > 86400 || n != float64(int(n)) {
				return nil, fmt.Errorf("%s must be an integer from 1 to 86400", key)
			}
		case "array":
			items, ok := value.([]any)
			if !ok || len(items) > 100 {
				return nil, fmt.Errorf("%s must contain at most 100 strings", key)
			}
			for _, item := range items {
				if str, ok := item.(string); !ok || len(str) > 4096 {
					return nil, fmt.Errorf("%s entries must be strings of at most 4096 bytes", key)
				}
			}
		}
	}
	for _, key := range spec.InputSchema["required"].([]string) {
		v, ok := args[key].(string)
		if !ok || strings.TrimSpace(v) == "" {
			return nil, fmt.Errorf("%s is required and must be nonempty", key)
		}
	}
	return args, nil
}

func schema(properties map[string]any, required ...string) map[string]any {
	if required == nil {
		required = []string{}
	}
	return map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false}
}

func str(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}

func stringsSchema(description string) map[string]any {
	return map[string]any{"type": "array", "description": description, "items": map[string]string{"type": "string"}, "maxItems": 100}
}

func tools() []tool {
	return []tool{
		{Name: "peers", Description: "List only members of your selected group, their published task/topics, status and capabilities. Use this to find an agent with relevant knowledge.", InputSchema: schema(map[string]any{})},
		{Name: "ask", Description: "Request information from a selected peer. Returns immediately with a request ID; queued does not mean read or answered. Check inbox/request_status later and continue independent work. Do not recursively forward peer requests.", InputSchema: schema(map[string]any{
			"to": str("Exact peer session ID from peers."), "body": str("Concrete question and the information needed."),
			"ttl_seconds": map[string]any{"type": "integer", "minimum": 1, "maximum": 86400, "description": "Optional expiry; default 900 seconds."},
			"idempotency_key": str("Optional key to avoid duplicate requests on retry."),
		}, "to", "body")},
		{Name: "inbox", Description: "Read your pending requests and replies; reading acknowledges receipt. Reply to a request with sources, but do not reply to replies or acknowledgements.", InputSchema: schema(map[string]any{})},
		{Name: "reply", Description: "Answer one request addressed to you, citing document URLs or file paths/lines in sources. Say when information is unavailable. Replies do not wake another agent automatically.", InputSchema: schema(map[string]any{
			"request_id": str("Request ID received in inbox."), "body": str("Answer or explanation that information is unavailable."), "sources": stringsSchema("Source references, such as /absolute/file:line or a URL. Contents are not automatically uploaded."),
		}, "request_id", "body")},
		{Name: "request_status", Description: "Check a request you sent or received without blocking. Accepted transport delivery does not establish that an agent read it; answered requests have an associated reply.", InputSchema: schema(map[string]any{"request_id": str("Request ID returned by ask or inbox.")}, "request_id")},
		{Name: "publish_state", Description: "Publish your current task, concise progress summary and knowledge topics to your selected peers. Publish on start/change/block/completion; heartbeats alone do not indicate progress.", InputSchema: schema(map[string]any{
			"status": map[string]any{"type": "string", "enum": []string{"unknown", "idle", "working", "blocked", "done", "stale"}},
			"task": str("Current task."), "summary": str("Concise progress or blocker."), "topics": stringsSchema("Documents, systems or subject areas you can answer questions about."),
		}, "status")},
		{Name: "heartbeat", Description: "Report that this member is reachable. This does not change your published task or prove work is progressing.", InputSchema: schema(map[string]any{})},
	}
}
