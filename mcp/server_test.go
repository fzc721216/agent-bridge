package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	bridge "github.com/fuzicheng/agent-bridge"
	"github.com/fuzicheng/agent-bridge/local"
)

var testCredentials = bridge.Credentials{GroupID: "group-a", SessionID: "claude:a", Token: "scoped-secret-never-print"}

func fixtureStream(lines ...string) string {
	return strings.Join(lines, "\n") + "\n"
}

func initialize(version string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":%q,"capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`, version)
}

const initialized = `{"jsonrpc":"2.0","method":"notifications/initialized"}`

func serveInput(t *testing.T, input string, client local.Client) ([]map[string]any, string) {
	t.Helper()
	var out bytes.Buffer
	if err := Serve(context.Background(), strings.NewReader(input), &out, client, testCredentials); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	responses := []map[string]any{}
	scanner := bufio.NewScanner(bytes.NewReader(out.Bytes()))
	for scanner.Scan() {
		var value map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			t.Fatalf("non-protocol stdout: %q: %v", scanner.Text(), err)
		}
		responses = append(responses, value)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return responses, out.String()
}

func TestInitializationAndToolSchemas(t *testing.T) {
	for _, version := range []string{"2024-11-05", "2025-03-26", "future-client-version"} {
		t.Run(version, func(t *testing.T) {
			responses, output := serveInput(t, fixtureStream(initialize(version), initialized,
				`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
				`{"jsonrpc":"2.0","id":3,"method":"ping"}`), local.Client{})
			if len(responses) != 3 {
				t.Fatalf("got %d responses: %s", len(responses), output)
			}
			negotiated := responses[0]["result"].(map[string]any)["protocolVersion"]
			want := version
			if version == "future-client-version" {
				want = latestProtocol
			}
			if negotiated != want {
				t.Errorf("protocol = %v, want %v", negotiated, want)
			}
			tools := responses[1]["result"].(map[string]any)["tools"].([]any)
			if len(tools) != 7 {
				t.Fatalf("got %d tools", len(tools))
			}
			for _, raw := range tools {
				tool := raw.(map[string]any)
				schema := tool["inputSchema"].(map[string]any)
				if _, ok := schema["required"].([]any); !ok {
					t.Errorf("%s has non-array required schema: %v", tool["name"], schema["required"])
				}
				if schema["additionalProperties"] != false {
					t.Errorf("%s permits unknown arguments", tool["name"])
				}
			}
			if strings.Contains(output, testCredentials.Token) {
				t.Fatal("credentials leaked to stdout")
			}
		})
	}
}

func TestInvalidRequestsAndInitializationOrder(t *testing.T) {
	responses, _ := serveInput(t, fixtureStream(
		`{`, `[]`, `null`,
		`{"jsonrpc":"2.0","id":[],"method":"ping"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		initialize(latestProtocol),
		`{"jsonrpc":"2.0","id":3,"method":"tools/list"}`,
		initialized,
		`{"jsonrpc":"2.0","id":4,"method":"unknown"}`,
		`{"jsonrpc":"2.0","method":"notifications/unknown"}`,
		initialize(latestProtocol)), local.Client{})
	wantCodes := []float64{-32700, -32600, -32600, -32600, -32002, 0, -32002, -32601, -32600}
	if len(responses) != len(wantCodes) {
		t.Fatalf("responses = %d, want %d: %#v", len(responses), len(wantCodes), responses)
	}
	for i, want := range wantCodes {
		if want == 0 {
			if responses[i]["error"] != nil {
				t.Errorf("response %d unexpectedly failed: %v", i, responses[i])
			}
			continue
		}
		if got := responses[i]["error"].(map[string]any)["code"]; got != want {
			t.Errorf("response %d code = %v, want %v", i, got, want)
		}
	}
}

// fakeBroker runs the actual local.Client Unix-socket transport, so these tests
// verify envelopes and credentials, not a second implementation of MCP routing.
func fakeBroker(t *testing.T, handler func(local.Request) local.Response) local.Client {
	t.Helper()
	dir, err := os.MkdirTemp("", "ab-mcp-")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "broker.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			var request local.Request
			if err := json.NewDecoder(conn).Decode(&request); err == nil {
				response := handler(request)
				response.ID = request.ID
				_ = json.NewEncoder(conn).Encode(response)
			}
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		wg.Wait()
		os.RemoveAll(dir)
	})
	return local.Client{Socket: path}
}

func TestToolsRouteWithScopedCredentials(t *testing.T) {
	methods := []string{"peers", "ask", "inbox", "reply", "request.status", "state", "heartbeat"}
	calls := make(chan local.Request, len(methods))
	client := fakeBroker(t, func(r local.Request) local.Response {
		calls <- r
		return local.Response{Result: json.RawMessage(`{"request_id":"request-123","state":"queued"}`)}
	})
	responses, output := serveInput(t, fixtureStream(initialize(latestProtocol), initialized,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"peers"}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"ask","arguments":{"to":"codex:peer","body":"Which schema?","ttl_seconds":60,"idempotency_key":"question-1"}}}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"inbox","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"reply","arguments":{"request_id":"request-123","body":"See schema.sql","sources":["/repo/schema.sql:5"]}}}`,
		`{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"request_status","arguments":{"request_id":"request-123"}}}`,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"publish_state","arguments":{"status":"working","task":"Check schemas","summary":"Reading docs","topics":["database"]}}}`,
		`{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"heartbeat"}}`), client)
	if len(responses) != 8 {
		t.Fatalf("responses = %d: %s", len(responses), output)
	}
	for i, method := range methods {
		request := <-calls
		if request.Method != method {
			t.Errorf("call %d method = %q, want %q", i, request.Method, method)
		}
		if request.Credentials == nil || *request.Credentials != testCredentials {
			t.Errorf("call %d missing scoped credentials", i)
		}
		result := responses[i+1]["result"].(map[string]any)
		if result["isError"] == true {
			t.Errorf("call %d failed: %#v", i, result)
		}
		text := result["content"].([]any)[0].(map[string]any)["text"].(string)
		var data map[string]any
		if err := json.Unmarshal([]byte(text), &data); err != nil || data["request_id"] != "request-123" {
			t.Errorf("call %d response content = %q", i, text)
		}
	}
	if strings.Contains(output, testCredentials.Token) {
		t.Fatal("credential token exposed")
	}
}

func TestToolArgumentsCannotOverrideGroupOrSender(t *testing.T) {
	invalid := []string{
		`{"name":"ask","arguments":{"to":"peer","body":"question","group_id":"foreign"}}`,
		`{"name":"ask","arguments":{"to":"peer","body":"question","from":"other"}}`,
		`{"name":"ask","arguments":{"to":"peer","body":"question","ttl_seconds":-1}}`,
		`{"name":"ask","arguments":{"to":"peer","body":"question","ttl_seconds":1.5}}`,
		`{"name":"ask","arguments":{"to":"peer","body":" "}}`,
		`{"name":"reply","arguments":{"request_id":"id","body":"answer","sources":[1]}}`,
		`{"name":"publish_state","arguments":{"status":"whatever"}}`,
		`{"name":"heartbeat","arguments":null}`,
		`{"name":"heartbeat","arguments":[]}`,
		`{"name":"unknown","arguments":{}}`,
	}
	for i, args := range invalid {
		input := fixtureStream(initialize(latestProtocol), initialized,
			fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":%s}`, args))
		responses, _ := serveInput(t, input, local.Client{Socket: "/must-not-connect"})
		if errorObject, ok := responses[1]["error"].(map[string]any); !ok || errorObject["code"] != float64(-32602) {
			t.Errorf("invalid arguments %d accepted: %v", i, responses[1])
		}
	}
}

func TestToolExecutionErrorIsContentAndRedactsCredentials(t *testing.T) {
	client := fakeBroker(t, func(r local.Request) local.Response {
		return local.Response{Error: "unauthorized " + testCredentials.Token}
	})
	responses, output := serveInput(t, fixtureStream(initialize(latestProtocol), initialized,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"inbox"}}`), client)
	if responses[1]["error"] != nil {
		t.Fatal("tool execution error incorrectly became a JSON-RPC error")
	}
	result := responses[1]["result"].(map[string]any)
	if result["isError"] != true {
		t.Fatalf("missing isError: %v", result)
	}
	if strings.Contains(output, testCredentials.Token) || !strings.Contains(output, "[redacted]") {
		t.Fatal("token was not redacted")
	}
}

func TestServeRejectsMissingCredentialsAndOversizedFrames(t *testing.T) {
	if err := Serve(context.Background(), strings.NewReader(""), io.Discard, local.Client{}, bridge.Credentials{}); err == nil {
		t.Fatal("missing credentials accepted")
	}
	if err := Serve(context.Background(), strings.NewReader(strings.Repeat("x", local.MaxFrame+1)), io.Discard, local.Client{}, testCredentials); err == nil {
		t.Fatal("oversized MCP frame accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Serve(ctx, strings.NewReader(""), io.Discard, local.Client{}, testCredentials); err != context.Canceled {
		t.Errorf("cancelled context = %v", err)
	}
}
