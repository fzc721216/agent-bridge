package tui

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	bridge "github.com/fuzicheng/agent-bridge"
	"github.com/fuzicheng/agent-bridge/local"
)

type fakeCaller struct {
	methods []string
	params  []any
}

func (f *fakeCaller) Call(_ context.Context, method string, params any, _ *bridge.Credentials, out any) error {
	f.methods = append(f.methods, method)
	f.params = append(f.params, params)
	if out != nil {
		raw := `{}`
		if method == "group.create" {
			raw = `{"id":"group-new","name":"team"}`
		}
		if method == "invite" {
			raw = `{"credentials_path":"/private/member.json","delivered":false}`
		}
		return json.Unmarshal([]byte(raw), out)
	}
	return nil
}

func key(s string) tea.KeyMsg {
	if s == "enter" {
		return tea.KeyMsg{Type: tea.KeyEnter}
	}
	if s == "esc" {
		return tea.KeyMsg{Type: tea.KeyEsc}
	}
	if s == " " {
		return tea.KeyMsg{Type: tea.KeySpace}
	}
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
}

func update(m model, msg tea.Msg) (model, tea.Cmd) {
	next, cmd := m.Update(msg)
	return next.(model), cmd
}

func TestSelectionAndCreateRequireSeparateConfirmation(t *testing.T) {
	client := &fakeCaller{}
	m := newModel(context.Background(), client)
	m.snapshot = local.Snapshot{Sessions: []bridge.Session{{ID: "codex:one", Provider: bridge.Codex}}}
	m, cmd := update(m, key(" "))
	if cmd != nil || !m.selected["codex:one"] || len(client.methods) != 0 {
		t.Fatal("selection mutated broker or failed")
	}
	m, cmd = update(m, key("g"))
	if cmd != nil || m.mode != nameInput {
		t.Fatal("g must open name form only")
	}
	m, _ = update(m, key("team"))
	m, cmd = update(m, key("enter"))
	if cmd != nil || m.mode != confirmCreate || len(client.methods) != 0 {
		t.Fatal("name entry must open review without creating")
	}
	m, cmd = update(m, key("n"))
	if cmd != nil || m.mode != browse || len(client.methods) != 0 {
		t.Fatal("cancel created group")
	}
	m.mode = confirmCreate
	m, cmd = update(m, key("y"))
	if cmd == nil || !m.busy {
		t.Fatal("explicit y should create")
	}
	msg := cmd()
	if len(client.methods) != 1 || client.methods[0] != "group.create" {
		t.Fatalf("calls: %v", client.methods)
	}
	params := client.params[0].(map[string]any)
	if params["allow_tmux"] != false {
		t.Fatal("tmux must default disabled")
	}
	m, _ = update(m, msg)
	if m.mode != browse || m.groupID() != "group-new" || len(m.selected) != 0 {
		t.Fatal("creation result not reflected")
	}
}

func TestInvitationDefaultIsManualAndNeverDisplaysToken(t *testing.T) {
	client := &fakeCaller{}
	m := newModel(context.Background(), client)
	m.snapshot = local.Snapshot{Groups: []bridge.Group{{ID: "g", Name: "team"}}, Members: []bridge.Member{{GroupID: "g", Session: bridge.Session{ID: "claude:one"}}}}
	m, cmd := update(m, key("i"))
	if cmd != nil || m.mode != confirmInvite || m.deliverInvites || len(client.methods) != 0 {
		t.Fatal("i must only open review")
	}
	m, cmd = update(m, key("y"))
	if cmd == nil {
		t.Fatal("confirmation did not produce invite")
	}
	m, _ = update(m, cmd())
	params := client.params[0].(map[string]any)
	if params["deliver"] != false {
		t.Fatal("manual invitation must not send")
	}
	view := m.View()
	if !strings.Contains(view, "/private/member.json") || strings.Contains(view, `"token"`) {
		t.Fatalf("unsafe or missing invitation view: %s", view)
	}
}

func TestInvitationDeliveryNeedsToggleAndConfirmation(t *testing.T) {
	client := &fakeCaller{}
	m := newModel(context.Background(), client)
	m.snapshot = local.Snapshot{Groups: []bridge.Group{{ID: "g", Name: "team"}}, Members: []bridge.Member{{GroupID: "g", Session: bridge.Session{ID: "codex:one"}}}}
	m, _ = update(m, key("i"))
	m, cmd := update(m, key("d"))
	if !m.deliverInvites || cmd != nil || len(client.methods) != 0 {
		t.Fatal("toggling delivery must not deliver")
	}
	m, cmd = update(m, key("esc"))
	if cmd != nil || len(client.methods) != 0 {
		t.Fatal("canceling invitation delivered it")
	}
	m, _ = update(m, key("i"))
	if m.deliverInvites {
		t.Fatal("a new review must reset delivery consent")
	}
	m, _ = update(m, key("d"))
	m, cmd = update(m, key("y"))
	if cmd == nil {
		t.Fatal("confirmed invitation did not run")
	}
	cmd()
	if client.params[0].(map[string]any)["deliver"] != true {
		t.Fatal("confirmed delivery not passed to daemon")
	}
}

func TestFilterPreservesHiddenSelectionAndClampsCursor(t *testing.T) {
	m := newModel(context.Background(), &fakeCaller{})
	m.snapshot.Sessions = []bridge.Session{{ID: "codex:a", Provider: bridge.Codex, Name: "api"}, {ID: "claude:b", Provider: bridge.Claude, Name: "docs"}}
	m.selected["codex:a"] = true
	m.filter = "claude"
	m.sessionCursor = 99
	m.clamp()
	if m.sessionCursor != 0 || len(m.visibleSessions()) != 1 || len(m.selectedIDs()) != 1 {
		t.Fatal("filter lost selection or invalid cursor")
	}
}

func TestViewStripsTerminalControls(t *testing.T) {
	m := newModel(context.Background(), &fakeCaller{})
	m.snapshot.Sessions = []bridge.Session{{ID: "a", Name: "\x1b[2Jhello\x00\rworld"}}
	if strings.ContainsAny(m.View(), "\x1b\x00\r") {
		t.Fatal("provider text controls reached terminal")
	}
}

func TestNameInputAcceptsSpaces(t *testing.T) {
	m := newModel(context.Background(), &fakeCaller{})
	m.mode = nameInput
	m, _ = update(m, key("team"))
	m, _ = update(m, key(" "))
	m, _ = update(m, key("one"))
	if m.name != "team one" {
		t.Fatalf("got name %q", m.name)
	}
}
