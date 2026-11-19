// Package tui provides an owner-facing terminal session picker and status board.
package tui

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"

	tea "github.com/charmbracelet/bubbletea"
	bridge "github.com/fuzicheng/agent-bridge"
	"github.com/fuzicheng/agent-bridge/local"
)

type caller interface {
	Call(context.Context, string, any, *bridge.Credentials, any) error
}

type mode int

const (
	browse mode = iota
	filterInput
	nameInput
	confirmCreate
	confirmInvite
)

type model struct {
	ctx            context.Context
	client         caller
	snapshot       local.Snapshot
	events         []bridge.Event
	eventsGroup    string
	selected       map[string]bool
	mode           mode
	focus          int
	sessionCursor  int
	groupCursor    int
	filter         string
	name           string
	allowTmux      bool
	deliverInvites bool
	busy           bool
	width          int
	height         int
	notice         string
	err            error
}

type refreshed struct {
	snapshot local.Snapshot
	events   []bridge.Event
	groupID  string
	err      error
}

type created struct {
	group bridge.Group
	err   error
}

type invited struct {
	paths    []string
	delivery []string
	err      error
}

type tick time.Time

// Run connects to an already-running daemon. Administrative mutations occur only
// after a separate confirmation screen; selecting a session performs no writes.
func Run(ctx context.Context, client local.Client) error {
	m := newModel(ctx, client)
	_, err := tea.NewProgram(m, tea.WithContext(ctx), tea.WithAltScreen()).Run()
	return err
}

func newModel(ctx context.Context, client caller) model {
	return model{ctx: ctx, client: client, selected: make(map[string]bool), width: 100, height: 35}
}

func nextTick() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg { return tick(t) })
}

func (m model) Init() tea.Cmd {
	return tea.Batch(m.refresh(true), nextTick())
}

func (m model) refresh(discover bool) tea.Cmd {
	groupID := m.groupID()
	existing := append([]bridge.Event(nil), m.events...)
	if m.eventsGroup != groupID {
		existing = nil
	}
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(m.ctx, 20*time.Second)
		defer cancel()
		if discover {
			if err := m.client.Call(ctx, "discover", struct{}{}, nil, nil); err != nil {
				return refreshed{err: err}
			}
		}
		var snapshot local.Snapshot
		if err := m.client.Call(ctx, "status", struct{}{}, nil, &snapshot); err != nil {
			return refreshed{err: err}
		}
		events := existing
		if groupID != "" {
			var after int64
			if len(events) != 0 {
				after = events[len(events)-1].ID
			}
			// Catch up a bounded number of pages. Continue from the last sequence on
			// the next tick rather than repeatedly displaying the first old events.
			for page := 0; page < 4; page++ {
				var next []bridge.Event
				if err := m.client.Call(ctx, "events", map[string]any{"group_id": groupID, "after": after, "limit": 256}, nil, &next); err != nil {
					return refreshed{snapshot: snapshot, err: err}
				}
				events = append(events, next...)
				if len(next) == 0 {
					break
				}
				after = next[len(next)-1].ID
				if len(next) < 256 {
					break
				}
			}
		}
		if len(events) > 8 {
			events = events[len(events)-8:]
		}
		return refreshed{snapshot: snapshot, events: events, groupID: groupID}
	}
}

func (m model) groupID() string {
	if m.groupCursor < 0 || m.groupCursor >= len(m.snapshot.Groups) {
		return ""
	}
	return m.snapshot.Groups[m.groupCursor].ID
}

func (m model) visibleSessions() []bridge.Session {
	q := strings.ToLower(m.filter)
	result := make([]bridge.Session, 0, len(m.snapshot.Sessions))
	for _, s := range m.snapshot.Sessions {
		if q == "" || strings.Contains(strings.ToLower(string(s.Provider)+" "+s.ID+" "+s.Name+" "+s.CWD+" "+string(s.Status)), q) {
			result = append(result, s)
		}
	}
	return result
}

func (m model) selectedIDs() []string {
	ids := make([]string, 0, len(m.selected))
	for id, selected := range m.selected {
		if selected {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func (m model) create() tea.Cmd {
	params := map[string]any{"name": strings.TrimSpace(m.name), "members": m.selectedIDs(), "allow_tmux": m.allowTmux}
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(m.ctx, 20*time.Second)
		defer cancel()
		var group bridge.Group
		err := m.client.Call(ctx, "group.create", params, nil, &group)
		return created{group: group, err: err}
	}
}

func (m model) invite() tea.Cmd {
	groupID, deliver := m.groupID(), m.deliverInvites
	var ids []string
	for _, member := range m.snapshot.Members {
		if member.GroupID == groupID {
			ids = append(ids, member.Session.ID)
		}
	}
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(m.ctx, 2*time.Minute)
		defer cancel()
		result := invited{}
		for _, id := range ids {
			var response struct {
				CredentialsPath string `json:"credentials_path"`
				Delivered       bool   `json:"delivered"`
				Error           string `json:"error"`
			}
			err := m.client.Call(ctx, "invite", map[string]any{"group_id": groupID, "session_id": id, "deliver": deliver}, nil, &response)
			if err != nil {
				result.err = fmt.Errorf("invitation for %s failed: %w", id, err)
				return result
			}
			result.paths = append(result.paths, id+": "+response.CredentialsPath)
			if deliver {
				status := "notification accepted; waiting for join"
				if !response.Delivered {
					status = "not delivered: " + response.Error
				}
				result.delivery = append(result.delivery, id+": "+status)
			}
		}
		return result
	}
}

func (m model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := message.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
	case tick:
		return m, tea.Batch(m.refresh(false), nextTick())
	case refreshed:
		m.err = msg.err
		if msg.err == nil {
			oldID := m.groupID()
			m.snapshot = msg.snapshot
			for i, group := range m.snapshot.Groups {
				if group.ID == oldID {
					m.groupCursor = i
					break
				}
			}
			m.clamp()
			if msg.groupID == m.groupID() {
				m.events = msg.events
				m.eventsGroup = msg.groupID
			}
		}
	case created:
		m.busy, m.mode, m.err = false, browse, msg.err
		if msg.err == nil {
			m.snapshot.Groups = append(m.snapshot.Groups, msg.group)
			m.groupCursor, m.focus = len(m.snapshot.Groups)-1, 1
			m.selected = make(map[string]bool)
			m.notice = "Created group " + msg.group.Name + ". Press i to review invitations."
			return m, m.refresh(false)
		}
	case invited:
		m.busy, m.mode, m.err = false, browse, msg.err
		m.notice = "Private credentials written (share each invitation only with its member):\n" + strings.Join(msg.paths, "\n")
		if len(msg.delivery) != 0 {
			m.notice += "\n" + strings.Join(msg.delivery, "\n")
		}
		if !m.deliverInvites {
			m.notice += "\nUse CLI invite output for the full join instructions; no notification was sent."
		}
		return m, m.refresh(false)
	case tea.KeyMsg:
		key := msg.String()
		if key == "ctrl+c" {
			return m, tea.Quit
		}
		if m.busy {
			return m, nil
		}
		if m.mode == nameInput || m.mode == filterInput {
			switch key {
			case "esc":
				m.mode = browse
			case "enter":
				if m.mode == nameInput {
					if strings.TrimSpace(m.name) == "" {
						m.err = fmt.Errorf("group name is required")
					} else {
						m.mode, m.err = confirmCreate, nil
					}
				} else {
					m.mode = browse
				}
			case "backspace", "ctrl+h":
				if m.mode == nameInput {
					m.name = popRune(m.name)
				} else {
					m.filter = popRune(m.filter)
				}
			default:
				if msg.Type == tea.KeyRunes || msg.Type == tea.KeySpace {
					runes := msg.Runes
					if msg.Type == tea.KeySpace {
						runes = []rune{' '}
					}
					if m.mode == nameInput && len(m.name) < 240 {
						m.name += string(runes)
					}
					if m.mode == filterInput && len(m.filter) < 240 {
						m.filter += string(runes)
						m.sessionCursor = 0
					}
				}
			}
			return m, nil
		}
		if m.mode == confirmCreate || m.mode == confirmInvite {
			switch key {
			case "esc", "n":
				m.mode = browse
			case "d":
				if m.mode == confirmInvite {
					m.deliverInvites = !m.deliverInvites
				}
			case "y":
				m.busy, m.err = true, nil
				if m.mode == confirmCreate {
					return m, m.create()
				}
				return m, m.invite()
			}
			return m, nil
		}
		switch key {
		case "q":
			return m, tea.Quit
		case "tab":
			m.focus = 1 - m.focus
		case "up", "k":
			if m.focus == 0 {
				m.sessionCursor--
			} else {
				m.groupCursor--
			}
			m.clamp()
			if m.focus == 1 {
				m.events = nil
				return m, m.refresh(false)
			}
		case "down", "j":
			if m.focus == 0 {
				m.sessionCursor++
			} else {
				m.groupCursor++
			}
			m.clamp()
			if m.focus == 1 {
				m.events = nil
				return m, m.refresh(false)
			}
		case " ":
			if m.focus == 0 {
				sessions := m.visibleSessions()
				if m.sessionCursor < len(sessions) {
					id := sessions[m.sessionCursor].ID
					m.selected[id] = !m.selected[id]
				}
			}
		case "/":
			m.mode = filterInput
		case "t":
			m.allowTmux = !m.allowTmux
		case "r":
			m.notice = "Discovering local sessions…"
			return m, m.refresh(true)
		case "g":
			if len(m.selectedIDs()) == 0 {
				m.err = fmt.Errorf("select at least one session with Space")
			} else {
				m.name, m.mode, m.err = "", nameInput, nil
			}
		case "i":
			if m.groupID() == "" {
				m.err = fmt.Errorf("select a group first")
			} else {
				m.mode, m.deliverInvites, m.err = confirmInvite, false, nil
			}
		}
	}
	return m, nil
}

func (m *model) clamp() {
	m.sessionCursor = clampCursor(m.sessionCursor, len(m.visibleSessions()))
	m.groupCursor = clampCursor(m.groupCursor, len(m.snapshot.Groups))
}

func clampCursor(value, length int) int {
	if length == 0 || value < 0 {
		return 0
	}
	if value >= length {
		return length - 1
	}
	return value
}

func popRune(s string) string {
	runes := []rune(s)
	if len(runes) == 0 {
		return s
	}
	return string(runes[:len(runes)-1])
}

func (m model) View() string {
	var b strings.Builder
	b.WriteString("agent-bridge  •  local agent-bridge/1 protocol\n")
	b.WriteString("Tab panel · ↑/↓ move · Space select · / filter · g create · t tmux consent · i invite · r discover · q quit\n")
	fmt.Fprintf(&b, "Filter: %s   Selected: %d   tmux delivery consent for new group: %t\n", clean(m.filter), len(m.selectedIDs()), m.allowTmux)
	if m.mode == nameInput {
		fmt.Fprintf(&b, "Group name: %s█  (Enter review, Esc cancel)\n", clean(m.name))
	}
	if m.mode == filterInput {
		b.WriteString("Type filter, Enter done, Esc back\n")
	}
	if m.mode == confirmCreate {
		fmt.Fprintf(&b, "\nCreate group %q with these members?\n", clean(m.name))
		for _, id := range m.selectedIDs() {
			fmt.Fprintf(&b, "  %s\n", clean(id))
		}
		fmt.Fprintf(&b, "tmux message injection allowed: %t\nPress y to create, n/Esc to cancel.\n", m.allowTmux)
	} else if m.mode == confirmInvite {
		fmt.Fprintf(&b, "\nInvite all members of group %s? Existing credentials will be rotated.\n", clean(m.groupID()))
		for _, member := range m.snapshot.Members {
			if member.GroupID == m.groupID() {
				fmt.Fprintf(&b, "  %s  transport=%s  joined=%t\n", clean(member.Session.ID), member.Session.Capabilities.Transport, member.Joined)
			}
		}
		fmt.Fprintf(&b, "Deliver invitations through provider/tmux: %t (d toggles).\nPress y to write credentials%s, n/Esc to cancel.\n", m.deliverInvites, map[bool]string{true: " and deliver notifications", false: " only"}[m.deliverInvites])
	} else {
		b.WriteString("\n")
		if m.focus == 0 {
			b.WriteString("> SESSIONS\n")
		} else {
			b.WriteString("  SESSIONS\n")
		}
		sessions := m.visibleSessions()
		rowLimit := max(3, min(12, (m.height-16)/2))
		start := max(0, m.sessionCursor-rowLimit+1)
		for i := start; i < min(len(sessions), start+rowLimit); i++ {
			s := sessions[i]
			cursor, selected := " ", " "
			if m.focus == 0 && m.sessionCursor == i {
				cursor = ">"
			}
			if m.selected[s.ID] {
				selected = "x"
			}
			activity := "saved"
			if s.Active {
				activity = "live"
			}
			fmt.Fprintf(&b, "%s[%s] %-6s %-5s %-8s %-12s %s  %s\n", cursor, selected, s.Provider, activity, s.Status, s.Capabilities.Transport, short(s.ID, 22), short(s.Name+" "+s.CWD, max(12, m.width-72)))
		}
		if len(sessions) == 0 {
			b.WriteString("  No sessions. Press r to discover; daemon warnings appear below.\n")
		}
		if len(sessions) > rowLimit {
			fmt.Fprintf(&b, "  %d sessions total; scroll to see more\n", len(sessions))
		}
		if m.focus == 1 {
			b.WriteString("\n> GROUPS\n")
		} else {
			b.WriteString("\n  GROUPS\n")
		}
		groupStart := max(0, m.groupCursor-3)
		for i := groupStart; i < min(len(m.snapshot.Groups), groupStart+4); i++ {
			g := m.snapshot.Groups[i]
			cursor := " "
			if i == m.groupCursor {
				cursor = ">"
			}
			fmt.Fprintf(&b, "%s %s  %s\n", cursor, short(g.ID, 36), short(g.Name, max(12, m.width-42)))
		}
		if len(m.snapshot.Groups) == 0 {
			b.WriteString("  No groups. Select sessions, then press g.\n")
		}
		for _, member := range m.snapshot.Members {
			if member.GroupID == m.groupID() {
				joined := "needs-join"
				if member.Joined {
					joined = "joined"
				}
				heartbeat := "no heartbeat"
				if !member.LastHeartbeat.IsZero() {
					heartbeat = "heartbeat " + max(time.Duration(0), time.Since(member.LastHeartbeat)).Round(time.Second).String() + " ago"
				}
				fmt.Fprintf(&b, "  %-12s %-10s %-8s %s\n", short(member.Session.Name, 12), joined, member.State.Status, short(member.State.Task+" "+member.State.Summary, max(12, m.width-38)))
				fmt.Fprintf(&b, "    %s; knows: %s\n", heartbeat, short(strings.Join(member.State.Topics, ", "), max(12, m.width-40)))
			}
		}
		if len(m.events) != 0 {
			b.WriteString("Recent group events (delivery accepted does not imply read):\n")
			for _, event := range m.events[max(0, len(m.events)-3):] {
				fmt.Fprintf(&b, "  #%d %s %s\n", event.ID, short(event.Type, 28), short(event.SessionID, 32))
			}
		}
	}
	if m.busy {
		b.WriteString("\nWorking…\n")
	}
	if m.err != nil {
		fmt.Fprintf(&b, "\nError: %s\n", clean(m.err.Error()))
	}
	if m.notice != "" {
		fmt.Fprintf(&b, "\n%s\n", clean(m.notice))
	}
	for _, warning := range m.snapshot.Warnings {
		fmt.Fprintf(&b, "Warning: %s\n", short(warning, m.width))
	}
	return b.String()
}

// Session names, paths, and provider diagnostics are untrusted terminal text.
func clean(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return r
		}
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, s)
}

func short(s string, width int) string {
	s = strings.ReplaceAll(strings.ReplaceAll(clean(s), "\n", " "), "\t", " ")
	r := []rune(s)
	if width < 1 {
		return ""
	}
	if len(r) <= width {
		return s
	}
	if width == 1 {
		return "…"
	}
	return string(r[:width-1]) + "…"
}
