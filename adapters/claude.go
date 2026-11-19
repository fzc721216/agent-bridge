package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	bridge "github.com/fuzicheng/agent-bridge"
)

func (m *Manager) discoverClaude(ctx context.Context) ([]bridge.Session, []string) {
	all := map[string]bridge.Session{}
	var warnings []string
	data, err := m.run(ctx, m.opts.ClaudeBin, "agents", "--json")
	if err == nil {
		var rows []json.RawMessage
		if err := json.Unmarshal(data, &rows); err != nil {
			warnings = append(warnings, "claude agents: unsupported JSON response")
		} else {
			for _, row := range rows {
				s, err := parseClaudeRecord(row, true)
				if err != nil {
					continue
				}
				all[s.NativeID] = s
			}
		}
	} else {
		warnings = append(warnings, "claude discovery: "+err.Error())
	}
	files, _ := filepath.Glob(filepath.Join(m.opts.ClaudeHome, "sessions", "*.json"))
	if len(files) > 4096 {
		files = files[:4096]
		warnings = append(warnings, "claude metadata: session file limit reached")
	}
	for _, path := range files {
		data, err := readSmallFile(path)
		if err != nil {
			continue
		}
		s, err := parseClaude(data)
		if err != nil {
			continue
		}
		if old, ok := all[s.NativeID]; ok {
			if old.LastSeen.After(s.LastSeen) || old.Active && !s.Active {
				continue
			}
			if s.Name == "" {
				s.Name = old.Name
			}
			if s.CWD == "" {
				s.CWD = old.CWD
			}
		}
		all[s.NativeID] = s
	}
	result := make([]bridge.Session, 0, len(all))
	for _, s := range all {
		result = append(result, s)
	}
	return result, warnings
}

func parseClaude(data []byte) (bridge.Session, error) {
	return parseClaudeRecord(data, false)
}

func parseClaudeRecord(data []byte, authoritative bool) (bridge.Session, error) {
	var row map[string]json.RawMessage
	if err := json.Unmarshal(data, &row); err != nil {
		return bridge.Session{}, err
	}
	getstr := func(names ...string) string {
		for _, key := range names {
			var s string
			if json.Unmarshal(row[key], &s) == nil && s != "" {
				return s
			}
		}
		return ""
	}
	getint := func(names ...string) int64 {
		for _, key := range names {
			var n int64
			if json.Unmarshal(row[key], &n) == nil && n != 0 {
				return n
			}
			var s string
			if json.Unmarshal(row[key], &s) == nil {
				n, _ := strconv.ParseInt(s, 10, 64)
				if n != 0 {
					return n
				}
			}
		}
		return 0
	}
	id := getstr("sessionId", "session_id", "id")
	if !validNativeID(id) {
		return bridge.Session{}, fmt.Errorf("invalid claude session id")
	}
	s := bridge.Session{ID: "claude:" + id, Provider: bridge.Claude, NativeID: id, Name: getstr("name", "title", "displayName"), CWD: getstr("cwd", "workingDirectory", "working_directory"), PID: int(getint("pid", "processId", "process_id")), Status: bridge.Unknown, Capabilities: bridge.Capabilities{Transport: bridge.ObserveOnly}, Note: "saved metadata; live session not verified"}
	stamp := getint("updatedAt", "updated_at", "startedAt", "started_at")
	if stamp > 0 {
		if stamp < 1e12 {
			stamp *= 1000
		}
		s.LastSeen = time.UnixMilli(stamp).UTC()
	} else {
		for _, key := range []string{"updatedAt", "updated_at", "startedAt"} {
			var t time.Time
			if json.Unmarshal(row[key], &t) == nil && !t.IsZero() {
				s.LastSeen = t
				break
			}
		}
	}
	p, err := inspectProcess(s.PID)
	if err != nil || !providerProcess(p.Command, bridge.Claude) {
		return s, nil
	}
	expected := getstr("procStart", "processStart", "process_start")
	if expected == "" {
		if start := getint("procStart", "processStart", "process_start"); start > 0 {
			expected = strconv.FormatInt(start, 10)
		}
	}
	// PID reuse invalidates a metadata record; missing start information can be
	// accepted only from the provider's freshly queried active-session registry.
	if expected == "" && !authoritative {
		s.Note = "saved metadata lacks process generation; live session not verified"
		return s, nil
	}
	if expected != "" && expected != p.Start {
		s.Note = "stale metadata: process identity changed"
		return s, nil
	}
	s.ProcessStart = p.Start
	s.Active = true
	s.Note = "live Claude session; register via CLI/MCP for collaboration, or explicitly enable tmux"
	status := bridge.Status(strings.ToLower(getstr("status")))
	if status.Valid() {
		s.Status = status
	}
	return s, nil
}
