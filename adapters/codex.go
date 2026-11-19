package adapters

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	bridge "github.com/fuzicheng/agent-bridge"
	_ "modernc.org/sqlite"
)

func (m *Manager) discoverCodex(ctx context.Context) ([]bridge.Session, []string) {
	result := map[string]bridge.Session{}
	var warnings []string
	files, _ := filepath.Glob(filepath.Join(m.opts.CodexHome, "state_*.sqlite"))
	sort.Slice(files, func(i, j int) bool { return stateVersion(files[i]) > stateVersion(files[j]) })
	if len(files) > 0 {
		sessions, err := readCodexDB(ctx, files[0])
		if err != nil {
			warnings = append(warnings, "codex metadata: "+err.Error())
		} else {
			for _, s := range sessions {
				result[s.NativeID] = s
			}
		}
	}
	index, err := readCodexIndex(filepath.Join(m.opts.CodexHome, "session_index.jsonl"))
	if err != nil && !os.IsNotExist(err) {
		warnings = append(warnings, "codex index: "+err.Error())
	}
	for id, s := range index {
		if old, ok := result[id]; ok {
			if s.Name != "" {
				old.Name = s.Name
			}
			result[id] = old
		} else {
			result[id] = s
		}
	}
	live := lockedCodexThreads(m.opts.CodexHome)
	for id, p := range m.loadedCodexThreads() {
		if _, ok := live[id]; !ok {
			live[id] = p
		}
	}
	queue := false
	if len(live) > 0 {
		queue = m.queueAvailable(ctx)
	}
	for id, p := range live {
		s, ok := result[id]
		if !ok {
			s = codexHistorical(id)
		}
		s.PID = p.PID
		s.ProcessStart = p.Start
		s.Active = true
		s.Status = bridge.Unknown
		s.Note = "live thread; task state requires agent handshake"
		if queue {
			s.Capabilities = bridge.Capabilities{CanSend: true, Transport: bridge.CodexQueue}
		} else {
			s.Note = "live thread; installed codex does not expose queue --thread/--message"
		}
		result[id] = s
	}
	all := make([]bridge.Session, 0, len(result))
	for _, s := range result {
		all = append(all, s)
	}
	return all, warnings
}

func stateVersion(path string) int {
	v := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "state_"), ".sqlite")
	n, _ := strconv.Atoi(v)
	return n
}

func codexHistorical(id string) bridge.Session {
	return bridge.Session{ID: "codex:" + id, Provider: bridge.Codex, NativeID: id, Status: bridge.Unknown, Capabilities: bridge.Capabilities{Transport: bridge.ObserveOnly}, Note: "saved metadata; live session not verified"}
}

func readCodexDB(ctx context.Context, path string) ([]bridge.Session, error) {
	u := url.URL{Scheme: "file", Path: path}
	q := u.Query()
	q.Set("mode", "ro")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "PRAGMA table_info(threads)")
	if err != nil {
		return nil, err
	}
	cols := map[string]bool{}
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); err != nil {
			rows.Close()
			return nil, err
		}
		cols[name] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if !cols["id"] {
		return nil, fmt.Errorf("unsupported threads schema")
	}
	column := func(name, fallback string) string {
		if cols[name] {
			return "COALESCE(\"" + name + "\", " + fallback + ")"
		}
		return fallback
	}
	name := column("title", "''")
	if cols["name"] {
		name = "COALESCE(NULLIF(name,''), " + name + ")"
	}
	updated := column("updated_at", "0") + " * 1000"
	if cols["updated_at_ms"] {
		updated = "COALESCE(updated_at_ms, " + updated + ")"
	}
	query := "SELECT id, " + name + ", " + column("cwd", "''") + ", " + updated + " FROM threads"
	if cols["archived"] {
		query += " WHERE archived = 0"
	}
	query += " ORDER BY " + updated + " DESC LIMIT 1000"
	rows, err = db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var all []bridge.Session
	for rows.Next() {
		var id, name, cwd string
		var updated int64
		if err := rows.Scan(&id, &name, &cwd, &updated); err != nil {
			return nil, err
		}
		if !validNativeID(id) {
			continue
		}
		s := codexHistorical(id)
		s.Name = name
		s.CWD = cwd
		if updated > 0 {
			s.LastSeen = time.UnixMilli(updated).UTC()
		}
		all = append(all, s)
	}
	return all, rows.Err()
}

func readCodexIndex(path string) (map[string]bridge.Session, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	const maxIndexBytes = 32 << 20
	// The index is append-only. Read its tail so a large history cannot hide the
	// most recently renamed/used sessions behind the size bound.
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(io.LimitReader(f, maxIndexBytes))
	if info.Size() > maxIndexBytes {
		if _, err := f.Seek(info.Size()-maxIndexBytes, io.SeekStart); err != nil {
			return nil, err
		}
		reader.Reset(io.LimitReader(f, maxIndexBytes))
		if _, err := reader.ReadString('\n'); err != nil && err != io.EOF {
			return nil, err
		}
	}
	scan := bufio.NewScanner(reader)
	scan.Buffer(make([]byte, 4096), 128<<10)
	all := map[string]bridge.Session{}
	for scan.Scan() {
		var row struct {
			ID      string    `json:"id"`
			Name    string    `json:"thread_name"`
			Updated time.Time `json:"updated_at"`
		}
		if json.Unmarshal(scan.Bytes(), &row) != nil || !validNativeID(row.ID) {
			continue
		}
		if old, ok := all[row.ID]; ok && old.LastSeen.After(row.Updated) {
			continue
		}
		s := codexHistorical(row.ID)
		s.Name = row.Name
		s.LastSeen = row.Updated
		all[row.ID] = s
	}
	return all, scan.Err()
}

func (m *Manager) loadedCodexThreads() map[string]processInfo {
	all := map[string]processInfo{}
	dir := filepath.Join(m.opts.CodexHome, "app-server-daemon")
	pidData, err := readSmallFile(filepath.Join(dir, "daemon.pid"))
	if err != nil {
		return all
	}
	pid, expectedStart, err := parseDaemonPID(pidData)
	if err != nil {
		return all
	}
	p, err := inspectProcess(pid)
	if err != nil || !providerProcess(p.Command, bridge.Codex) || !strings.Contains(p.Command, "app-server") || (runtime.GOOS == "linux" && expectedStart != "" && expectedStart != p.Start) {
		return all
	}
	data, err := readSmallFile(filepath.Join(dir, "loaded-threads.json"))
	if err != nil {
		return all
	}
	var v any
	if json.Unmarshal(data, &v) != nil {
		return all
	}
	// A deliberately limited set of known shapes: array of IDs/objects, or
	// threadIds/loadedThreadIds/threads wrapping such an array. No timestamps imply liveness.
	var collect func(any)
	collect = func(v any) {
		switch x := v.(type) {
		case string:
			if validNativeID(x) {
				all[x] = p
			}
		case []any:
			for _, v := range x {
				collect(v)
			}
		case map[string]any:
			if id, ok := x["id"].(string); ok && validNativeID(id) {
				all[id] = p
			}
			for _, key := range []string{"threadIds", "loadedThreadIds", "threads"} {
				if child, ok := x[key]; ok {
					collect(child)
				}
			}
		}
	}
	collect(v)
	return all
}

// Codex versions use either a plain PID or a structured process identity file.
// The JSON startTicks value lets us reject daemon PID reuse on Linux.
func parseDaemonPID(data []byte) (int, string, error) {
	trimmed := strings.TrimSpace(string(data))
	if pid, err := strconv.Atoi(trimmed); err == nil && pid > 0 {
		return pid, "", nil
	}
	var row struct {
		PID             int `json:"pid"`
		ProcessIdentity struct {
			StartTicks json.Number `json:"startTicks"`
		} `json:"processIdentity"`
	}
	if err := json.Unmarshal(data, &row); err != nil || row.PID <= 0 {
		return 0, "", fmt.Errorf("unsupported daemon process metadata")
	}
	return row.PID, row.ProcessIdentity.StartTicks.String(), nil
}

func readSmallFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 1<<20+1))
	if err != nil {
		return nil, err
	}
	if len(b) > 1<<20 {
		return nil, fmt.Errorf("metadata file exceeds 1MiB")
	}
	return b, nil
}
