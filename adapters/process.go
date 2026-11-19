package adapters

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	bridge "github.com/fuzicheng/agent-bridge"
)

// processInfo describes one currently live process owned by this user. Start is
// an OS start-time identifier, not a timestamp inferred from session history.
type processInfo struct {
	PID     int
	PPID    int
	Start   string
	Command string
}

func inspectProcess(pid int) (processInfo, error) {
	if pid <= 0 {
		return processInfo{}, fmt.Errorf("invalid process id")
	}
	if runtime.GOOS == "linux" {
		return inspectLinuxProcess(pid)
	}
	if runtime.GOOS == "darwin" {
		return inspectDarwinProcess(pid)
	}
	return processInfo{}, fmt.Errorf("process discovery is unsupported on %s", runtime.GOOS)
}

func inspectLinuxProcess(pid int) (processInfo, error) {
	path := filepath.Join("/proc", strconv.Itoa(pid))
	info, err := os.Stat(path)
	if err != nil {
		return processInfo{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return processInfo{}, fmt.Errorf("process %d is not owned by the current user", pid)
	}
	data, err := readProcessFile(filepath.Join(path, "stat"), 8192)
	if err != nil {
		return processInfo{}, err
	}
	parent, start, err := parseLinuxStat(string(data))
	if err != nil {
		return processInfo{}, err
	}
	data, err = readProcessFile(filepath.Join(path, "cmdline"), 65536)
	if err != nil {
		return processInfo{}, err
	}
	command := strings.TrimSpace(strings.ReplaceAll(string(data), "\x00", " "))
	if command == "" {
		return processInfo{}, fmt.Errorf("process %d has no live command line", pid)
	}
	// Re-read the start identifier to reject a process replaced during inspection.
	check, err := readProcessFile(filepath.Join(path, "stat"), 8192)
	if err != nil {
		return processInfo{}, err
	}
	_, current, err := parseLinuxStat(string(check))
	if err != nil || current != start {
		return processInfo{}, fmt.Errorf("process %d changed during inspection", pid)
	}
	return processInfo{PID: pid, PPID: parent, Start: start, Command: command}, nil
}

func readProcessFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err == nil && int64(len(data)) > limit {
		err = fmt.Errorf("process metadata exceeds limit")
	}
	return data, err
}

func parseLinuxStat(line string) (int, string, error) {
	// A Linux comm field may contain spaces or parentheses. The final closing
	// parenthesis precedes fixed stat fields starting at field 3 (process state).
	end := strings.LastIndexByte(line, ')')
	if end < 0 {
		return 0, "", fmt.Errorf("invalid proc stat")
	}
	fields := strings.Fields(line[end+1:])
	if len(fields) < 20 || fields[0] == "Z" || fields[0] == "X" {
		return 0, "", fmt.Errorf("process has exited or stat is incomplete")
	}
	parent, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, "", fmt.Errorf("invalid process parent: %w", err)
	}
	if _, err := strconv.ParseUint(fields[19], 10, 64); err != nil {
		return 0, "", fmt.Errorf("invalid process start: %w", err)
	}
	return parent, fields[19], nil
}

func inspectDarwinProcess(pid int) (processInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// args includes the script path for interpreter-based CLI installations;
	// comm alone would identify them only as node or sh on macOS.
	cmd := exec.CommandContext(ctx, "ps", "-p", strconv.Itoa(pid), "-o", "uid=", "-o", "ppid=", "-o", "lstart=", "-o", "args=")
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	data, err := cmd.Output()
	if err != nil {
		return processInfo{}, err
	}
	fields := strings.Fields(string(data))
	if len(fields) < 8 {
		return processInfo{}, fmt.Errorf("invalid process metadata")
	}
	uid, err := strconv.Atoi(fields[0])
	if err != nil || uid != os.Geteuid() {
		return processInfo{}, fmt.Errorf("process %d is not owned by the current user", pid)
	}
	parent, err := strconv.Atoi(fields[1])
	if err != nil {
		return processInfo{}, fmt.Errorf("invalid process parent: %w", err)
	}
	return processInfo{PID: pid, PPID: parent, Start: strings.Join(fields[2:7], " "), Command: strings.Join(fields[7:], " ")}, nil
}

// lockedCodexThreads discovers actual active writer lock owners. A recently
// modified history entry or an unlocked leftover .lock file does not qualify.
func lockedCodexThreads(home string) map[string]processInfo {
	result := make(map[string]processInfo)
	if runtime.GOOS != "linux" {
		return result
	}
	locks, err := readProcessFile("/proc/locks", 8<<20)
	if err != nil {
		return result
	}
	type lockKey struct{ major, minor, inode uint64 }
	owners := make(map[lockKey]int)
	for _, line := range strings.Split(string(locks), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 6 || fields[1] != "FLOCK" || fields[3] != "WRITE" {
			continue
		}
		pid, err := strconv.Atoi(fields[4])
		parts := strings.Split(fields[5], ":")
		if err != nil || len(parts) != 3 {
			continue
		}
		major, e1 := strconv.ParseUint(parts[0], 16, 64)
		minor, e2 := strconv.ParseUint(parts[1], 16, 64)
		inode, e3 := strconv.ParseUint(parts[2], 10, 64)
		if e1 == nil && e2 == nil && e3 == nil {
			owners[lockKey{major, minor, inode}] = pid
		}
	}
	dir, err := os.Open(filepath.Join(home, "thread-writer-locks"))
	if err != nil {
		return result
	}
	defer dir.Close()
	entries, err := dir.ReadDir(10001)
	if err != nil && err != io.EOF || len(entries) > 10000 {
		return result
	}
	for _, entry := range entries {
		name := entry.Name()
		id := strings.TrimSuffix(name, ".lock")
		if id == name || strings.HasPrefix(name, ".") || !validNativeID(id) || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || int(stat.Uid) != os.Geteuid() {
			continue
		}
		dev := uint64(stat.Dev)
		major := (dev&0x00000000000fff00)>>8 | (dev&0xfffff00000000000)>>32
		minor := (dev & 0x00000000000000ff) | (dev&0x00000ffffff00000)>>12
		pid, ok := owners[lockKey{major, minor, stat.Ino}]
		if !ok {
			continue
		}
		process, err := inspectProcess(pid)
		if err == nil && providerProcess(process.Command, bridge.Codex) {
			result[id] = process
		}
	}
	return result
}
