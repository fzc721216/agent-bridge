package adapters

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestParseLinuxStatHandlesComplexCommandNames(t *testing.T) {
	fields := []string{"S", "23"}
	for len(fields) < 19 {
		fields = append(fields, "0")
	}
	fields = append(fields, "123456")
	parent, start, err := parseLinuxStat("123 (a program (name)) " + strings.Join(fields, " "))
	if err != nil || parent != 23 || start != "123456" {
		t.Fatalf("parent=%d start=%q err=%v", parent, start, err)
	}
	fields[0] = "Z"
	if _, _, err := parseLinuxStat("123 (zombie) " + strings.Join(fields, " ")); err == nil {
		t.Fatal("accepted a zombie process")
	}
	if _, _, err := parseLinuxStat("bad"); err == nil {
		t.Fatal("accepted malformed stat")
	}
}

func TestInspectProcessRejectsInvalidAndReportsCurrentIdentity(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("Unix process discovery")
	}
	if _, err := inspectProcess(-1); err == nil {
		t.Fatal("accepted invalid PID")
	}
	info, err := inspectProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if info.PID != os.Getpid() || info.PPID != os.Getppid() || info.Start == "" || info.Command == "" {
		t.Fatalf("incomplete process identity: %#v", info)
	}
	if runtime.GOOS == "linux" {
		if _, err := strconv.ParseUint(info.Start, 10, 64); err != nil {
			t.Fatalf("expected Linux start ticks: %v", err)
		}
	}
}
