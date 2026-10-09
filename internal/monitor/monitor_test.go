package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type testOptions struct {
	logPath      string
	maxEvents    int
	initialBytes int64
}

func testMonitor(t *testing.T, opts testOptions) *Monitor {
	t.Helper()
	m, err := New(opts.logPath, opts.maxEvents, opts.initialBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return m
}

func writeLog(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func appendLog(t *testing.T, path string, data []byte) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		t.Fatal(err)
	}
}

func TestAppendPartialAndSnapshotCopy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.log")
	writeLog(t, path, record(t, "SELECT 1", nil))
	m := testMonitor(t, testOptions{logPath: path, maxEvents: 2000, initialBytes: 2 * 1024 * 1024})
	m.Poll()
	partial := record(t, "SELECT 2", nil)
	appendLog(t, path, partial[:len(partial)-1])
	m.Poll()
	if len(m.Snapshot().Events) != 1 {
		t.Fatal("incomplete line consumed")
	}
	appendLog(t, path, []byte{'\n'})
	m.Poll()
	snapshot := m.Snapshot()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Events) != 2 || snapshot.Events[0].ID != 1 || snapshot.Events[1].ID != 2 || snapshot.Status.LinesSeen != 2 || snapshot.Status.BytesRead != info.Size() || snapshot.Status.LastRead == nil {
		t.Fatalf("bad snapshot: %+v", snapshot)
	}
	snapshot.Events[0].SQL = "mutated"
	*snapshot.Events[0].Rows = 7
	*snapshot.Events[0].ElapsedMS = 7
	*snapshot.Status.LastRead = "mutated"
	snapshot.Status.Capacity = 7
	again := m.Snapshot()
	if again.Events[0].SQL != "SELECT ?" || *again.Events[0].Rows != 1 || *again.Events[0].ElapsedMS != 38.01 || *again.Status.LastRead == "mutated" || again.Status.Capacity != 2000 {
		t.Fatal("snapshot aliases state")
	}
}

func TestInitialTailBoundaries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.log")
	first, second := record(t, "SELECT 1", nil), record(t, "SELECT 2", nil)
	writeLog(t, path, append(bytes.Clone(first), second...))
	for _, tc := range []struct {
		initial int64
		want    []string
	}{
		{int64(len(second) + 7), []string{"SELECT 2"}},
		{int64(len(second)), []string{"SELECT 2"}},
		{int64(len(first) + len(second)), []string{"SELECT 1", "SELECT 2"}},
		{0, []string{}},
	} {
		m := testMonitor(t, testOptions{logPath: path, maxEvents: 2000, initialBytes: tc.initial})
		m.Poll()
		got := []string{}
		for _, event := range m.Snapshot().Events {
			raw, _ := m.RawEvent(event.ID)
			got = append(got, raw.SQL)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("initial %d got %v want %v", tc.initial, got, tc.want)
		}
	}
}

func TestInitialZeroPartialEOF(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.log")
	first := record(t, "SELECT 1", nil)
	writeLog(t, path, first[:len(first)-1])
	m := testMonitor(t, testOptions{logPath: path, maxEvents: 2000})
	m.Poll()
	appendLog(t, path, append([]byte{'\n'}, record(t, "SELECT 2", nil)...))
	m.Poll()
	raw, found := m.RawEvent(1)
	if !found || raw.SQL != "SELECT 2" || len(m.Snapshot().Events) != 1 {
		t.Fatalf("bad initial partial handling: %+v", m.Snapshot())
	}
}

func TestMissingAndNonregularSources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.log")
	m := testMonitor(t, testOptions{logPath: path, maxEvents: 2000, initialBytes: 2 * 1024 * 1024})
	m.Poll()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("missing file was created: %v", err)
	}
	if status := m.Snapshot().Status; status.Exists || !strings.Contains(status.Error, "missing") {
		t.Fatalf("bad missing status: %+v", status)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	m.Poll()
	if !strings.Contains(m.Snapshot().Status.Error, "Cannot read") {
		t.Fatal("directory accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	m.Poll()
	if !strings.Contains(m.Snapshot().Status.Error, "Cannot read") {
		t.Fatal("FIFO accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	writeLog(t, path, record(t, "SELECT 1", nil))
	m.Poll()
	if snapshot := m.Snapshot(); snapshot.Status.Error != "" || !snapshot.Status.Exists || len(snapshot.Events) != 1 {
		t.Fatalf("failed to recover: %+v", snapshot)
	}
}

func TestPermissionRetry(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permissions")
	}
	path := filepath.Join(t.TempDir(), "service.log")
	writeLog(t, path, record(t, "SELECT 1", nil))
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	m := testMonitor(t, testOptions{logPath: path, maxEvents: 2000, initialBytes: 2 * 1024 * 1024})
	m.Poll()
	if !strings.Contains(m.Snapshot().Status.Error, "Cannot read") {
		t.Fatal("permission failure not reported")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	m.Poll()
	if snapshot := m.Snapshot(); snapshot.Status.Error != "" || len(snapshot.Events) != 1 {
		t.Fatalf("failed permission recovery: %+v", snapshot)
	}
}

func TestTruncationAndFastRegrow(t *testing.T) {
	for _, fast := range []bool{false, true} {
		t.Run(fmt.Sprint(fast), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "service.log")
			data := record(t, "SELECT 'old value'", nil)
			if !fast {
				data = append(data, []byte(`{"msg":"SQL Query",`)...)
			}
			writeLog(t, path, data)
			before, _ := os.Stat(path)
			m := testMonitor(t, testOptions{logPath: path, maxEvents: 2000, initialBytes: 2 * 1024 * 1024})
			m.Poll()
			replacement := record(t, "SELECT 22222222", nil)
			if fast {
				replacement = append(replacement, record(t, "SELECT 3333333", nil)...)
			}
			writeLog(t, path, replacement)
			after, _ := os.Stat(path)
			if !os.SameFile(before, after) {
				t.Fatal("test changed inode")
			}
			m.Poll()
			snapshot := m.Snapshot()
			raw, found := m.RawEvent(2)
			if snapshot.Status.Rotations != 1 || snapshot.Status.ParseErrors != 0 || !found || raw.SQL != "SELECT 22222222" {
				t.Fatalf("rotation failed: %+v %+v", snapshot, raw)
			}
			want := 2
			if fast {
				want = 3
			}
			if len(snapshot.Events) != want {
				t.Fatalf("got %d events, want %d", len(snapshot.Events), want)
			}
		})
	}
}

func TestInodeRotationAndRecreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.log")
	writeLog(t, path, append(record(t, "SELECT 1", nil), []byte(`{"msg":"SQL Query",`)...))
	m := testMonitor(t, testOptions{logPath: path, maxEvents: 2000, initialBytes: 1 << 20})
	m.Poll()
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	m.Poll()
	if m.Snapshot().Status.Exists {
		t.Fatal("missing rotation not detected")
	}
	writeLog(t, path, record(t, "SELECT 2", nil))
	m.Poll()
	raw, found := m.RawEvent(2)
	if m.Snapshot().Status.Rotations != 1 || m.Snapshot().Status.ParseErrors != 0 || !found || raw.SQL != "SELECT 2" {
		t.Fatalf("recreate failed: %+v", m.Snapshot())
	}
	writeLog(t, path+".new", record(t, "SELECT 3", nil))
	if err := os.Rename(path+".new", path); err != nil {
		t.Fatal(err)
	}
	m.Poll()
	if raw, found := m.RawEvent(3); !found || raw.SQL != "SELECT 3" || m.Snapshot().Status.Rotations != 2 {
		t.Fatal("atomic replacement failed")
	}
}

func TestRetentionAndRawEviction(t *testing.T) {
	for _, capacity := range []int{1, 2, 2000} {
		m := testMonitor(t, testOptions{logPath: filepath.Join(t.TempDir(), "missing.log"), maxEvents: capacity})
		for i := 0; i < capacity+3; i++ {
			kind := []string{"SQL Query", "Slow SQL", "SQL Error"}[i%3]
			m.Consume(record(t, fmt.Sprintf("SELECT '%d'", i), map[string]any{"msg": kind}))
		}
		snapshot := m.Snapshot()
		if len(snapshot.Events) != capacity || len(m.raw) != capacity || snapshot.Status.DroppedEvents != 3 || snapshot.Events[0].ID != 4 || snapshot.Events[capacity-1].ID != uint64(capacity+3) {
			t.Fatalf("bad retention for %d: %+v", capacity, snapshot.Status)
		}
		if _, found := m.RawEvent(1); found {
			t.Fatal("evicted raw retained")
		}
		raw, found := m.RawEvent(4)
		if !found {
			t.Fatal("retained raw missing")
		}
		raw.SQL = "mutated"
		if again, _ := m.RawEvent(4); again.SQL == raw.SQL {
			t.Fatal("raw aliases state")
		}
	}
}

func TestOversizeRecoveryAndExactLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.log")
	writeLog(t, path, bytes.Repeat([]byte{'x'}, maxLine+37))
	m := testMonitor(t, testOptions{logPath: path, maxEvents: 2000, initialBytes: 2 * 1024 * 1024})
	m.Poll()
	if m.Snapshot().Status.OversizeLines != 1 || len(m.pending) != 0 {
		t.Fatal("oversize line not discarded while streaming")
	}
	appendLog(t, path, append([]byte("more discarded\n"), record(t, "SELECT 1", nil)...))
	m.Poll()
	if snapshot := m.Snapshot(); snapshot.Status.OversizeLines != 1 || snapshot.Status.LinesSeen != 2 || len(snapshot.Events) != 1 || snapshot.Status.ParseErrors != 0 {
		t.Fatalf("oversize recovery failed: %+v", snapshot)
	}
	m.Consume(bytes.Repeat([]byte{' '}, maxLine))
	m.Consume(append([]byte{'\n'}, record(t, "SELECT 2", nil)...))
	if snapshot := m.Snapshot(); snapshot.Status.OversizeLines != 1 || snapshot.Status.IgnoredLines != 1 || len(snapshot.Events) != 2 {
		t.Fatalf("exact limit rejected: %+v", snapshot)
	}
}

func TestCountersCompactContinuationAndUTF8Chunks(t *testing.T) {
	m := testMonitor(t, testOptions{logPath: filepath.Join(t.TempDir(), "missing.log"), maxEvents: 2000})
	m.Consume([]byte("{\"msg\":\"request\", \"body\":\"private-request\"}\n{\"msg\":\"SQL Query\",\n\"sql\":\"SELECT 1\"}\n"))
	for _, sql := range []string{"SELECT col7", "SELECT col7 FROM t", "SELECT 1;"} {
		m.Consume([]byte("[t] DEBUG [svc] [] caller.go:9 - SQL Query | sql=" + sql + "\nWHERE name=\"continuation-secret\"\n"))
		snapshot := m.Snapshot()
		event := snapshot.Events[len(snapshot.Events)-1]
		raw, _ := m.RawEvent(event.ID)
		if !event.Truncated || event.Source != "caller.go:9" || raw.SQL != sql {
			t.Fatal("compact framing mismatch")
		}
	}
	data := record(t, "SELECT 'µ-secret'\nFROM t", nil)
	for _, b := range data {
		m.Consume([]byte{b})
	}
	snapshot := m.Snapshot()
	if snapshot.Status.ParseErrors != 1 || snapshot.Status.IgnoredLines != 5 || len(snapshot.Events) != 4 || snapshot.Events[3].Truncated {
		t.Fatalf("counter or chunk mismatch: %+v", snapshot)
	}
	raw, _ := m.RawEvent(4)
	if raw.SQL != "SELECT 'µ-secret'\nFROM t" {
		t.Fatal("UTF-8 chunks broken")
	}
	encoded, _ := json.Marshal(snapshot)
	if strings.Contains(string(encoded), "private-request") || strings.Contains(string(encoded), "continuation-secret") || strings.Contains(string(encoded), "µ-secret") {
		t.Fatal("ignored or raw payload leaked")
	}
}

func TestPollReadBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.log")
	data := bytes.Repeat([]byte("ignored\n"), (32*readChunk)/8+10)
	data = append(data, record(t, "SELECT 1", nil)...)
	writeLog(t, path, data)
	m := testMonitor(t, testOptions{logPath: path, maxEvents: 2000, initialBytes: int64(len(data))})
	m.Poll()
	if m.Snapshot().Status.BytesRead != 32*readChunk {
		t.Fatal("poll read budget not enforced")
	}
	m.Poll()
	if m.Snapshot().Status.BytesRead != int64(len(data)) || len(m.Snapshot().Events) != 1 {
		t.Fatal("poll failed to resume")
	}
}

func TestConcurrentSnapshotsConsumeAndPoll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.log")
	writeLog(t, path, record(t, "SELECT 1", nil))
	m := testMonitor(t, testOptions{logPath: path, maxEvents: 10, initialBytes: 2 * 1024 * 1024})
	data := record(t, "SELECT 2", nil)
	var group sync.WaitGroup
	for range 4 {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 100 {
				snapshot := m.Snapshot()
				for _, event := range snapshot.Events {
					m.RawEvent(event.ID)
					*event.Rows = 999
				}
				m.Poll()
			}
		}()
	}
	for range 100 {
		m.Consume(data)
	}
	group.Wait()
	if len(m.Snapshot().Events) != 10 {
		t.Fatal("concurrent retention failed")
	}
}

func TestRunCancellationAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.log")
	writeLog(t, path, record(t, "SELECT 1", nil))
	m := testMonitor(t, testOptions{logPath: path, maxEvents: 2000, initialBytes: 2 * 1024 * 1024})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); m.Run(ctx) }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
waiting:
	for {
		select {
		case <-ticker.C:
			if len(m.Snapshot().Events) == 1 {
				break waiting
			}
		case <-deadline.C:
			t.Fatal("tail did not start")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("tail did not shut down")
	}
	if m.file != nil || !m.closed {
		t.Fatal("file left open")
	}
	m.Close()
	m.Poll()
	for _, capacity := range []int{-1, 0, 2001} {
		if _, err := New(path, capacity, 0); err == nil {
			t.Fatal("invalid capacity accepted")
		}
	}
	if _, err := New(path, 1, -1); err == nil {
		t.Fatal("negative initial bytes accepted")
	}
}
