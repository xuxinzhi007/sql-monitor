package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const nativeHeader = "2026/09/24 10:20:30 /demo/main.go:42"

func TestNativeGORM(t *testing.T) {
	for _, tc := range []struct{ suffix, kind, level, rows string }{
		{"", "query", "INFO", "1"},
		{" SLOW SQL >= 200ms", "slow", "WARN", "-"},
		{" no such table: secret_table", "error", "ERROR", "0"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			m := testMonitor(t, options{logPath: filepath.Join(t.TempDir(), "missing"), maxEvents: 2})
			text := "\r\n\x1b[32m" + nativeHeader + tc.suffix + "\x1b[0m\n\x1b[33m[12.300ms] \x1b[34;1m[rows:" + tc.rows + "]\x1b[0m SELECT id FROM demo WHERE label='private-µ'\r\n"
			for _, b := range []byte(text) {
				m.Consume([]byte{b})
			}
			snapshot := m.Snapshot()
			if len(snapshot.Events) != 1 {
				t.Fatalf("no native event: %+v", snapshot.Status)
			}
			event := snapshot.Events[0]
			if event.Kind != tc.kind || event.Level != tc.level || event.Source != "/demo/main.go:42" || event.ElapsedMS == nil || *event.ElapsedMS != 12.3 || event.TraceID != "" || event.SpanID != "" || !event.Truncated || event.SQL != "SELECT id FROM demo WHERE label=?" {
				t.Fatalf("native event: %+v", event)
			}
			if (event.Rows == nil) != (tc.rows == "-") {
				t.Fatal("unknown rows not preserved")
			}
			if snapshot.Status.Formats.GORM != 1 || snapshot.Status.SQLObserved != 1 || snapshot.Status.WithDuration != 1 || snapshot.Status.WithSource != 1 || snapshot.Status.WithTrace != 0 || snapshot.Status.ParseErrors != 0 {
				t.Fatalf("capabilities: %+v", snapshot.Status)
			}
			raw, _ := m.RawEvent(event.ID)
			if !strings.Contains(raw.SQL, "private-µ") || (raw.Error != "") != (tc.kind == "error") {
				t.Fatal("raw conversion")
			}
			other, _ := mustParse(t, record(t, raw.SQL, nil))
			if other.Fingerprint != event.Fingerprint {
				t.Fatal("GORM fingerprint diverges from JSON")
			}
			body, _ := json.Marshal(snapshot)
			if strings.Contains(string(body), "private-µ") || strings.Contains(string(body), "secret_table") {
				t.Fatal("GORM raw leak")
			}
		})
	}
}

func TestGORMMalformedAndConservativeFraming(t *testing.T) {
	cases := []struct {
		text                                 string
		events, invalid, incomplete, ignored int64
	}{
		{"[1.0ms] [rows:1] SELECT 1\n", 0, 1, 0, 0},
		{nativeHeader + "\n[badms] [rows:1] SELECT 1\n", 0, 1, 1, 0},
		{nativeHeader + "\n[1ms] [rows:1] request-body-secret\n", 0, 1, 0, 0},
		{nativeHeader + "\n[1ms] [rows:999999999999999999] SELECT 1\n", 0, 1, 0, 0},
		{nativeHeader + "\nprivate-request-body\n[1ms] [rows:1] SELECT 1\n", 0, 1, 1, 1},
		{nativeHeader + "\n" + nativeHeader + "\n[1ms] [rows:1] SELECT 1\n", 1, 0, 1, 0},
		{nativeHeader + "\n[1ms] [rows:1] SELECT id\nFROM demo WHERE secret='private'\n", 1, 0, 0, 1},
		{nativeHeader + "\n[1ms] [rows:1] SELECT 'private...\n", 1, 0, 0, 0},
		{nativeHeader + "\n", 0, 0, 1, 0},
		{nativeHeader + "\n[warn] request-body-secret SELECT 1\n", 0, 0, 0, 2},
		{nativeHeader + "\n[info] ordinary non-SQL log\n", 0, 0, 0, 2},
		{nativeHeader + "\n[1ms] [rows:1] ", 0, 1, 1, 0},
		{"2026/99/24 10:20:30 /demo/main.go:42\n", 0, 1, 0, 0},
	}
	for _, tc := range cases {
		m := testMonitor(t, options{logPath: filepath.Join(t.TempDir(), "missing"), maxEvents: 2})
		m.Consume([]byte(tc.text))
		m.finishInput(io.EOF)
		snapshot := m.Snapshot()
		status := snapshot.Status
		if status.SQLObserved != tc.events || status.ParseErrors != tc.invalid || status.IncompleteRecords != tc.incomplete || status.IgnoredLines != tc.ignored {
			t.Fatalf("%q: %+v", tc.text, status)
		}
		for _, event := range snapshot.Events {
			if !event.Truncated {
				t.Fatal("multiline completeness claimed")
			}
		}
		body, _ := json.Marshal(snapshot)
		if strings.Contains(string(body), "private") {
			t.Fatal("continuation or malformed body leaked")
		}
	}
}

func TestGORMTemporaryEOFAndRotation(t *testing.T) {
	for _, replace := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "gorm.log")
		writeLog(t, path, []byte(nativeHeader+"\n"))
		m := testMonitor(t, options{logPath: path, maxEvents: 4, initialBytes: 1 << 20})
		m.Poll()
		if m.Snapshot().Status.IncompleteRecords != 0 {
			t.Fatal("temporary EOF flushed header")
		}
		appendLog(t, path, []byte("[1ms] [rows:1] SELECT 1\n"+nativeHeader+"\n"))
		m.Poll()
		if len(m.Snapshot().Events) != 1 {
			t.Fatal("header lost at temporary EOF")
		}
		if replace {
			if err := os.Rename(path, path+".old"); err != nil {
				t.Fatal(err)
			}
		}
		writeLog(t, path, []byte("[1ms] [rows:1] SELECT 2\n"))
		m.Poll()
		status := m.Snapshot().Status
		if status.SQLObserved != 1 || status.Rotations != 1 || status.IncompleteRecords != 1 || status.ParseErrors != 1 {
			t.Fatalf("rotation frame not reset: %+v", status)
		}
	}
}

func TestGORMBoundsAndCapabilityRetention(t *testing.T) {
	m := testMonitor(t, options{logPath: filepath.Join(t.TempDir(), "missing"), maxEvents: 1})
	m.Consume([]byte(nativeHeader + "\n" + strings.Repeat("x", maxLine+1)))
	if len(m.pending) != 0 || m.gormPending != nil {
		t.Fatal("oversize pending retained")
	}
	m.Consume([]byte("\n" + nativeHeader + "\n[1ms] [rows:1] SELECT 1\n"))
	m.Consume(record(t, "SELECT 2", map[string]any{"trace.id": strings.Repeat("a", 32), "source": "", "elapsed": nil}))
	m.Consume([]byte("[t] INFO [demo] caller.go:9 - SQL Query | 2ms | sql=SELECT 3\n"))
	snapshot := m.Snapshot()
	s := snapshot.Status
	if s.SQLObserved != 3 || s.WithDuration != 2 || s.WithSource != 2 || s.WithTrace != 1 || s.Formats != (FormatCounts{JSON: 1, Compact: 1, GORM: 1}) || s.DroppedEvents != 2 || len(snapshot.Events) != 1 || len(m.raw) != 1 {
		t.Fatalf("capability retention: %+v", s)
	}
	snapshot.Status.Formats.GORM = 99
	if m.Snapshot().Status.Formats.GORM != 1 {
		t.Fatal("formats alias mutable state")
	}
	if _, found := m.RawEvent(1); found {
		t.Fatal("GORM raw survived eviction")
	}
}
