package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestInputFinalLineAndErrors(t *testing.T) {
	for _, tc := range []struct {
		text            string
		events, invalid int64
	}{
		{`{"msg":"SQL Query","sql":"SELECT 'secret'"}`, 1, 0},
		{nativeHeader + "\n[0.010ms] [rows:-] SELECT 1", 1, 0},
		{`{"msg":"SQL Query",`, 0, 1},
		{"", 0, 0},
	} {
		path := filepath.Join(t.TempDir(), "input")
		writeLog(t, path, []byte(tc.text))
		input, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		m := testMonitor(t, testOptions{logPath: path, maxEvents: 2})
		m.RunInput(context.Background(), input)
		snapshot := m.Snapshot()
		if snapshot.Status.InputState != "eof" || snapshot.Status.SourceMode != "stdin" || snapshot.Status.BytesRead != int64(len(tc.text)) || snapshot.Status.SQLObserved != tc.events || snapshot.Status.ParseErrors != tc.invalid || m.closed {
			t.Fatalf("EOF: %+v", snapshot.Status)
		}
		if _, err := input.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
			t.Fatal("input ownership not released")
		}
	}
	m := testMonitor(t, testOptions{logPath: filepath.Join(t.TempDir(), "unused"), maxEvents: 2})
	m.Consume([]byte(`{"msg":"SQL Query","sql":"SELECT 1"}`))
	m.finishInput(errors.New("sensitive input error"))
	if len(m.Snapshot().Events) != 0 || m.Snapshot().Status.InputState != "error" || m.Snapshot().Status.IncompleteRecords != 1 || strings.Contains(m.Snapshot().Status.Error, "sensitive") {
		t.Fatal("error flushed partial input or leaked cause")
	}
}

func TestInputIdleCancellationAndConcurrentSnapshots(t *testing.T) {
	input, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	m := testMonitor(t, testOptions{logPath: filepath.Join(t.TempDir(), "unused"), maxEvents: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); m.RunInput(ctx, input) }()
	if _, err := writer.Write(record(t, "SELECT 1", nil)); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
wait:
	for {
		select {
		case <-deadline.C:
			t.Fatal("stdin read did not start")
		case <-ticker.C:
			snapshot := m.Snapshot()
			if len(snapshot.Events) == 1 {
				break wait
			}
		}
	}
	for range 100 {
		m.Snapshot()
		m.Poll()
		m.RawEvent(1)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idle stdin shutdown hung")
	}
	if _, err := input.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatal("idle reader handle leaked")
	}
}
