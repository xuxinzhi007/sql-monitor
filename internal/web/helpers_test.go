package web

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"local/sql-monitor/internal/monitor"
)

// record builds a JSON log line for a SQL event. It mirrors the monitor
// package's own helper so web-package tests can drive a full Monitor through
// the parser.
func record(t *testing.T, sql string, fields map[string]any) []byte {
	t.Helper()
	value := map[string]any{"level": "DEBUG", "ts": "2026-09-23T10:00:00Z", "source": "internal/data/tag.go:549", "elapsed": "38.01ms", "rows": 1, "sql": sql, "msg": "SQL Query"}
	for key, field := range fields {
		value[key] = field
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func writeLog(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func testMonitor(t *testing.T, logPath string, maxEvents int, initialBytes int64) *monitor.Monitor {
	t.Helper()
	m, err := monitor.New(logPath, maxEvents, initialBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return m
}

func httpFixture(t *testing.T) (*monitor.Monitor, Handler) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "service.log")
	writeLog(t, path, record(t, "SELECT 'raw-only-secret'", map[string]any{"msg": "SQL Error", "error": "raw-only-error"}))
	m := testMonitor(t, path, 1, 2*1024*1024)
	m.Poll()
	h := NewHandler(m, 8767)
	t.Cleanup(h.Close)
	return m, *h
}

func request(h Handler, method, path string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, nil)
	r.Host = "127.0.0.1:" + strconv.Itoa(h.port)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
