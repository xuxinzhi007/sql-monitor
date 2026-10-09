package main

import (
	"encoding/json"
	"os"
	"testing"
)

// record builds a JSON log line for a SQL event, mirroring the monitor
// package's own helper so the command's tests can drive a full runtime.
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
