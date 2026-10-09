package app

import (
	"os"
	"path/filepath"
	"testing"
)

func writeLog(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

// writeFakeKubectl writes an executable shell script and returns its path.
func writeFakeKubectl(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kubectl")
	script := "#!/bin/sh\n" + body + "\n"
	if err := os.WriteFile(path, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}
