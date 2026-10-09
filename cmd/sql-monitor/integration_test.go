package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"local/sql-monitor/internal/monitor"
)

func TestCLIStdinIdleSignalAndEOF(t *testing.T) {
	for _, eof := range []bool{false, true} {
		name := "idle"
		if eof {
			name = "eof"
		}
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			port := listener.Addr().(*net.TCPAddr).Port
			listener.Close()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestCLIProcess$")
			args, _ := json.Marshal([]string{"--stdin", "--port", strconv.Itoa(port)})
			cmd.Env = append(os.Environ(), "SQL_MONITOR_CLI_TEST=1", "SQL_MONITOR_CLI_ARGS="+string(args))
			input, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			diagnostic, err := cmd.StderrPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stdout bytes.Buffer
			cmd.Stdout = &stdout
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
			scanner := bufio.NewScanner(diagnostic)
			for range 3 {
				if !scanner.Scan() {
					t.Fatal("stdin startup diagnostics absent")
				}
			}
			if eof {
				if _, err := io.WriteString(input, `{"msg":"SQL Query","sql":"SELECT 1"}`); err != nil {
					t.Fatal(err)
				}
				input.Close()
			}
			client := &http.Client{Timeout: time.Second}
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
		wait:
			for {
				select {
				case <-ctx.Done():
					t.Fatal("stdin status unavailable")
				case <-ticker.C:
					response, err := client.Get("http://127.0.0.1:" + strconv.Itoa(port) + "/api/state")
					if err != nil {
						continue
					}
					var snapshot monitor.Snapshot
					err = json.NewDecoder(response.Body).Decode(&snapshot)
					response.Body.Close()
					if err != nil {
						t.Fatal(err)
					}
					if snapshot.Status.InputState == "error" {
						t.Fatalf("stdin failed: %+v", snapshot.Status)
					}
					if snapshot.Status.SourceMode == "stdin" && (!eof || snapshot.Status.InputState == "eof" && len(snapshot.Events) == 1) {
						break wait
					}
				}
			}
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Wait(); err != nil {
				t.Fatalf("stdin signal shutdown: %v", err)
			}
			if stdout.Len() != 0 {
				t.Fatal("stdin mode wrote to stdout")
			}
		})
	}
}
