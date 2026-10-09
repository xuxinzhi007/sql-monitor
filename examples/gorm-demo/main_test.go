package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type state struct {
	Status struct {
		SourceMode        string `json:"source_mode"`
		InputState        string `json:"input_state"`
		BytesRead         int    `json:"bytes_read"`
		SQLObserved       int    `json:"sql_observed"`
		WithDuration      int    `json:"with_duration"`
		WithSource        int    `json:"with_source"`
		WithTrace         int    `json:"with_trace"`
		ParseErrors       int    `json:"parse_errors"`
		IncompleteRecords int    `json:"incomplete_records"`
		Formats           struct {
			GORM int `json:"gorm"`
		} `json:"formats"`
	} `json:"status"`
	Events []struct {
		ID          int      `json:"id"`
		Kind        string   `json:"kind"`
		SQL         string   `json:"sql"`
		Source      string   `json:"source"`
		Fingerprint string   `json:"fingerprint"`
		Error       string   `json:"error"`
		Elapsed     *float64 `json:"elapsed_ms"`
	} `json:"events"`
}

func TestRealSQLThroughMonitor(t *testing.T) {
	var logs bytes.Buffer
	if err := runDemo(&logs); err != nil {
		t.Fatal(err)
	}
	if strings.Count(logs.String(), "[rows:") != 11 || !strings.Contains(logs.String(), "SLOW SQL >= 1ns") || !strings.Contains(logs.String(), "no such table: missing_demo_table") || strings.Contains(logs.String(), "\x1b") {
		t.Fatal("unexpected real GORM output")
	}
	directory := t.TempDir()
	binary := filepath.Join(directory, "sql-monitor")
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-race", "-o", binary, "./cmd/sql-monitor")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build monitor: %v\n%s", err, output)
	}
	for _, mode := range []string{"stdin", "file"} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			port := listener.Addr().(*net.TCPAddr).Port
			listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			args := []string{"--port", fmt.Sprint(port)}
			input := logs.Bytes()
			if mode == "file" {
				path := filepath.Join(directory, "gorm.log")
				if err := os.WriteFile(path, input, 0600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "--log", path)
			} else {
				input = bytes.TrimSuffix(input, []byte{'\n'})
				args = append(args, "--stdin")
			}
			cmd := exec.CommandContext(ctx, binary, args...)
			cmd.Dir = directory
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if mode == "stdin" {
				cmd.Stdin = bytes.NewReader(input)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
			client := &http.Client{Timeout: time.Second}
			url := fmt.Sprintf("http://127.0.0.1:%d", port)
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			var snapshot state
		wait:
			for {
				select {
				case <-ctx.Done():
					t.Fatal("monitor did not acquire real logs")
				case <-ticker.C:
					response, err := client.Get(url + "/api/state")
					if err != nil {
						continue
					}
					body, err := io.ReadAll(response.Body)
					response.Body.Close()
					if err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(body, &snapshot); err != nil {
						t.Fatal(err)
					}
					if strings.Contains(string(body), "demo-value-") || strings.Contains(string(body), "no such table:") {
						t.Fatal("raw SQL or error leaked through state")
					}
					if snapshot.Status.SQLObserved == 11 && (mode == "file" || snapshot.Status.InputState == "eof") {
						break wait
					}
				}
			}
			status := snapshot.Status
			if status.SourceMode != mode || status.BytesRead != len(input) || status.Formats.GORM != 11 || status.WithDuration != 11 || status.WithSource != 11 || status.WithTrace != 0 || status.ParseErrors != 0 || status.IncompleteRecords != 0 || len(snapshot.Events) != 11 {
				t.Fatalf("capability mismatch: %+v", status)
			}
			counts := map[string]int{}
			groups := map[string]int{}
			rawInsert, rawError := false, false
			for _, event := range snapshot.Events {
				counts[event.Kind]++
				if event.Elapsed == nil || !strings.Contains(event.Source, "main.go:") {
					t.Fatalf("missing real metrics: %+v", event)
				}
				if strings.HasPrefix(event.SQL, "SELECT id, label FROM demo_items WHERE id = ?") {
					groups[event.Fingerprint+event.Source]++
				}
				response, err := client.Get(fmt.Sprintf("%s/api/event/%d?raw=1", url, event.ID))
				if err != nil {
					t.Fatal(err)
				}
				var raw struct {
					SQL   string `json:"sql"`
					Error string `json:"error"`
				}
				err = json.NewDecoder(response.Body).Decode(&raw)
				response.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				rawInsert = rawInsert || strings.Contains(raw.SQL, "demo-value-")
				rawError = rawError || strings.Contains(raw.Error, "no such table: missing_demo_table")
			}
			if counts["slow"] != 10 || counts["error"] != 1 || !rawInsert || !rawError || len(groups) != 1 {
				t.Fatalf("diagnostics mismatch: kinds=%v repeated=%v", counts, groups)
			}
			for _, count := range groups {
				if count != 6 {
					t.Fatalf("repeat count %d", count)
				}
			}
			// EOF must not terminate the HTTP server.
			response, err := client.Get(url + "/")
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatal("UI unavailable after acquisition")
			}
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Wait(); err != nil {
				t.Fatalf("shutdown: %v %s", err, stderr.String())
			}
			if mode == "stdin" && (stdout.Len() != 0 || !strings.Contains(stderr.String(), "SQL Monitor:")) {
				t.Fatal("stdin diagnostics must go only to stderr")
			}
		})
	}
}
