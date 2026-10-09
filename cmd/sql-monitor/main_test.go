package main

import (
	"bufio"
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
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"local/sql-monitor/internal/web"
)

func TestCLIProcess(t *testing.T) {
	if os.Getenv("SQL_MONITOR_CLI_TEST") != "1" {
		return
	}
	var args []string
	if err := json.Unmarshal([]byte(os.Getenv("SQL_MONITOR_CLI_ARGS")), &args); err != nil {
		panic(err)
	}
	os.Args = append([]string{"sql-monitor"}, args...)
	main()
}

func TestCLISignalsStartupAndEmbeddedUIFromOtherDirectory(t *testing.T) {
	for _, signal := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
		t.Run(signal.String(), func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "synthetic.log")
			writeLog(t, path, record(t, "SELECT 1", nil))
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
			args, _ := json.Marshal([]string{"--log", path, "--port", strconv.Itoa(port), "--max-events", "12"})
			cmd.Env = append(os.Environ(), "SQL_MONITOR_CLI_TEST=1", "SQL_MONITOR_CLI_ARGS="+string(args), "TMPDIR="+directory)
			cmd.Dir = directory
			pipe, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
			scanner := bufio.NewScanner(pipe)
			var lines []string
			for range 3 {
				if !scanner.Scan() {
					t.Fatal("startup output missing")
				}
				lines = append(lines, scanner.Text())
			}
			url := fmt.Sprintf("http://127.0.0.1:%d/", port)
			if !strings.Contains(lines[0], url) || !strings.Contains(lines[1], path) || !strings.Contains(lines[2], "12 events") {
				t.Fatalf("startup details missing: %v", lines)
			}
			client := &http.Client{Timeout: time.Second}
			response, err := client.Get(url)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil || !bytes.Equal(body, web.LiveHTML) {
				t.Fatal("UI requires working-directory assets")
			}
			// Exercise import writes over real HTTP, then verify signal cleanup
			// removes only owned copies, including incomplete uploads.
			for _, size := range []int{0, 10} {
				r, err := http.NewRequest("POST", url+"api/logs/imports", strings.NewReader(fmt.Sprintf(`{"name":"arbitrary.txt","size":%d}`, size)))
				if err != nil {
					t.Fatal(err)
				}
				r.Header.Set("Origin", strings.TrimSuffix(url, "/"))
				r.Header.Set("Content-Type", "application/json")
				response, err := client.Do(r)
				if err != nil {
					t.Fatal(err)
				}
				var item web.ImportReply
				err = json.NewDecoder(response.Body).Decode(&item)
				response.Body.Close()
				if err != nil || response.StatusCode != 201 {
					t.Fatalf("wire import: %v %d", err, response.StatusCode)
				}
				if size == 0 {
					r, err = http.NewRequest("POST", url+"api/logs/imports/finalize", strings.NewReader("{}"))
					if err != nil {
						t.Fatal(err)
					}
					r.Header.Set("Origin", strings.TrimSuffix(url, "/"))
					r.Header.Set("Content-Type", "application/json")
					r.Header.Set("X-Log-Import", item.ID)
					response, err = client.Do(r)
					if err != nil {
						t.Fatal(err)
					}
					response.Body.Close()
					if response.StatusCode != 200 {
						t.Fatal("wire finalize", response.StatusCode)
					}
				}
			}
			copies, err := filepath.Glob(filepath.Join(directory, "sql-monitor-imports-*"))
			if err != nil || len(copies) != 1 {
				t.Fatal("missing private import directory", err)
			}
			if err := cmd.Process.Signal(signal); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Wait(); err != nil {
				t.Fatalf("signal shutdown failed: %v %s", err, stderr.String())
			}
			if _, err := os.Stat(copies[0]); !os.IsNotExist(err) {
				t.Fatal("signal left imported copies")
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatal("signal removed original SQL source")
			}
		})
	}
}
