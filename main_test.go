package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func httpFixture(t *testing.T) (*Monitor, monitorHandler) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "service.log")
	writeLog(t, path, record(t, "SELECT 'raw-only-secret'", map[string]any{"msg": "SQL Error", "error": "raw-only-error"}))
	m := testMonitor(t, options{logPath: path, maxEvents: 1, initialBytes: 2 * 1024 * 1024})
	m.Poll()
	return m, monitorHandler{monitor: m, port: 8767}
}

func request(h monitorHandler, method, path string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, nil)
	r.Host = "127.0.0.1:" + strconv.Itoa(h.port)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestHTTPStateContractAndHeaders(t *testing.T) {
	_, h := httpFixture(t)
	w := request(h, "GET", "/api/state")
	if w.Code != 200 {
		t.Fatalf("state status: %d", w.Code)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 || decoded["status"] == nil || decoded["events"] == nil {
		t.Fatal("state keys changed")
	}
	var status map[string]any
	var events []map[string]any
	if err := json.Unmarshal(decoded["status"], &status); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(decoded["events"], &events); err != nil {
		t.Fatal(err)
	}
	statusKeys := []string{"path", "exists", "bytes_read", "lines_seen", "ignored_lines", "parse_errors", "rotations", "dropped_events", "oversize_lines", "started_at", "last_read", "capacity", "error", "source_mode", "input_state", "formats", "sql_observed", "with_duration", "with_source", "with_trace", "incomplete_records"}
	eventKeys := []string{"id", "time", "received_at", "level", "kind", "sql", "fingerprint", "elapsed_ms", "rows", "source", "trace_id", "span_id", "error", "truncated"}
	if len(status) != len(statusKeys) || len(events) != 1 || len(events[0]) != len(eventKeys) {
		t.Fatal("API field count changed")
	}
	for _, key := range statusKeys {
		if _, found := status[key]; !found {
			t.Errorf("missing status key %s", key)
		}
	}
	for _, key := range eventKeys {
		if _, found := events[0][key]; !found {
			t.Errorf("missing event key %s", key)
		}
	}
	if _, ok := events[0]["id"].(float64); !ok {
		t.Fatal("JSON id is not numeric")
	}
	if strings.Contains(w.Body.String(), "raw-only-secret") || strings.Contains(w.Body.String(), "raw-only-error") {
		t.Fatal("raw payload leaked through state")
	}
	for name, expected := range map[string]string{"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer", "Connection": "close", "Content-Security-Policy": contentSecurityPolicy} {
		if w.Header().Get(name) != expected {
			t.Errorf("bad %s header: %q", name, w.Header().Get(name))
		}
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("CORS enabled")
	}
	if w.Header().Get("Content-Length") != strconv.Itoa(w.Body.Len()) {
		t.Fatal("incorrect content length")
	}
}

func TestEmptyEventsAndNullableMetrics(t *testing.T) {
	m := testMonitor(t, options{logPath: filepath.Join(t.TempDir(), "missing.log"), maxEvents: 1})
	h := monitorHandler{monitor: m, port: 8767}
	body := request(h, "GET", "/api/state").Body.String()
	if !strings.Contains(body, `"events":[]`) || !strings.Contains(body, `"last_read":null`) {
		t.Fatalf("empty state contract: %s", body)
	}
	m.Consume(record(t, "SELECT 1", map[string]any{"elapsed": nil, "rows": nil}))
	body = request(h, "GET", "/api/state").Body.String()
	if !strings.Contains(body, `"elapsed_ms":null`) || !strings.Contains(body, `"rows":null`) {
		t.Fatalf("nullable metrics changed: %s", body)
	}
}

func TestHTTPRawOptInEvictionAndRoutes(t *testing.T) {
	m, h := httpFixture(t)
	for _, path := range []string{"/api/event/1", "/api/event/1?raw=0", "/api/event/1?raw=1&x=2", "/api/event/%31?raw=1", "/api/event/-1?raw=1", "/api/event/999999999999999999999?raw=1", "/api/event/99999999999999999999?raw=1", "/api/state?raw=1", "/api/state?log=/etc/passwd", "/monitor.py", "/index.html", "/../service.log", "/%2e%2e/service.log", "/?x=1", "http://127.0.0.1:8767/api/state"} {
		w := request(h, "GET", path)
		if w.Code != 404 || !strings.Contains(w.Header().Get("Content-Type"), "application/json") {
			t.Errorf("unexpected route response for %s: %d", path, w.Code)
		}
	}
	w := request(h, "GET", "/api/event/1?raw=1")
	var raw RawEvent
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || raw.SQL != "SELECT 'raw-only-secret'" || raw.Error != "raw-only-error" {
		t.Fatalf("raw response mismatch: %d %+v", w.Code, raw)
	}
	if request(h, "GET", "/api/event/0001?raw=1").Code != 200 {
		t.Fatal("numeric id parsing mismatch")
	}
	m.Consume(record(t, "SELECT 2", nil))
	w = request(h, "GET", "/api/event/1?raw=1")
	if w.Code != 404 || w.Body.String() != `{"error":"Event not found"}` {
		t.Fatal("evicted raw still accessible")
	}
}

func TestHTTPHostOriginAndFetchGuards(t *testing.T) {
	_, h := httpFixture(t)
	host := "127.0.0.1:8767"
	cases := []struct {
		host    string
		headers http.Header
	}{
		{"", nil}, {"evil.example", nil}, {"localhost:9", nil}, {"127.0.0.1.evil:8767", nil},
		{host, http.Header{"Origin": {"http://evil.example"}}},
		{host, http.Header{"Origin": {"null"}}},
		{host, http.Header{"Origin": {"https://" + host}}},
		{host, http.Header{"Origin": {"http://localhost:8767"}}},
		{host, http.Header{"Origin": {"http://" + host, "http://" + host}}},
		{host, http.Header{"Host": {host}}},
		{host, http.Header{"Sec-Fetch-Site": {"cross-site"}}},
		{host, http.Header{"Sec-Fetch-Site": {"same-origin", "cross-site"}}},
	}
	for _, tc := range cases {
		for _, path := range []string{"/api/state", "/api/event/1?raw=1"} {
			for _, method := range []string{"GET", "POST"} {
				r := httptest.NewRequest(method, path, nil)
				r.Host = tc.host
				if tc.headers != nil {
					r.Header = tc.headers.Clone()
				}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				if w.Code != 403 {
					t.Errorf("untrusted request accepted: host=%q headers=%v code=%d", tc.host, tc.headers, w.Code)
				}
			}
		}
	}
	for _, host := range []string{"127.0.0.1:8767", "localhost:8767", "LOCALHOST:8767"} {
		r := httptest.NewRequest("GET", "/api/state", nil)
		r.Host = host
		r.Header.Set("Origin", "http://"+host)
		r.Header.Set("Sec-Fetch-Site", "same-origin")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Errorf("trusted request rejected: %s", host)
		}
	}
	h.port = 80
	for _, host := range []string{"localhost", "127.0.0.1", "localhost:80", "127.0.0.1:80"} {
		r := httptest.NewRequest("GET", "/api/state", nil)
		r.Host = host
		if !h.trusted(r) {
			t.Errorf("default-port host rejected: %s", host)
		}
	}
}

func TestHTTPReadOnlyAndEmbeddedUI(t *testing.T) {
	m, h := httpFixture(t)
	before, err := os.ReadFile(m.status.Path)
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD", "TRACE", "CONNECT", "CUSTOM"} {
		w := request(h, method, "/api/state")
		if w.Code != 405 {
			t.Errorf("write method %s returned %d", method, w.Code)
		}
		if method == "HEAD" && w.Body.Len() != 0 {
			t.Fatal("HEAD response has body")
		}
	}
	after, err := os.ReadFile(m.status.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("HTTP request modified log")
	}
	w := request(h, "GET", "/")
	if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), liveHTML) || !strings.Contains(w.Header().Get("Content-Type"), "text/html") {
		t.Fatal("embedded UI not served")
	}
	if len(indexHTML) == 0 {
		t.Fatal("UI embed is empty")
	}
}

func TestHTTPWireDenialAndConcurrentReads(t *testing.T) {
	m, h := httpFixture(t)
	server := httptest.NewUnstartedServer(nil)
	h.port = server.Listener.Addr().(*net.TCPAddr).Port
	if server.Listener.Addr().(*net.TCPAddr).IP.String() != "127.0.0.1" {
		t.Fatal("test server not bound to loopback")
	}
	server.Config.Handler = h
	server.Start()
	defer server.Close()
	host := server.Listener.Addr().String()
	for _, headers := range []string{"", "Host: " + host + "\r\nHost: " + host + "\r\n", "Host: " + host + "\r\nOrigin: http://" + host + "\r\nOrigin: http://" + host + "\r\n"} {
		conn, err := net.DialTimeout("tcp4", host, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		_, err = fmt.Fprintf(conn, "GET /api/event/1?raw=1 HTTP/1.1\r\n%sConnection: close\r\n\r\n", headers)
		if err != nil {
			conn.Close()
			t.Fatal(err)
		}
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			conn.Close()
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		conn.Close()
		if response.StatusCode != 400 && response.StatusCode != 403 {
			t.Fatalf("wire request not denied: %d", response.StatusCode)
		}
		if strings.Contains(string(body), "raw-only") {
			t.Fatal("rejection exposed raw")
		}
	}
	client := &http.Client{Timeout: 2 * time.Second}
	var group sync.WaitGroup
	for range 4 {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 10 {
				for _, path := range []string{"/api/state", "/api/event/1?raw=1"} {
					response, err := client.Get(server.URL + path)
					if err != nil {
						t.Error(err)
						return
					}
					_, _ = io.Copy(io.Discard, response.Body)
					response.Body.Close()
					if response.StatusCode != 200 {
						t.Errorf("concurrent response: %d", response.StatusCode)
					}
				}
			}
		}()
	}
	for range 10 {
		m.Poll()
	}
	group.Wait()
}

func TestCLIOptionsAndOccupiedPort(t *testing.T) {
	var output bytes.Buffer
	opts, err := parseOptions(nil, &output)
	if err != nil || !reflect.DeepEqual(opts, options{logPath: "/tmp/data-service-local.log", port: 8767, maxEvents: 2000, initialBytes: 2 * 1024 * 1024}) {
		t.Fatalf("bad defaults: %+v %v", opts, err)
	}
	_, err = parseOptions([]string{"--help"}, &output)
	if !errors.Is(err, flag.ErrHelp) || !strings.Contains(output.String(), "--initial-bytes") {
		t.Fatal("CLI help missing")
	}
	for _, args := range [][]string{{"--max-events", "2001"}, {"--max-events", "0"}, {"--port", "0"}, {"--port", "65536"}, {"--initial-bytes", "-1"}, {"--log", ""}, {"--port", "bad"}, {"--unknown"}, {"extra"}} {
		if code := run(context.Background(), args, io.Discard, io.Discard); code != 2 {
			t.Errorf("invalid args %v returned %d", args, code)
		}
	}
	var helpOut, helpErr bytes.Buffer
	if code := run(context.Background(), []string{"--help"}, &helpOut, &helpErr); code != 0 || !strings.Contains(helpOut.String(), "--initial-bytes") || helpErr.Len() != 0 {
		t.Fatal("help exit status or output stream")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	path := filepath.Join(t.TempDir(), "not-created.log")
	port := listener.Addr().(*net.TCPAddr).Port
	output.Reset()
	if code := run(context.Background(), []string{"--log", path, "--port", strconv.Itoa(port)}, io.Discard, &output); code != 1 {
		t.Fatalf("occupied port returned %d", code)
	}
	for _, text := range []string{"Cannot bind", "existing UI", "--port", strconv.Itoa(port)} {
		if !strings.Contains(output.String(), text) {
			t.Errorf("occupied-port message missing %q", text)
		}
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("occupied port touched logfile")
	}
}

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
			if err != nil || !bytes.Equal(body, liveHTML) {
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
				var item importReply
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
