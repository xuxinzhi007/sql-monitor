package web

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"local/sql-monitor/internal/monitor"
)

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
	m := testMonitor(t, filepath.Join(t.TempDir(), "missing.log"), 1, 0)
	h := *NewHandler(m, 8767)
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
	var raw monitor.RawEvent
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
	before, err := os.ReadFile(m.Snapshot().Status.Path)
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
	after, err := os.ReadFile(m.Snapshot().Status.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("HTTP request modified log")
	}
	w := request(h, "GET", "/")
	if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), LiveHTML) || !strings.Contains(w.Header().Get("Content-Type"), "text/html") {
		t.Fatal("embedded UI not served")
	}
	if len(IndexHTML) == 0 {
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
