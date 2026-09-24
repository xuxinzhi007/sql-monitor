package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestLiveCaptureDoesNotChangeSQL(t *testing.T) {
	m, err := newMonitor(filepath.Join(t.TempDir(), "unused.log"), 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	lines := "{\"level\":\"DEBUG\",\"msg\":\"Lock already held by another instance\",\"caller\":\"lock.go:58\"}\n" +
		"{\"level\":\"WARN\",\"msg\":\"invalid thing mqtt reply\",\"caller\":\"thing.go:260\"}\n" +
		"{\"msg\":\"SQL Query\",\"sql\":\"SELECT 1\",\"elapsed\":\"1ms\"}\n" +
		"unstructured message\n"
	for _, b := range []byte(lines) {
		m.Consume([]byte{b})
	}
	sql := m.Snapshot()
	if len(sql.Events) != 1 || sql.Status.SQLObserved != 1 {
		t.Fatal("SQL capture changed")
	}
	body, err := json.Marshal(m.live.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	var live struct {
		Events []struct {
			Message string `json:"message"`
		}
		Stats struct {
			Observed int `json:"observed"`
		}
		Anomalies []json.RawMessage
	}
	if err := json.Unmarshal(body, &live); err != nil {
		t.Fatal(err)
	}
	if len(live.Events) != 4 || live.Stats.Observed != 4 || len(live.Anomalies) != 1 {
		t.Fatalf("incorrect live capture: %s", body)
	}
}

func TestLiveOversizeAndPermanentEOF(t *testing.T) {
	m, err := newMonitor(filepath.Join(t.TempDir(), "unused.log"), 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.Consume([]byte(strings.Repeat("x", maxLine+100)))
	m.Consume([]byte("\nlast line without newline"))
	m.finishInput(io.EOF)
	data, _ := json.Marshal(m.live.snapshot())
	var got struct {
		Events []struct {
			Raw       string
			Truncated bool
		}
		Stats struct{ Observed int }
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Stats.Observed != 2 || len(got.Events) != 2 || !got.Events[0].Truncated || len(got.Events[0].Raw) > 16<<10 || got.Events[1].Raw != "last line without newline" {
		t.Fatal("oversize/EOF capture incorrect")
	}
	if m.Snapshot().Status.OversizeLines != 1 {
		t.Fatal("SQL oversize accounting changed")
	}
}

func TestLiveHTTPPrivacyAndPages(t *testing.T) {
	m, h := httpFixture(t)
	m.Consume([]byte("{\"level\":\"WARN\",\"msg\":\"PRIVATE_LIVE_MESSAGE\"}\n"))
	for _, path := range []string{"/", "/overview", "/requests", "/live"} {
		r := request(h, "GET", path)
		if r.Code != 200 || !bytes.Equal(r.Body.Bytes(), liveHTML) {
			t.Fatalf("missing page %s", path)
		}
		if strings.Contains(r.Body.String(), "PRIVATE_LIVE_MESSAGE") {
			t.Fatal("raw content embedded in HTML")
		}
	}
	for _, path := range []string{"/api/live", "/api/live?raw=0", "/api/live?raw=1&other=1", "/api/live?raw=1&raw=1", "/api/live?raw=%31", "/api/%6cive?raw=1"} {
		r := request(h, "GET", path)
		if r.Code == 200 || strings.Contains(r.Body.String(), "PRIVATE_LIVE_MESSAGE") {
			t.Fatalf("unguarded live API: %s", path)
		}
	}
	if r := request(h, "GET", "/api/state"); strings.Contains(r.Body.String(), "PRIVATE_LIVE_MESSAGE") {
		t.Fatal("existing SQL API leaked general logs")
	}
	r := request(h, "GET", "/api/live?raw=1")
	if r.Code != 200 || !strings.Contains(r.Body.String(), "PRIVATE_LIVE_MESSAGE") || r.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("explicit live API missing")
	}
	if r := request(h, "GET", "/sql"); r.Code != 200 || !bytes.Equal(r.Body.Bytes(), indexHTML) {
		t.Fatal("SQL page regression")
	}
	req := httptest.NewRequest("GET", "/api/live?raw=1", nil)
	req.Host = "127.0.0.1:8767"
	req.Header.Set("Origin", "https://untrusted.example")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, req)
	if response.Code != http.StatusForbidden {
		t.Fatal("untrusted origin allowed")
	}
	if r := request(h, "POST", "/api/live?raw=1"); r.Code != 405 {
		t.Fatal("live writes allowed")
	}
}
