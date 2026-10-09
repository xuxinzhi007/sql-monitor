package web

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLiveHTTPPrivacyAndPages(t *testing.T) {
	m, h := httpFixture(t)
	m.Consume([]byte("{\"level\":\"WARN\",\"msg\":\"PRIVATE_LIVE_MESSAGE\"}\n"))
	for _, path := range []string{"/", "/overview", "/requests", "/live"} {
		r := request(h, "GET", path)
		if r.Code != 200 || !bytes.Equal(r.Body.Bytes(), LiveHTML) {
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
	if r := request(h, "GET", "/sql"); r.Code != 200 || !bytes.Equal(r.Body.Bytes(), IndexHTML) {
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
