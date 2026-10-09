// Package web serves the read-only monitor UI and API over local HTTP. It
// depends on the monitor package for data and owns log-token signing and the
// bounded import store. It has no configuration or process-management duties.
package web

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"local/sql-monitor/internal/monitor"
)

//go:embed index.html
var IndexHTML []byte

//go:embed logs.html
var LogsHTML []byte

//go:embed live.html
var LiveHTML []byte

var liveReadSlots = make(chan struct{}, 2)

var rawRoute = regexp.MustCompile(`^/api/event/([0-9]{1,20})\?raw=1$`)

const contentSecurityPolicy = "default-src 'none'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"

type Handler struct {
	monitor *monitor.Monitor
	logs    *logAccess
	port    int
}

func NewHandler(m *monitor.Monitor, port int) *Handler {
	return &Handler{monitor: m, logs: &logAccess{}, port: port}
}

// Close releases the import store's temporary directory and stops its sweeper.
func (h *Handler) Close() {
	h.logs.imports.close()
}

func (h Handler) trusted(r *http.Request) bool {
	host := strings.ToLower(r.Host)
	port := strconv.Itoa(h.port)
	if host != "127.0.0.1:"+port && host != "localhost:"+port && !(h.port == 80 && (host == "127.0.0.1" || host == "localhost")) {
		return false
	}
	if len(r.Header.Values("Host")) != 0 {
		return false
	}
	origins := r.Header.Values("Origin")
	if len(origins) > 0 && (len(origins) != 1 || strings.ToLower(origins[0]) != "http://"+host) {
		return false
	}
	for _, site := range r.Header.Values("Sec-Fetch-Site") {
		if strings.EqualFold(site, "cross-site") {
			return false
		}
	}
	return true
}

func sendData(w http.ResponseWriter, r *http.Request, code int, data []byte, contentType string) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", contentSecurityPolicy)
	w.Header().Set("Connection", "close")
	r.Close = true
	w.WriteHeader(code)
	if r.Method != http.MethodHead {
		_, _ = w.Write(data)
	}
}

func sendJSON(w http.ResponseWriter, r *http.Request, code int, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		code, data = http.StatusInternalServerError, []byte(`{"error":"Cannot encode response"}`)
	}
	sendData(w, r, code, data, "application/json; charset=utf-8")
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.trusted(r) {
		sendJSON(w, r, http.StatusForbidden, map[string]string{"error": "Untrusted host or origin"})
		return
	}
	if importEndpoint(r.RequestURI) {
		h.serveImport(w, r)
		return
	}
	if r.Method != http.MethodGet {
		sendJSON(w, r, http.StatusMethodNotAllowed, map[string]string{"error": "Read-only server; use GET"})
		return
	}
	switch r.RequestURI {
	case "/", "/overview", "/requests", "/live":
		sendData(w, r, http.StatusOK, LiveHTML, "text/html; charset=utf-8")
	case "/sql":
		sendData(w, r, http.StatusOK, IndexHTML, "text/html; charset=utf-8")
	case "/api/live?raw=1":
		select {
		case liveReadSlots <- struct{}{}:
			defer func() { <-liveReadSlots }()
			sendJSON(w, r, http.StatusOK, h.monitor.LiveSnapshot())
		default:
			sendJSON(w, r, http.StatusTooManyRequests, map[string]string{"error": "Too many live snapshot readers"})
		}
	case "/api/status":
		sendJSON(w, r, http.StatusOK, h.monitor.StatusSnapshot())
	case "/api/state":
		sendJSON(w, r, http.StatusOK, h.monitor.Snapshot())
	case "/logs":
		sendData(w, r, http.StatusOK, LogsHTML, "text/html; charset=utf-8")
	default:
		if strings.HasPrefix(r.RequestURI, "/api/logs/") && r.URL.EscapedPath() == r.URL.Path {
			h.serveLogs(w, r)
			return
		}
		match := rawRoute.FindStringSubmatch(r.RequestURI)
		if match == nil {
			sendJSON(w, r, http.StatusNotFound, map[string]string{"error": "Not found"})
			return
		}
		id, err := strconv.ParseUint(match[1], 10, 64)
		if err == nil {
			if raw, found := h.monitor.RawEvent(id); found {
				sendJSON(w, r, http.StatusOK, raw)
				return
			}
		}
		sendJSON(w, r, http.StatusNotFound, map[string]string{"error": "Event not found"})
	}
}
