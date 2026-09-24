package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

func importFixture(t testing.TB) monitorHandler {
	t.Helper()
	m, err := newMonitor(filepath.Join(t.TempDir(), "missing-sql.log"), 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return monitorHandler{monitor: m, port: 8767}
}
func importRequest(h monitorHandler, method, endpoint, id, offset, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/api/logs/imports"+endpoint, strings.NewReader(body))
	r.Host = "127.0.0.1:" + strconv.Itoa(h.port)
	r.Header.Set("Origin", "http://"+r.Host)
	r.Header.Set("Content-Type", "application/json")
	if endpoint == "/chunk" {
		r.Header.Set("Content-Type", "application/octet-stream")
		r.Header.Set("X-Log-Offset", offset)
	}
	if id != "" {
		r.Header.Set("X-Log-Import", id)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func importDecode(t testing.TB, w *httptest.ResponseRecorder, code int) importReply {
	t.Helper()
	if w.Code != code {
		t.Fatalf("import: got %d want %d: %s", w.Code, code, w.Body.String())
	}
	var out importReply
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func createImport(t testing.TB, h monitorHandler, name string, size int64) importReply {
	t.Helper()
	data, err := json.Marshal(map[string]any{"name": name, "size": size})
	if err != nil {
		t.Fatal(err)
	}
	return importDecode(t, importRequest(h, "POST", "", "", "", string(data)), 201)
}
func uploadReader(t testing.TB, h monitorHandler, name string, size int64, reader io.Reader) importReply {
	t.Helper()
	item := createImport(t, h, name, size)
	buf := make([]byte, importChunkSize)
	for off := int64(0); off < size; {
		n, err := io.ReadFull(reader, buf[:min(int64(len(buf)), size-off)])
		if err != nil {
			t.Fatal(err)
		}
		item = importDecode(t, importRequest(h, "POST", "/chunk", item.ID, strconv.FormatInt(off, 10), string(buf[:n])), 200)
		off += int64(n)
	}
	return importDecode(t, importRequest(h, "POST", "/finalize", item.ID, "", "{}"), 200)
}
func uploadText(t testing.TB, h monitorHandler, name, text string) importReply {
	t.Helper()
	return uploadReader(t, h, name, int64(len(text)), strings.NewReader(text))
}
func onlyImport(h monitorHandler) importReply {
	s := &h.monitor.logs.imports
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.items) != 1 {
		panic("test requires exactly one import")
	}
	for _, item := range s.items {
		return item.meta
	}
	panic("unreachable")
}
func importPath(h monitorHandler, id string) string {
	s := &h.monitor.logs.imports
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.items[id].path
}
func importRead(h monitorHandler, id, kind, extra string) *httptest.ResponseRecorder {
	return request(h, "GET", "/api/logs/"+kind+"?raw=1&id="+id+extra)
}
func requireCode(t testing.TB, w *httptest.ResponseRecorder, code int) {
	t.Helper()
	if w.Code != code {
		t.Fatalf("got %d want %d: %s", w.Code, code, w.Body.String())
	}
}

func TestImportUploadFinalizeDeleteAndEmpty(t *testing.T) {
	for _, text := range []string{"", "中文\r\n{\"message\":\"needle\"}\nstack trace\n", strings.Repeat("arbitrary needle\n", 90000)} {
		h := importFixture(t)
		item := createImport(t, h, "strange.extension", int64(len(text)))
		requireCode(t, importRead(h, item.ID, "search", ""), 409)
		if text != "" {
			requireCode(t, importRequest(h, "POST", "/finalize", item.ID, "", "{}"), 409)
		}
		for off := 0; off < len(text); {
			end := min(off+importChunkSize, len(text))
			item = importDecode(t, importRequest(h, "POST", "/chunk", item.ID, strconv.Itoa(off), text[off:end]), 200)
			if item.Offset != int64(end) || item.Complete {
				t.Fatal("chunk state")
			}
			off = end
		}
		requireCode(t, importRead(h, item.ID, "tail", ""), 409)
		item = importDecode(t, importRequest(h, "POST", "/finalize", item.ID, "", "{}"), 200)
		if !item.Complete || item.Size != int64(len(text)) {
			t.Fatal("finalization state")
		}
		requireCode(t, importRequest(h, "POST", "/finalize", item.ID, "", "{}"), 200)
		requireCode(t, importRequest(h, "POST", "/chunk", item.ID, strconv.Itoa(len(text)), "x"), 409)
		requireCode(t, importRead(h, item.ID, "search", "&q=needle"), 200)
		tail := getLogs(t, h, "tail", nil)
		if text == "" && (len(tail.Lines) != 0 || tail.Size != 0) {
			t.Fatal("empty import")
		}
		path := importPath(h, item.ID)
		contents, err := os.ReadFile(path)
		if err != nil || string(contents) != text {
			t.Fatal("uploaded content differs", err)
		}
		requireCode(t, importRequest(h, "DELETE", "", item.ID, "", "{}"), 200)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("copy not removed")
		}
		requireCode(t, importRead(h, item.ID, "tail", ""), 410)
		requireCode(t, importRequest(h, "DELETE", "", item.ID, "", "{}"), 410)
	}
}
func TestImportLimitsNamesAndPermissions(t *testing.T) {
	h := importFixture(t)
	if h.monitor.logs.imports.dir != "" {
		t.Fatal("eager temp directory")
	}
	for _, body := range []string{`{}`, `null`, `[]`, `{"name":"x"}`, `{"name":"x","size":-1}`, `{"name":"x","size":1.5}`, `{"name":"x","size":0,"path":"/etc/passwd"}`, `{"name":"x","size":0} {}`, strings.Repeat(" ", 4097)} {
		w := importRequest(h, "POST", "", "", "", body)
		if w.Code != 400 && w.Code != 413 {
			t.Fatalf("invalid metadata: %d", w.Code)
		}
	}
	requireCode(t, importRequest(h, "POST", "", "", "", fmt.Sprintf(`{"name":"x","size":%d}`, importMaxSize+1)), 413)
	if h.monitor.logs.imports.dir != "" {
		t.Fatal("invalid metadata created directory")
	}
	a := createImport(t, h, "../../secret\\"+strings.Repeat("中", 100)+"\n\u202e.log", importMaxSize)
	if len(a.Name) > 180 || !utf8.ValidString(a.Name) || strings.ContainsAny(a.Name, "/\\\n\u202e") {
		t.Fatal("unsafe display name", a.Name)
	}
	path := importPath(h, a.ID)
	if filepath.Base(path) != a.ID || strings.Contains(path, a.Name) {
		t.Fatal("display name used as path")
	}
	for p, mode := range map[string]os.FileMode{path: 0600, filepath.Dir(path): 0700} {
		info, err := os.Stat(p)
		if err != nil || info.Mode().Perm() != mode {
			t.Fatalf("permissions %s: %v", p, err)
		}
	}
	b := createImport(t, h, "other", importMaxSize)
	requireCode(t, importRequest(h, "POST", "", "", "", `{"name":"over-total","size":1}`), 429)
	if a.ID == b.ID || len(a.ID) != 64 {
		t.Fatal("predictable/nonunique ID")
	}
	requireCode(t, importRequest(h, "DELETE", "", a.ID, "", "{}"), 200)
	requireCode(t, importRequest(h, "DELETE", "", b.ID, "", "{}"), 200)
	for range importMaxFiles {
		createImport(t, h, "empty", 0)
	}
	requireCode(t, importRequest(h, "POST", "", "", "", `{"name":"fifth","size":0}`), 429)
}
func TestImportChunkOffsetsBodiesAndConcurrentWrites(t *testing.T) {
	h := importFixture(t)
	a := createImport(t, h, "x", 4)
	for _, tc := range []struct {
		offset, body string
		code         int
	}{
		{"0", "", 400}, {"1", "x", 409}, {"-1", "x", 400}, {"+0", "x", 400}, {"00", "x", 400}, {"", "x", 400}, {"9223372036854775808", "x", 400},
		{"0", "12345", 400}, {"0", strings.Repeat("x", importChunkSize+1), 413},
	} {
		requireCode(t, importRequest(h, "POST", "/chunk", a.ID, tc.offset, tc.body), tc.code)
	}
	var wg sync.WaitGroup
	codes := make(chan int, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); codes <- importRequest(h, "POST", "/chunk", a.ID, "0", "ab").Code }()
	}
	wg.Wait()
	close(codes)
	counts := map[int]int{}
	for c := range codes {
		counts[c]++
	}
	if counts[200] != 1 || counts[409] != 1 {
		t.Fatal("concurrent offsets", counts)
	}
	requireCode(t, importRequest(h, "POST", "/chunk", a.ID, "0", "ab"), 409)
	requireCode(t, importRequest(h, "POST", "/chunk", a.ID, "2", "cd"), 200)
	requireCode(t, importRequest(h, "POST", "/finalize", a.ID, "", "{}"), 200)
	data, err := os.ReadFile(importPath(h, a.ID))
	if err != nil || string(data) != "abcd" {
		t.Fatal("concurrent corruption", err)
	}
	for range cap(importSlots) {
		importSlots <- struct{}{}
	}
	w := importRequest(h, "POST", "", "", "", `{"name":"x","size":0}`)
	for range cap(importSlots) {
		<-importSlots
	}
	requireCode(t, w, 429)
}

type brokenImportBody struct{ read bool }

func (b *brokenImportBody) Read(p []byte) (int, error) {
	if b.read {
		return 0, io.ErrUnexpectedEOF
	}
	b.read = true
	return copy(p, "partial"), nil
}
func TestImportInterruptedBodyAndCancellation(t *testing.T) {
	h := importFixture(t)
	a := createImport(t, h, "cancel", 10)
	for _, cancelled := range []bool{false, true} {
		var body io.Reader = &brokenImportBody{}
		if cancelled {
			body = strings.NewReader("hello")
		}
		r := httptest.NewRequest("POST", "/api/logs/imports/chunk", body)
		r.Host = "127.0.0.1:8767"
		r.Header.Set("Origin", "http://"+r.Host)
		r.Header.Set("Content-Type", "application/octet-stream")
		r.Header.Set("X-Log-Import", a.ID)
		r.Header.Set("X-Log-Offset", "0")
		if cancelled {
			ctx, cancel := context.WithCancel(r.Context())
			cancel()
			r = r.WithContext(ctx)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		want := 400
		if cancelled {
			want = 408
		}
		requireCode(t, w, want)
		if onlyImport(h).Offset != 0 {
			t.Fatal("failed body advanced offset")
		}
		info, err := os.Stat(importPath(h, a.ID))
		if err != nil || info.Size() != 0 {
			t.Fatal("partial body written")
		}
	}
	requireCode(t, importRequest(h, "POST", "/chunk", a.ID, "0", "hello"), 200)
	path := importPath(h, a.ID)
	requireCode(t, importRequest(h, "DELETE", "", a.ID, "", "{}"), 200)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("cancel left a copy")
	}
	requireCode(t, importRequest(h, "POST", "/chunk", a.ID, "5", "world"), 410)
}
func TestImportSecurityExactRoutesOriginTypeMethods(t *testing.T) {
	h := importFixture(t)
	a := createImport(t, h, "x", 1)
	for _, endpoint := range []string{"", "/chunk", "/finalize"} {
		methods := []string{"GET", "HEAD", "PUT", "PATCH", "OPTIONS", "TRACE"}
		if endpoint != "" {
			methods = append(methods, "DELETE")
		}
		for _, method := range methods {
			requireCode(t, importRequest(h, method, endpoint, a.ID, "0", "{}"), 405)
		}
		for _, tc := range []struct {
			key    string
			values []string
			code   int
		}{
			{"Origin", nil, 403}, {"Origin", []string{"null"}, 403}, {"Origin", []string{"http://localhost:8767"}, 403},
			{"Origin", []string{"http://evil.test"}, 403}, {"Origin", []string{"HTTP://127.0.0.1:8767"}, 403},
			{"Origin", []string{"http://127.0.0.1:8767", "http://127.0.0.1:8767"}, 403},
			{"Sec-Fetch-Site", []string{"cross-site"}, 403}, {"Host", []string{"127.0.0.1:8767"}, 403},
			{"Content-Type", nil, 415}, {"Content-Type", []string{"text/plain"}, 415}, {"Content-Type", []string{"application/x-www-form-urlencoded"}, 415},
			{"Content-Type", []string{"multipart/form-data; boundary=abc"}, 415}, {"Content-Type", []string{"application/json", "application/json"}, 415},
		} {
			for _, method := range []string{"POST", "DELETE"} {
				if method == "DELETE" && endpoint != "" {
					continue
				}
				r := httptest.NewRequest(method, "/api/logs/imports"+endpoint, strings.NewReader(`{"name":"x","size":0}`))
				r.Host = "127.0.0.1:8767"
				r.Header.Set("Origin", "http://"+r.Host)
				r.Header.Set("Content-Type", "application/json")
				if endpoint == "/chunk" {
					r.Header.Set("Content-Type", "application/octet-stream")
				}
				r.Header.Set("X-Log-Import", a.ID)
				r.Header.Set("X-Log-Offset", "0")
				r.Header[tc.key] = tc.values
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				requireCode(t, w, tc.code)
				if w.Header().Get("Access-Control-Allow-Origin") != "" {
					t.Fatal("CORS enabled")
				}
			}
		}
	}
	for _, uri := range []string{"/api/logs/imports?x=1", "/api/logs/imports/", "/api/logs/%69mports", "/api/logs/imports/chunk/", "/api/logs/imports/../imports", "http://127.0.0.1:8767/api/logs/imports", "/api/state", "/api/event/1?raw=1", "/logs"} {
		for _, method := range []string{"POST", "DELETE"} {
			requireCode(t, request(h, method, uri), 405)
		}
	}
	for _, endpoint := range []string{"", "/chunk", "/finalize"} {
		method := "POST"
		if endpoint == "" {
			method = "DELETE"
		}
		for _, id := range []string{"", strings.Repeat("a", 64), "../../etc/passwd"} {
			requireCode(t, importRequest(h, method, endpoint, id, "0", "{}"), 410)
		}
	}
	if len(h.monitor.logs.imports.items) != 1 {
		t.Fatal("rejected requests created imports")
	}
}
func TestImportSourceIsolationAndCursorBinding(t *testing.T) {
	m, h := httpFixture(t)
	sqlBefore, _ := os.ReadFile(m.status.Path)
	for _, mode := range []string{"file", "stdin"} {
		m.status.SourceMode = mode
		for _, kind := range []string{"tail", "search"} {
			requireCode(t, request(h, "GET", "/api/logs/"+kind+"?raw=1"), 410)
		}
	}
	a := uploadText(t, h, "a.log", "arbitrary\nneedle\n")
	b := uploadText(t, h, "b.jsonl", "different\n")
	w := importRead(h, a.ID, "search", "&limit=1")
	requireCode(t, w, 200)
	var first logReply
	if err := json.Unmarshal(w.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"search?raw=1&id=" + b.ID + "&cursor=" + first.Cursor, "tail?raw=1&id=" + b.ID + "&version=" + first.Version, "context?raw=1&id=" + b.ID + "&version=" + first.Version + "&offset=0"} {
		requireCode(t, request(h, "GET", "/api/logs/"+route), 409)
	}
	// Even a ticket with identical version metadata must be bound to its import ID.
	ticket, _ := m.logs.verify(first.Cursor)
	ticket.ImportID = b.ID
	forgedForOther, _ := m.logs.sign(ticket)
	requireCode(t, importRead(h, a.ID, "search", "&cursor="+forgedForOther), 409)
	for _, kind := range []string{"search", "tail"} {
		w := importRead(h, a.ID, kind, "")
		requireCode(t, w, 200)
		if strings.Contains(w.Body.String(), "raw-only") || !strings.Contains(w.Body.String(), "needle") {
			t.Fatal("SQL source leaked into import")
		}
	}
	for _, key := range []string{"path", "log", "source"} {
		requireCode(t, importRead(h, a.ID, "tail", "&"+key+"=/etc/passwd"), 400)
	}
	requireCode(t, importRequest(h, "DELETE", "", a.ID, "", "{}"), 200)
	requireCode(t, importRead(h, a.ID, "search", "&cursor="+first.Cursor), 410)
	after, _ := os.ReadFile(m.status.Path)
	if !bytes.Equal(sqlBefore, after) {
		t.Fatal("SQL original changed")
	}
	state := request(h, "GET", "/api/state").Body.String()
	if strings.Contains(state, "arbitrary") || strings.Contains(state, b.ID) {
		t.Fatal("import leaked into SQL state")
	}
}
func TestImportExpiryShutdownAndReadLease(t *testing.T) {
	h := importFixture(t)
	a := uploadText(t, h, "complete", "needle\n")
	b := createImport(t, h, "partial", 100)
	s := &h.monitor.logs.imports
	dir := s.dir
	item, release, err := s.acquire(a.ID)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(item.path)
	if err != nil {
		release()
		t.Fatal(err)
	}
	started := make(chan struct{})
	removed := make(chan *httptest.ResponseRecorder, 1)
	go func() { close(started); removed <- importRequest(h, "DELETE", "", a.ID, "", "{}") }()
	<-started
	select {
	case <-removed:
		t.Fatal("removal passed active read lease")
	default:
	}
	data, err := io.ReadAll(f)
	if err != nil || string(data) != "needle\n" {
		t.Fatal("active read invalidated")
	}
	f.Close()
	release()
	requireCode(t, <-removed, 200)
	requireCode(t, importRead(h, a.ID, "tail", ""), 410)
	s.mu.Lock()
	s.items[b.ID].meta.ExpiresAt = time.Now().Add(-time.Second)
	s.mu.Unlock()
	requireCode(t, importRead(h, b.ID, "tail", ""), 410)
	// Explicitly exercise the periodic sweeper's cleanup, including incomplete files.
	c := createImport(t, h, "expire", 0)
	s.mu.Lock()
	s.expireLocked(time.Now().Add(importTTL + time.Second))
	s.mu.Unlock()
	requireCode(t, importRead(h, c.ID, "tail", ""), 410)
	createImport(t, h, "shutdown", 100)
	uploadText(t, h, "shutdown-complete", "x")
	h.monitor.Close()
	h.monitor.Close()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("shutdown left temp directory")
	}
	if s.reserved != 0 || len(s.items) != 0 {
		t.Fatal("shutdown leaked reservations")
	}
	requireCode(t, importRequest(h, "POST", "", "", "", `{"name":"closed","size":0}`), 410)
	select {
	case <-s.done:
	default:
		t.Fatal("sweeper did not stop")
	}
}
func TestImportReadDeleteAndCloseRaces(t *testing.T) {
	for range 10 {
		h := importFixture(t)
		a := uploadText(t, h, "race", strings.Repeat("needle\n", 10000))
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				w := importRead(h, a.ID, "search", "")
				if w.Code != 200 && w.Code != 410 && w.Code != 429 {
					t.Errorf("read/removal: %d", w.Code)
				}
			}()
		}
		wg.Add(2)
		go func() {
			defer wg.Done()
			w := importRequest(h, "DELETE", "", a.ID, "", "{}")
			if w.Code != 200 && w.Code != 410 {
				t.Errorf("delete/close: %d", w.Code)
			}
		}()
		go func() { defer wg.Done(); h.monitor.Close() }()
		wg.Wait()
	}
}
func TestImportPageContract(t *testing.T) {
	page := string(logsHTML)
	for _, forbidden := range []string{"/api/state", "id=\"follow\"", "fileMode", "FileReader", ".arrayBuffer(", ".text()", "localStorage", "innerHTML"} {
		if strings.Contains(page, forbidden) {
			t.Fatal("page violates independence/bounds", forbidden)
		}
	}
	for _, required := range []string{"file.slice(offset,end)", "/chunk", "/finalize", "X-Log-Import", "X-Log-Offset", "ondrop", "30 分钟", "2 GiB", "4 GiB", "原文未经脱敏"} {
		if !strings.Contains(page, required) {
			t.Fatal("missing upload UI contract", required)
		}
	}
	if strings.Contains(page, " accept=") || strings.Contains(page, " multiple") {
		t.Fatal("file picker is restricted or multi-file")
	}
}

// A failed write may leave bytes beyond the committed offset. A retry must
// overwrite those bytes and finalization must reject any unexpected file length.
func TestImportPartialWriteRecoveryAndLengthValidation(t *testing.T) {
	h := importFixture(t)
	a := createImport(t, h, "partial", 4)
	p := importPath(h, a.ID)
	if err := os.WriteFile(p, []byte("junk"), 0600); err != nil {
		t.Fatal(err)
	}
	requireCode(t, importRequest(h, "POST", "/chunk", a.ID, "0", "good"), 200)
	if err := os.Truncate(p, 10); err != nil {
		t.Fatal(err)
	}
	requireCode(t, importRequest(h, "POST", "/finalize", a.ID, "", "{}"), 409)
	requireCode(t, importRead(h, a.ID, "tail", ""), 409)
	if err := os.Truncate(p, 4); err != nil {
		t.Fatal(err)
	}
	requireCode(t, importRequest(h, "POST", "/finalize", a.ID, "", "{}"), 200)
	data, err := os.ReadFile(p)
	if err != nil || string(data) != "good" {
		t.Fatal("partial retry failed", err)
	}
	// Cancellation after WriteAt rolls back this chunk without advancing offset.
	b := createImport(t, h, "cancel-during-write", 4)
	r := httptest.NewRequest("POST", "/api/logs/imports/chunk", strings.NewReader("data"))
	r.Host = "127.0.0.1:8767"
	r.Header.Set("Origin", "http://"+r.Host)
	r.Header.Set("Content-Type", "application/octet-stream")
	r.Header.Set("X-Log-Import", b.ID)
	r.Header.Set("X-Log-Offset", "0")
	r = r.WithContext(&errAfterContext{Context: context.Background(), remaining: 2})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	requireCode(t, w, 500)
	info, err := os.Stat(importPath(h, b.ID))
	if err != nil || info.Size() != 0 {
		t.Fatal("cancelled chunk not rolled back")
	}
	s := &h.monitor.logs.imports
	s.mu.Lock()
	offset := s.items[b.ID].meta.Offset
	s.mu.Unlock()
	if offset != 0 {
		t.Fatal("cancelled chunk committed")
	}
	requireCode(t, importRequest(h, "POST", "/chunk", b.ID, "0", "data"), 200)
	if _, _, err := s.acquire(b.ID); err == nil || errors.Is(err, errImportGone) {
		t.Fatal("incomplete import readable")
	}
}
