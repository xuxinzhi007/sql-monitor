package web

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"local/sql-monitor/internal/monitor"
)

func logsFixture(t *testing.T, text string) (string, Handler) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input.log")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	h := importFixture(t)
	uploadText(t, h, "input.log", text)
	return path, h
}
func logsURL(h Handler, kind string, params map[string]string) string {
	q := url.Values{"raw": {"1"}, "id": {onlyImport(h).ID}}
	for k, v := range params {
		q.Set(k, v)
	}
	return "/api/logs/" + kind + "?" + q.Encode()
}
func getLogs(t testing.TB, h Handler, kind string, params map[string]string) logReply {
	t.Helper()
	w := request(h, "GET", logsURL(h, kind, params))
	if w.Code != 200 {
		t.Fatalf("%s: %d %s", kind, w.Code, w.Body.String())
	}
	var out logReply
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Lines == nil {
		t.Fatal("nil lines")
	}
	return out
}
func collectLogs(t *testing.T, h Handler, query string, limit int) ([]logLine, []logReply) {
	t.Helper()
	cursor := ""
	var lines []logLine
	var pages []logReply
	last := int64(0)
	for calls := 0; calls < 10000; calls++ {
		out := getLogs(t, h, "search", map[string]string{"q": query, "limit": strconv.Itoa(limit), "cursor": cursor})
		if len(out.Lines) > limit || out.Offset < last || out.Offset-last > logChunk+logLineLimit+1 {
			t.Fatalf("unbounded reply: %+v", out)
		}
		if !out.Done && out.Offset == last {
			t.Fatal("no cursor progress")
		}
		lines = append(lines, out.Lines...)
		pages = append(pages, out)
		if out.Done {
			return lines, pages
		}
		last = out.Offset
		cursor = out.Cursor
	}
	t.Fatal("scan did not finish")
	return nil, nil
}
func TestLogsPaginationUnicodeAndBoundaries(t *testing.T) {
	var b strings.Builder
	var want []logLine
	for i := 0; i < 731; i++ {
		off := int64(b.Len())
		line := fmt.Sprintf("行 %d 中文 🙂 needle", i)
		if i%7 == 0 {
			line = strings.Repeat("a", 16330) + line
		}
		want = append(want, logLine{Offset: off, Text: line})
		b.WriteString(line)
		if i < 730 {
			if i%2 == 0 {
				b.WriteByte('\r')
			}
			b.WriteByte('\n')
		}
	}
	_, h := logsFixture(t, b.String())
	for _, limit := range []int{1, 7, 100} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			got, pages := collectLogs(t, h, "needle", limit)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("pagination mismatch: got %d want %d", len(got), len(want))
			}
			replay := getLogs(t, h, "search", map[string]string{"q": "needle", "limit": strconv.Itoa(limit), "cursor": pages[0].StartCursor})
			if !reflect.DeepEqual(replay.Lines, pages[0].Lines) {
				t.Fatal("start cursor cannot replay")
			}
		})
	}
}
func TestLogsChunkEdgesHugeLinesAndSearchBounds(t *testing.T) {
	text := "needle " + strings.Repeat("x", logChunk*3) + "HIDDEN_ONLY\r\n" + strings.Repeat("y", logLineLimit) + "suffix_only\nneedle 中文\nlast needle"
	_, h := logsFixture(t, text)
	got, pages := collectLogs(t, h, "needle", 100)
	if len(got) != 3 || len(pages) < 3 || !got[0].Truncated || len(got[0].Text) > logLineLimit || !strings.Contains(got[0].Text, "已截断") {
		t.Fatalf("huge line failed: hits %d chunks %d", len(got), len(pages))
	}
	for _, line := range got {
		if !utf8.ValidString(line.Text) {
			t.Fatal("invalid UTF-8")
		}
	}
	for _, query := range []string{"HIDDEN_ONLY", "suffix_only", "absent"} {
		hits, _ := collectLogs(t, h, query, 100)
		if len(hits) != 0 {
			t.Fatalf("undocumented search beyond prefix: %s", query)
		}
	}
	// A keyword split across a read-window boundary must still be found.
	for _, boundary := range []int{64 << 10, logChunk} {
		data := strings.Repeat("a\n", (boundary-8)/2) + "中文needle\r\nneedle"
		_, other := logsFixture(t, data)
		hits, _ := collectLogs(t, other, "needle", 100)
		if len(hits) != 2 {
			t.Fatalf("lost boundary hit at %d", boundary)
		}
	}
}
func TestLogsEmptyNoMatchesAndBlankLines(t *testing.T) {
	for _, text := range []string{"", "nothing\r\n", "\n\n\r\n", "no final newline"} {
		_, h := logsFixture(t, text)
		hits, _ := collectLogs(t, h, "absent", 100)
		if len(hits) != 0 {
			t.Fatal("unexpected match")
		}
		tail := getLogs(t, h, "tail", nil)
		if text == "" && (len(tail.Lines) != 0 || tail.Size != 0 || !tail.Done) {
			t.Fatal("empty file")
		}
	}
	_, h := logsFixture(t, "\n\r\nfinal")
	hits, _ := collectLogs(t, h, "", 1)
	if len(hits) != 3 || hits[0].Offset != 0 || hits[1].Offset != 1 || hits[2].Offset != 3 || hits[1].Text != "" {
		t.Fatalf("blank lines: %+v", hits)
	}
}
func TestLogsTailContextAndAppendSnapshot(t *testing.T) {
	var b strings.Builder
	var offsets []int64
	for i := 0; i < 300; i++ {
		offsets = append(offsets, int64(b.Len()))
		fmt.Fprintf(&b, "line %03d 中文\r\n", i)
	}
	path, h := logsFixture(t, b.String())
	tail := getLogs(t, h, "tail", map[string]string{"limit": "5"})
	if len(tail.Lines) != 5 || tail.Lines[0].Offset != offsets[295] {
		t.Fatal("wrong tail")
	}
	first := getLogs(t, h, "search", map[string]string{"limit": "3"})
	contextReply := getLogs(t, h, "context", map[string]string{"version": first.Version, "offset": strconv.FormatInt(offsets[150], 10), "before": "2", "after": "3"})
	if len(contextReply.Lines) != 6 || contextReply.Lines[0].Offset != offsets[148] || contextReply.Lines[5].Offset != offsets[153] {
		t.Fatalf("context: %+v", contextReply.Lines)
	}
	for _, n := range []int{0, 299} {
		r := getLogs(t, h, "context", map[string]string{"version": first.Version, "offset": strconv.FormatInt(offsets[n], 10), "before": "0", "after": "0"})
		if len(r.Lines) != 1 || r.Lines[0].Offset != offsets[n] {
			t.Fatal("exact context")
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("APPENDED\n")
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	cursor := first.Cursor
	total := len(first.Lines)
	for {
		out := getLogs(t, h, "search", map[string]string{"cursor": cursor})
		if out.Size != int64(b.Len()) {
			t.Fatal("snapshot size grew")
		}
		for _, line := range out.Lines {
			if strings.Contains(line.Text, "APPENDED") {
				t.Fatal("append leaked into snapshot")
			}
		}
		total += len(out.Lines)
		cursor = out.Cursor
		if out.Done {
			break
		}
	}
	if total != 300 {
		t.Fatalf("snapshot lost/duplicated lines: %d", total)
	}
	refreshed := getLogs(t, h, "tail", map[string]string{"version": tail.Version, "limit": "1"})
	if len(refreshed.Lines) != 1 || refreshed.Lines[0].Text != "line 299 中文" {
		t.Fatal("import snapshot followed original file append")
	}
	// The final non-newline line is fixed at snapshot size, even when extended.
	p, hh := logsFixture(t, "first\nneedle")
	r := getLogs(t, hh, "search", map[string]string{"limit": "1"})
	ff, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	ff.WriteString("extended\n")
	ff.Close()
	r = getLogs(t, hh, "search", map[string]string{"cursor": r.Cursor})
	if len(r.Lines) != 1 || r.Lines[0].Text != "needle" {
		t.Fatal("snapshot partial line grew")
	}
}
func TestLogsContextBoundsAndInvalidOffsets(t *testing.T) {
	text := strings.Repeat("z", logWindow*3) + "\nneedle\n" + strings.Repeat("x", logWindow*3)
	_, h := logsFixture(t, text)
	hits, _ := collectLogs(t, h, "needle", 10)
	if len(hits) != 1 {
		t.Fatal("missing middle hit")
	}
	tail := getLogs(t, h, "tail", nil)
	if len(tail.Lines) != 0 || !tail.Limited {
		t.Fatal("partial giant tail should not be fabricated as a line")
	}
	r := getLogs(t, h, "context", map[string]string{"version": tail.Version, "offset": strconv.FormatInt(hits[0].Offset, 10)})
	if r.WindowEnd-r.WindowStart > 2*logWindow || !r.Limited || len(r.Lines) != 2 || !r.Lines[1].Truncated {
		t.Fatalf("context window: %+v", r)
	}
	w := request(h, "GET", logsURL(h, "context", map[string]string{"version": tail.Version, "offset": strconv.FormatInt(hits[0].Offset+1, 10)}))
	if w.Code != 409 {
		t.Fatal("mid-line offset accepted")
	}
	w = request(h, "GET", logsURL(h, "context", map[string]string{"version": tail.Version, "offset": strconv.Itoa(logWindow + 1)}))
	if w.Code != 409 {
		t.Fatal("offset inside a newline-free giant window accepted")
	}
}
func TestLogsReplacementTruncationAndMutation(t *testing.T) {
	for _, change := range []string{"replace", "truncate", "rewrite", "restored-mtime", "edge-and-append", "missing"} {
		t.Run(change, func(t *testing.T) {
			text := strings.Repeat("normal needle\n", 2000)
			_, h := logsFixture(t, text)
			// Defense in depth against corruption of the owned temporary copy.
			path := importPath(h, onlyImport(h).ID)
			first := getLogs(t, h, "search", map[string]string{"limit": "1"})
			info, _ := os.Stat(path)
			switch change {
			case "replace":
				replacement := filepath.Join(filepath.Dir(path), "new.log")
				if err := os.WriteFile(replacement, []byte(text), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(replacement, path); err != nil {
					t.Fatal(err)
				}
			case "truncate":
				if err := os.Truncate(path, 2); err != nil {
					t.Fatal(err)
				}
			case "rewrite":
				f, _ := os.OpenFile(path, os.O_WRONLY, 0)
				f.WriteAt([]byte("changed"), 12000)
				f.Close()
				if err := os.Chtimes(path, time.Now(), info.ModTime().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			case "restored-mtime":
				f, err := os.OpenFile(path, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.WriteAt([]byte("changed"), 12000); err != nil {
					t.Fatal(err)
				}
				f.Close()
				if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
					t.Fatal(err)
				}
			case "edge-and-append":
				f, _ := os.OpenFile(path, os.O_WRONLY, 0)
				f.WriteAt([]byte("changed"), 0)
				f.WriteAt([]byte("append"), info.Size())
				f.Close()
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			for _, route := range []string{logsURL(h, "search", map[string]string{"cursor": first.Cursor}), logsURL(h, "tail", map[string]string{"version": first.Version}), logsURL(h, "context", map[string]string{"version": first.Version, "offset": "0"})} {
				w := request(h, "GET", route)
				if w.Code != 409 {
					t.Fatalf("stale %s: %d", change, w.Code)
				}
			}
		})
	}
}
func TestLogsHTTPValidationPrivacyAndSource(t *testing.T) {
	path, h := logsFixture(t, "private-secret\nsecond\n")
	before, _ := os.ReadFile(path)
	for _, kind := range []string{"search", "tail", "context"} {
		for _, suffix := range []string{"", "?raw=0", "?raw=1&path=/etc/passwd", "?raw=1&log=/etc/passwd", "?raw=1&raw=1", "?raw=1&limit=-1", "?raw=1&limit=101", "?raw=1&limit=", "?raw=1&x=%zz"} {
			w := request(h, "GET", "/api/logs/"+kind+suffix)
			if w.Code != 400 || strings.Contains(w.Body.String(), "private-secret") {
				t.Fatalf("bad validation %s%s: %d", kind, suffix, w.Code)
			}
		}
		for _, method := range []string{"POST", "HEAD", "PUT", "DELETE", "OPTIONS"} {
			if request(h, method, logsURL(h, kind, nil)).Code != 405 {
				t.Fatal("method accepted")
			}
		}
		for _, header := range []http.Header{{"Origin": {"http://evil.test"}}, {"Origin": {"null"}}, {"Origin": {"http://127.0.0.1:8767", "http://127.0.0.1:8767"}}, {"Sec-Fetch-Site": {"cross-site"}}, {"Host": {"127.0.0.1:8767"}}} {
			r := httptest.NewRequest("GET", logsURL(h, kind, nil), nil)
			r.Host = "127.0.0.1:8767"
			r.Header = header
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 403 {
				t.Fatal("origin guard bypassed")
			}
		}
		r := httptest.NewRequest("GET", logsURL(h, kind, nil), nil)
		r.Host = "evil.test"
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 403 {
			t.Fatal("host guard bypassed")
		}
	}
	for _, route := range []string{"/logs?raw=1", "/logs.html", "/api/logs/%74ail?raw=1", "/api/logs/unknown?raw=1", "http://127.0.0.1:8767/api/logs/tail?raw=1"} {
		if request(h, "GET", route).Code != 404 {
			t.Fatalf("unexpected route %s", route)
		}
	}
	for _, params := range []map[string]string{{"q": "a\nb"}, {"q": strings.Repeat("a", 1025)}, {"limit": "+1"}, {"limit": "1.0"}, {"limit": "99999999999999999999999999"}} {
		if request(h, "GET", logsURL(h, "search", params)).Code != 400 {
			t.Fatal("invalid search params")
		}
	}
	first := getLogs(t, h, "search", map[string]string{"limit": "1"})
	for _, params := range []map[string]string{{"cursor": first.Cursor + "x"}, {"cursor": first.Cursor, "q": "different"}, {"cursor": first.Version}} {
		if request(h, "GET", logsURL(h, "search", params)).Code != 409 {
			t.Fatal("invalid signed cursor accepted")
		}
	}
	_, other := logsFixture(t, "other\n")
	if request(other, "GET", logsURL(other, "search", map[string]string{"cursor": first.Cursor})).Code != 409 {
		t.Fatal("cursor crossed monitor source")
	}
	if request(h, "GET", logsURL(h, "context", map[string]string{"version": first.Version})).Code != 400 {
		t.Fatal("missing offset accepted")
	}
	if request(h, "GET", logsURL(h, "context", map[string]string{"version": first.Version, "offset": "0", "before": "51"})).Code != 400 {
		t.Fatal("unbounded context")
	}
	w := request(h, "GET", "/logs")
	if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), LogsHTML) || strings.Contains(w.Body.String(), "private-secret") {
		t.Fatal("embedded page leaked raw")
	}
	w = request(h, "GET", logsURL(h, "tail", nil))
	for k, v := range map[string]string{"Cache-Control": "no-store", "Content-Security-Policy": contentSecurityPolicy, "X-Content-Type-Options": "nosniff"} {
		if w.Header().Get(k) != v {
			t.Fatal("missing security header")
		}
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("log modified")
	}
	h.monitor.SetSource("stdin", "", "reading")
	for _, kind := range []string{"search", "tail", "context"} {
		params := map[string]string{}
		if kind == "context" {
			params["version"], params["offset"] = first.Version, "0"
		}
		w := request(h, "GET", logsURL(h, kind, params))
		if w.Code != 200 || !strings.Contains(w.Body.String(), "private-secret") {
			t.Fatal("import depends on SQL source mode")
		}
	}
}
func TestLogsNonregularCancelConcurrency(t *testing.T) {
	_, h := logsFixture(t, strings.Repeat("line\n", 10000))
	path := importPath(h, onlyImport(h).ID)
	r := httptest.NewRequest("GET", logsURL(h, "search", nil), nil)
	r.Host = "127.0.0.1:8767"
	ctx, cancel := context.WithCancel(r.Context())
	cancel()
	r = r.WithContext(ctx)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 408 {
		t.Fatalf("cancel: %d", w.Code)
	}
	for i := 0; i < cap(logSlots); i++ {
		logSlots <- struct{}{}
	}
	w = request(h, "GET", logsURL(h, "search", nil))
	for i := 0; i < cap(logSlots); i++ {
		<-logSlots
	}
	if w.Code != 429 {
		t.Fatal("concurrency unbounded")
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				w := request(h, "GET", logsURL(h, "search", nil))
				if w.Code != 200 && w.Code != 429 {
					t.Errorf("concurrent: %d", w.Code)
				}
			}
		}()
	}
	wg.Wait()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if request(h, "GET", logsURL(h, "tail", nil)).Code != 409 {
		t.Fatal("FIFO accepted")
	}
	if time.Since(start) > time.Second {
		t.Fatal("FIFO blocked")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if request(h, "GET", logsURL(h, "tail", nil)).Code != 409 {
		t.Fatal("directory accepted")
	}
}

// errAfterContext exercises cooperative cancellation after work has begun,
// without timing-dependent sleeps or a test-only hook in the scanner.
type errAfterContext struct {
	context.Context
	remaining int
}

func (c *errAfterContext) Err() error {
	c.remaining--
	if c.remaining <= 0 {
		return context.Canceled
	}
	return nil
}

func TestLogsCancellationDuringReadAndMaximumKeyword(t *testing.T) {
	path, h := logsFixture(t, strings.Repeat("a short line\n", 100000))
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, _ := f.Stat()
	ctx := &errAfterContext{Context: context.Background(), remaining: 30}
	_, offset, _, err := scanLog(ctx, f, info.Size(), 0, false, "absent", 100)
	if err != context.Canceled || offset == 0 || offset >= info.Size() {
		t.Fatalf("mid-scan cancellation: offset=%d err=%v", offset, err)
	}
	ctx = &errAfterContext{Context: context.Background(), remaining: 30}
	if _, _, _, _, err := windowLog(ctx, f, info.Size(), info.Size(), 0, 100, true); err != context.Canceled {
		t.Fatalf("context cancellation: %v", err)
	}
	// JSON escaping can make a legal 1024-byte keyword much larger in a token.
	query := strings.Repeat("\x00", 1024)
	first := getLogs(t, h, "search", map[string]string{"q": query})
	getLogs(t, h, "search", map[string]string{"q": query, "cursor": first.Cursor})
	for _, data := range [][]byte{bytes.Repeat([]byte("中"), logLineLimit), bytes.Repeat([]byte{0xff, 'x'}, logLineLimit)} {
		text := logDisplay(data[:logLineLimit], true)
		if len(text) > logLineLimit || !utf8.ValidString(text) || !strings.HasSuffix(text, "[已截断]") {
			t.Fatal("truncated display is not bounded UTF-8")
		}
	}
}

// BenchmarkLogs500k uses only generated mixed JSON/text/stack/Unicode records.
// Each operation traverses the complete 500,000-line file through the HTTP API.
func BenchmarkLogs500k(b *testing.B) {
	path := filepath.Join(b.TempDir(), "synthetic.log")
	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	w := bufio.NewWriter(f)
	for i := 0; i < 500000; i++ {
		marker := "ordinary"
		if i%1000 == 0 {
			marker = "needle"
		}
		var line string
		switch i % 4 {
		case 0:
			line = fmt.Sprintf("{\"level\":\"info\",\"msg\":\"%s\",\"n\":%d}", marker, i)
		case 1:
			line = fmt.Sprintf("2026-01-01 INFO %s 中文任务 %d %s", marker, i, strings.Repeat("x", 64))
		case 2:
			line = fmt.Sprintf("    at worker.go:%d %s %s", i, marker, strings.Repeat("y", 160))
		default:
			line = fmt.Sprintf("arbitrary text %s %s", marker, strings.Repeat("z", 300))
		}
		if _, err := w.WriteString(line + "\r\n"); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		b.Fatal(err)
	}
	f.Close()
	info, _ := os.Stat(path)
	b.Logf("synthetic lines=500000 bytes=%d", info.Size())
	for _, query := range []string{"needle", "never-present", ""} {
		name := query
		if name == "" {
			name = "all"
		}
		b.Run(name, func(b *testing.B) {
			m, err := monitor.New(path, 1, 0)
			if err != nil {
				b.Fatal(err)
			}
			defer m.Close()
			h := Handler{monitor: m, logs: &logAccess{}, port: 8767}
			input, err := os.Open(path)
			if err != nil {
				b.Fatal(err)
			}
			uploadReader(b, h, "synthetic.log", info.Size(), input)
			input.Close()
			b.SetBytes(info.Size())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cursor := ""
				count := 0
				for {
					out := getLogs(b, h, "search", map[string]string{"q": query, "cursor": cursor})
					count += len(out.Lines)
					if out.Done {
						break
					}
					cursor = out.Cursor
				}
				expected := 0
				if query == "needle" {
					expected = 500
				}
				if query == "" {
					expected = 500000
				}
				if count != expected {
					b.Fatalf("hits=%d expected=%d", count, expected)
				}
			}
		})
	}
}
