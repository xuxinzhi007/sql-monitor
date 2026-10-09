package web

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

const logChunk = 2 << 20
const logLineLimit = 16 << 10
const logWindow = 256 << 10

var errLogStale = errors.New("文件已变化或游标失效，请重新读取")
var logSlots = make(chan struct{}, 4)

// Tokens are authenticated, scoped to this monitor, import and query, and contain
// no log text. Only owned import files survive requests; no search jobs or match
// lists are retained.
type logAccess struct {
	once    sync.Once
	key     [32]byte
	err     error
	imports importStore
}
type logVersion struct {
	Device   uint64 `json:"d"`
	Inode    uint64 `json:"i"`
	Size     int64  `json:"s"`
	Observed int64  `json:"z"`
	Modified int64  `json:"m"`
	Changed  int64  `json:"c"`
	Edge     string `json:"e"`
}
type logTicket struct {
	ImportID string     `json:"i"`
	Version  logVersion `json:"v"`
	Kind     string     `json:"k"`
	Query    string     `json:"q,omitempty"`
	Offset   int64      `json:"o"`
	Skip     bool       `json:"x,omitempty"`
}
type logLine struct {
	Offset    int64  `json:"offset"`
	Text      string `json:"text"`
	Truncated bool   `json:"truncated"`
}
type logReply struct {
	Version     string    `json:"version"`
	Cursor      string    `json:"cursor"`
	StartCursor string    `json:"start_cursor"`
	Size        int64     `json:"size"`
	Offset      int64     `json:"offset"`
	Done        bool      `json:"done"`
	Lines       []logLine `json:"lines"`
	WindowStart int64     `json:"window_start"`
	WindowEnd   int64     `json:"window_end"`
	Limited     bool      `json:"limited"`
}

func (a *logAccess) sign(t logTicket) (string, error) {
	a.once.Do(func() { _, a.err = rand.Read(a.key[:]) })
	if a.err != nil {
		return "", a.err
	}
	data, err := json.Marshal(t)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, a.key[:])
	mac.Write(data)
	return base64.RawURLEncoding.EncodeToString(data) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}
func (a *logAccess) verify(s string) (logTicket, error) {
	var t logTicket
	if len(s) > 12000 {
		return t, errLogStale
	}
	parts := strings.Split(s, ".")
	if len(parts) != 2 {
		return t, errLogStale
	}
	data, e := base64.RawURLEncoding.DecodeString(parts[0])
	if e != nil {
		return t, errLogStale
	}
	sig, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil {
		return t, errLogStale
	}
	a.once.Do(func() { _, a.err = rand.Read(a.key[:]) })
	if a.err != nil {
		return t, a.err
	}
	mac := hmac.New(sha256.New, a.key[:])
	mac.Write(data)
	if !hmac.Equal(sig, mac.Sum(nil)) || json.Unmarshal(data, &t) != nil {
		return t, errLogStale
	}
	return t, nil
}
func logIdentity(info os.FileInfo) (uint64, uint64) {
	s := info.Sys().(*syscall.Stat_t)
	return uint64(s.Dev), uint64(s.Ino)
}

// Darwin calls this Ctimespec; Linux and several other Unix targets use Ctim.
// Unlike mtime, ctime is not restored by os.Chtimes or ordinary copy utilities.
func logChangeTime(info os.FileInfo) int64 {
	stat := reflect.ValueOf(info.Sys()).Elem()
	for _, name := range []string{"Ctimespec", "Ctim"} {
		field := stat.FieldByName(name)
		if field.IsValid() {
			return field.FieldByName("Sec").Int()*1e9 + field.FieldByName("Nsec").Int()
		}
	}
	return info.ModTime().UnixNano()
}

func logEdges(f *os.File, size int64) (string, error) {
	hash := sha256.New()
	buf := make([]byte, 4096)
	for _, off := range []int64{0, max(int64(0), size-4096)} {
		n := int(min(int64(len(buf)), size-off))
		if n < 0 {
			return "", errLogStale
		}
		if _, err := f.ReadAt(buf[:n], off); err != nil {
			return "", err
		}
		hash.Write(buf[:n])
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
func newLogVersion(f *os.File, info os.FileInfo) (logVersion, error) {
	d, i := logIdentity(info)
	edge, err := logEdges(f, info.Size())
	return logVersion{Device: d, Inode: i, Size: info.Size(), Observed: info.Size(), Modified: info.ModTime().UnixNano(), Changed: logChangeTime(info), Edge: edge}, err
}

// Same-size writes are rejected by metadata; growth must preserve snapshot edges.
// Bounded validation cannot prove an arbitrary middle rewrite combined with append
// did not occur. This limitation is also disclosed in the UI.
func checkLogVersion(f *os.File, path string, v logVersion) (logVersion, error) {
	info, err := f.Stat()
	if err != nil {
		return v, errLogStale
	}
	current, err := os.Stat(path)
	if err != nil || !os.SameFile(info, current) {
		return v, errLogStale
	}
	d, i := logIdentity(info)
	if d != v.Device || i != v.Inode || info.Size() < v.Observed || (info.Size() == v.Observed && (info.ModTime().UnixNano() != v.Modified || logChangeTime(info) != v.Changed)) {
		return v, errLogStale
	}
	edge, err := logEdges(f, v.Size)
	if err != nil || edge != v.Edge {
		return v, errLogStale
	}
	v.Observed = info.Size()
	v.Modified = info.ModTime().UnixNano()
	v.Changed = logChangeTime(info)
	return v, nil
}
func logDisplay(data []byte, truncated bool) string {
	const marker = " … [已截断]"
	data = bytes.TrimSuffix(data, []byte{'\r'})
	text := strings.ToValidUTF8(string(data), "�")
	limit := logLineLimit
	if truncated {
		limit -= len(marker)
	}
	if len(text) > limit {
		text = text[:limit]
		for !utf8.ValidString(text) {
			text = text[:len(text)-1]
		}
		truncated = true
	}
	if truncated {
		if len(text) > logLineLimit-len(marker) {
			text = text[:logLineLimit-len(marker)]
			for !utf8.ValidString(text) {
				text = text[:len(text)-1]
			}
		}
		text += marker
	}
	return text
}

// scanLog searches only the first 16 KiB of each physical line. Oversized lines
// are drained without accumulating data, even when they span many requests.
func scanLog(ctx context.Context, f *os.File, size, offset int64, skip bool, query string, limit int) ([]logLine, int64, bool, error) {
	lines := make([]logLine, 0, min(limit, 100))
	const block = 64 << 10
	storage := make([]byte, block+logLineLimit+1)
	start := offset
	deadline := time.Now().Add(700 * time.Millisecond)
	for offset < size && offset-start < logChunk && len(lines) < limit {
		if err := ctx.Err(); err != nil {
			return lines, offset, skip, err
		}
		if time.Now().After(deadline) {
			break
		}
		blockStart := offset
		budget := min(int64(block), logChunk-(offset-start))
		end := min(size, offset+budget+logLineLimit+1)
		buf := storage[:int(end-offset)]
		if _, err := f.ReadAt(buf, offset); err != nil {
			return lines, offset, skip, err
		}
		pos := 0
		for pos < len(buf) && offset-blockStart < budget && len(lines) < limit {
			if err := ctx.Err(); err != nil {
				return lines, offset, skip, err
			}
			rest := buf[pos:]
			nl := bytes.IndexByte(rest, '\n')
			length := len(rest)
			if nl >= 0 {
				length = nl
			}
			if !skip {
				prefix := rest[:min(length, logLineLimit)]
				if query == "" || bytes.Contains(prefix, []byte(query)) {
					truncated := length > logLineLimit
					lines = append(lines, logLine{Offset: offset, Text: logDisplay(prefix, truncated), Truncated: truncated})
				}
			}
			consumed := length
			if nl >= 0 {
				consumed++
				skip = false
			} else {
				skip = end < size
			}
			pos += consumed
			offset += int64(consumed)
		}
	}
	return lines, offset, skip, nil
}

// windowLog reads a fixed window, never counts earlier lines. Partial leading
// physical lines are excluded; long trailing lines get explicit truncation.
func windowLog(ctx context.Context, f *os.File, size, selected int64, before, after int, tail bool) ([]logLine, int64, int64, bool, error) {
	start := max(int64(0), selected-logWindow)
	end := min(size, selected+logWindow)
	if tail {
		end = size
		start = max(int64(0), size-logWindow)
	}
	if err := ctx.Err(); err != nil {
		return nil, start, end, false, err
	}
	buf := make([]byte, int(end-start))
	if _, err := f.ReadAt(buf, start); err != nil {
		return nil, start, end, false, err
	}
	pos := 0
	limited := start > 0 || end < size
	if start > 0 {
		var prev [1]byte
		if _, err := f.ReadAt(prev[:], start-1); err != nil {
			return nil, start, end, false, err
		}
		if prev[0] != '\n' {
			idx := bytes.IndexByte(buf, '\n')
			if idx < 0 {
				if !tail {
					return nil, start, end, true, errLogStale
				}
				return []logLine{}, start, end, true, nil
			}
			pos = idx + 1
		}
	}
	lines := make([]logLine, 0, 101)
	found := tail
	for pos < len(buf) {
		if err := ctx.Err(); err != nil {
			return nil, start, end, false, err
		}
		off := start + int64(pos)
		n := bytes.IndexByte(buf[pos:], '\n')
		terminated := n >= 0
		if n < 0 {
			n = len(buf) - pos
		}
		truncated := n > logLineLimit || (!terminated && end < size)
		line := logLine{Offset: off, Text: logDisplay(buf[pos:pos+min(n, logLineLimit)], truncated), Truncated: truncated}
		if tail {
			lines = append(lines, line)
			if len(lines) > after {
				copy(lines, lines[1:])
				lines = lines[:after]
			}
		} else if off < selected {
			lines = append(lines, line)
			if len(lines) > before {
				copy(lines, lines[1:])
				lines = lines[:before]
			}
		} else {
			if off == selected {
				found = true
			}
			lines = append(lines, line)
			after--
			if after < 0 {
				break
			}
		}
		pos += n
		if terminated {
			pos++
		}
	}
	if !found {
		return nil, start, end, limited, errLogStale
	}
	return lines, start, end, limited, nil
}

func logParams(r *http.Request, allowed ...string) (url.Values, error) {
	if len(r.URL.RawQuery) > 16000 {
		return nil, errors.New("参数过长")
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	for key, v := range q {
		valid := false
		for _, a := range allowed {
			if key == a {
				valid = true
			}
		}
		if !valid || len(v) != 1 {
			return nil, errors.New("未知或重复参数")
		}
	}
	if q.Get("raw") != "1" {
		return nil, errors.New("请先确认原文隐私提示（raw=1）")
	}
	return q, nil
}
func logNumber(q url.Values, key string, def, minValue, maxValue int64) (int64, error) {
	v, ok := q[key]
	if !ok {
		return def, nil
	}
	if v[0] == "" {
		return 0, errors.New("无效数值参数")
	}
	for _, c := range v[0] {
		if c < '0' || c > '9' {
			return 0, errors.New("无效数值参数")
		}
	}
	n, e := strconv.ParseInt(v[0], 10, 64)
	if e != nil || n < minValue || n > maxValue {
		return 0, errors.New("数值参数超出范围")
	}
	return n, nil
}
func (h Handler) serveLogs(w http.ResponseWriter, r *http.Request) {
	fail := func(code int, err error) { sendJSON(w, r, code, map[string]string{"error": err.Error()}) }
	kind := strings.TrimPrefix(r.URL.Path, "/api/logs/")
	var allowed []string
	switch kind {
	case "search":
		allowed = []string{"raw", "q", "cursor", "limit"}
	case "tail":
		allowed = []string{"raw", "version", "limit"}
	case "context":
		allowed = []string{"raw", "version", "offset", "before", "after"}
	default:
		fail(404, errors.New("Not found"))
		return
	}
	allowed = append(allowed, "id")
	q, err := logParams(r, allowed...)
	if err != nil {
		fail(400, err)
		return
	}
	query := q.Get("q")
	if len(query) > 1024 || !utf8.ValidString(query) || strings.ContainsAny(query, "\r\n") {
		fail(400, errors.New("关键词必须为单行 UTF-8 文本，最多 1024 bytes"))
		return
	}
	limit, err := logNumber(q, "limit", 100, 1, 100)
	if err != nil {
		fail(400, err)
		return
	}
	before, err := logNumber(q, "before", 10, 0, 50)
	if err != nil {
		fail(400, err)
		return
	}
	after, err := logNumber(q, "after", 10, 0, 50)
	if err != nil {
		fail(400, err)
		return
	}
	select {
	case <-r.Context().Done():
		fail(408, r.Context().Err())
		return
	case logSlots <- struct{}{}:
		defer func() { <-logSlots }()
	default:
		fail(429, errors.New("日志读取繁忙，请稍后重试"))
		return
	}
	item, release, err := h.logs.imports.acquire(q.Get("id"))
	if err != nil {
		code := 409
		if errors.Is(err, errImportGone) {
			code = 410
		}
		fail(code, err)
		return
	}
	defer release()
	path := item.path
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		fail(409, errLogStale)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		fail(409, errors.New("导入副本不是可读普通文件"))
		return
	}
	var ticket logTicket
	token := q.Get("version")
	if kind == "search" {
		token = q.Get("cursor")
	}
	if token != "" {
		ticket, err = h.logs.verify(token)
		if err != nil || ticket.ImportID != q.Get("id") || (kind == "search" && (ticket.Kind != "search" || ticket.Query != query)) || (kind != "search" && ticket.Kind != "version") {
			fail(409, errLogStale)
			return
		}
		ticket.Version, err = checkLogVersion(f, path, ticket.Version)
		if err != nil {
			fail(409, errLogStale)
			return
		}
	} else {
		if kind == "context" {
			fail(400, errors.New("上下文需要 version 和 offset"))
			return
		}
		ticket.Version, err = newLogVersion(f, info)
		if err != nil {
			fail(409, errLogStale)
			return
		}
	}
	if kind == "tail" {
		ticket.Version, err = newLogVersion(f, info)
		if err != nil {
			fail(409, errLogStale)
			return
		}
	}
	ticket.ImportID = q.Get("id")
	v := ticket.Version
	out := logReply{Size: v.Size, Lines: []logLine{}}
	switch kind {
	case "search":
		if ticket.Offset < 0 || ticket.Offset > v.Size {
			fail(409, errLogStale)
			return
		}
		ticket.Kind = "search"
		ticket.Query = query
		out.StartCursor, err = h.logs.sign(ticket)
		if err != nil {
			fail(500, errors.New("无法生成安全游标"))
			return
		}
		out.Lines, out.Offset, ticket.Skip, err = scanLog(r.Context(), f, v.Size, ticket.Offset, ticket.Skip, query, int(limit))
		ticket.Offset = out.Offset
		ticket.Kind = "search"
		ticket.Query = query
		out.Done = out.Offset == v.Size
	case "tail":
		out.Lines, out.WindowStart, out.WindowEnd, out.Limited, err = windowLog(r.Context(), f, v.Size, v.Size, 0, int(limit), true)
		out.Offset = v.Size
		out.Done = true
	case "context":
		var offset int64
		offset, err = logNumber(q, "offset", -1, 0, max(int64(0), v.Size-1))
		if err != nil || offset < 0 {
			fail(400, errors.New("无效字节偏移"))
			return
		}
		out.Lines, out.WindowStart, out.WindowEnd, out.Limited, err = windowLog(r.Context(), f, v.Size, offset, int(before), int(after), false)
		out.Offset = offset
		out.Done = true
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			fail(408, err)
		} else {
			fail(409, errLogStale)
		}
		return
	}
	v, err = checkLogVersion(f, path, v)
	if err != nil {
		fail(409, errLogStale)
		return
	}
	if err = r.Context().Err(); err != nil {
		fail(408, err)
		return
	}
	ticket.Version = v
	out.Version, err = h.logs.sign(logTicket{ImportID: q.Get("id"), Version: v, Kind: "version"})
	if err == nil && kind == "search" {
		out.Cursor, err = h.logs.sign(ticket)
	}
	if err != nil {
		fail(500, errors.New("无法生成安全游标"))
		return
	}
	sendJSON(w, r, 200, out)
}
