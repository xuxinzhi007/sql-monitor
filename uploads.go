package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	importChunkSize       = 1 << 20
	importMaxSize   int64 = 2 << 30
	importMaxTotal  int64 = 4 << 30
	importMaxFiles        = 4
	importTTL             = 30 * time.Minute
)

var errImportGone = errors.New("导入不存在、已移除或已过期，请重新导入")
var importSlots = make(chan struct{}, 4)

// Only server-generated paths live here. The declared size is reserved before
// creating a file, including incomplete imports. Lock order: store, then item.
// Readers hold item.RLock until their file is closed; removal waits for them.
type logImport struct {
	mu   sync.RWMutex
	path string
	meta importReply
}
type importReply struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Size      int64     `json:"size"`
	Offset    int64     `json:"offset"`
	Complete  bool      `json:"complete"`
	ChunkSize int       `json:"chunk_size"`
	ExpiresAt time.Time `json:"expires_at"`
}
type importStore struct {
	mu       sync.Mutex
	dir      string
	items    map[string]*logImport
	reserved int64
	closed   bool
	stop     chan struct{}
	done     chan struct{}
}

func importName(name string) string {
	// Treat both path separators as separators, irrespective of server OS.
	name = filepath.Base(strings.ReplaceAll(name, "\\", "/"))
	var b strings.Builder
	for _, c := range name {
		if unicode.IsControl(c) || unicode.Is(unicode.Cf, c) {
			continue
		}
		if b.Len()+len(string(c)) > 180 {
			break
		}
		b.WriteRune(c)
	}
	name = strings.TrimSpace(b.String())
	if name == "" || name == "." || name == ".." {
		return "imported-log"
	}
	return name
}

func (s *importStore) initLocked() error {
	if s.dir != "" {
		return nil
	}
	dir, err := os.MkdirTemp("", "sql-monitor-imports-") // 0700
	if err != nil {
		return err
	}
	s.dir, s.items = dir, make(map[string]*logImport)
	s.stop, s.done = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case now := <-ticker.C:
				s.mu.Lock()
				s.expireLocked(now)
				s.mu.Unlock()
			}
		}
	}()
	return nil
}
func (s *importStore) removeLocked(id string, item *logImport) error {
	item.mu.Lock()
	defer item.mu.Unlock()
	if err := os.Remove(item.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	delete(s.items, id)
	s.reserved -= item.meta.Size
	return nil
}
func (s *importStore) expireLocked(now time.Time) {
	for id, item := range s.items {
		if !now.Before(item.meta.ExpiresAt) {
			_ = s.removeLocked(id, item)
		}
	}
}
func (s *importStore) close() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		if s.stop != nil {
			close(s.stop)
		}
		for id, item := range s.items {
			_ = s.removeLocked(id, item)
		}
		if s.dir != "" {
			_ = os.RemoveAll(s.dir)
		}
	}
	done := s.done
	s.mu.Unlock()
	if done != nil {
		<-done
	}
}

// The returned unlock must run AFTER closing the opened file.
func (s *importStore) acquire(id string) (*logImport, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.items[id]
	if s.closed || item == nil {
		return nil, nil, errImportGone
	}
	if !time.Now().Before(item.meta.ExpiresAt) {
		_ = s.removeLocked(id, item)
		return nil, nil, errImportGone
	}
	if !item.meta.Complete {
		return nil, nil, errors.New("导入尚未完成，请先完成上传")
	}
	item.meta.ExpiresAt = time.Now().Add(importTTL)
	item.mu.RLock()
	return item, item.mu.RUnlock, nil
}

// Static, exact endpoints only; query strings and escaped paths are not aliases.
func importEndpoint(uri string) bool {
	return uri == "/api/logs/imports" || uri == "/api/logs/imports/chunk" || uri == "/api/logs/imports/finalize"
}
func (h monitorHandler) serveImport(w http.ResponseWriter, r *http.Request) {
	fail := func(code int, message string) { sendJSON(w, r, code, map[string]string{"error": message}) }
	if r.Method != http.MethodPost && !(r.Method == http.MethodDelete && r.RequestURI == "/api/logs/imports") {
		fail(405, "此导入端点不支持该方法")
		return
	}
	origins := r.Header.Values("Origin")
	if len(origins) != 1 || origins[0] != "http://"+r.Host {
		fail(403, "写入导入副本需要完全同源的 Origin")
		return
	}
	chunk := r.RequestURI == "/api/logs/imports/chunk"
	wantType := "application/json"
	if chunk {
		wantType = "application/octet-stream"
	}
	types := r.Header.Values("Content-Type")
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if len(types) != 1 || err != nil || mediaType != wantType {
		fail(415, "需要 Content-Type: "+wantType)
		return
	}
	select {
	case importSlots <- struct{}{}:
		defer func() { <-importSlots }()
	default:
		fail(429, "上传繁忙，请稍后重试")
		return
	}
	maxBody := int64(4096)
	if chunk {
		maxBody = importChunkSize
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			fail(413, "请求体超过限制")
		} else {
			fail(400, "未完整接收请求体，可重试当前块")
		}
		return
	}
	if r.Context().Err() != nil {
		fail(408, "上传已取消")
		return
	}
	var metadata struct {
		Name string `json:"name"`
		Size *int64 `json:"size"`
	}
	create := r.Method == http.MethodPost && r.RequestURI == "/api/logs/imports"
	if !chunk {
		decoder := json.NewDecoder(strings.NewReader(string(body)))
		decoder.DisallowUnknownFields()
		var target any = &struct{}{}
		if create {
			target = &metadata
		}
		if err := decoder.Decode(target); err != nil || strings.TrimSpace(string(body)) == "null" {
			fail(400, "无效 JSON 元数据")
			return
		}
		if decoder.Decode(&struct{}{}) != io.EOF {
			fail(400, "只接受单个 JSON 对象")
			return
		}
	}
	s := &h.monitor.logs.imports
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		fail(410, errImportGone.Error())
		return
	}
	s.expireLocked(time.Now())
	if create {
		if metadata.Size == nil || *metadata.Size < 0 || metadata.Name == "" {
			fail(400, "需要文件名和非负文件长度")
			return
		}
		if *metadata.Size > importMaxSize {
			fail(413, "单文件最多 2 GiB")
			return
		}
		if len(s.items) >= importMaxFiles || *metadata.Size > importMaxTotal-s.reserved {
			fail(429, "最多 4 个导入，总计 4 GiB；请移除旧导入")
			return
		}
		var token [32]byte
		if _, err := rand.Read(token[:]); err != nil {
			fail(500, "无法生成导入标识")
			return
		}
		if err := s.initLocked(); err != nil {
			fail(500, "无法创建本地临时目录")
			return
		}
		id := hex.EncodeToString(token[:])
		path := filepath.Join(s.dir, id)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			fail(500, "无法创建临时副本")
			return
		}
		if err := f.Close(); err != nil {
			_ = os.Remove(path)
			fail(500, "无法关闭临时副本")
			return
		}
		item := &logImport{path: path, meta: importReply{ID: id, Name: importName(metadata.Name), Size: *metadata.Size, ChunkSize: importChunkSize, ExpiresAt: time.Now().Add(importTTL)}}
		s.items[id], s.reserved = item, s.reserved+item.meta.Size
		sendJSON(w, r, 201, item.meta)
		return
	}
	ids := r.Header.Values("X-Log-Import")
	if len(ids) != 1 || len(ids[0]) != 64 {
		fail(410, errImportGone.Error())
		return
	}
	id := ids[0]
	item := s.items[id]
	if item == nil {
		fail(410, errImportGone.Error())
		return
	}
	if r.Method == http.MethodDelete {
		if err := s.removeLocked(id, item); err != nil {
			fail(500, "无法移除临时副本，请重试")
			return
		}
		sendJSON(w, r, 200, map[string]bool{"removed": true})
		return
	}
	item.mu.Lock()
	defer item.mu.Unlock()
	if chunk {
		offsets := r.Header.Values("X-Log-Offset")
		offset, err := strconv.ParseInt(r.Header.Get("X-Log-Offset"), 10, 64)
		if len(offsets) != 1 || err != nil || strconv.FormatInt(offset, 10) != offsets[0] || offset < 0 {
			fail(400, "需要有效的 X-Log-Offset")
			return
		}
		if item.meta.Complete || offset != item.meta.Offset {
			fail(409, "上传状态或块偏移不匹配")
			return
		}
		if len(body) == 0 || int64(len(body)) > item.meta.Size-offset {
			fail(400, "块长度必须为 1..1 MiB 且不能超过声明文件长度")
			return
		}
		f, err := os.OpenFile(item.path, os.O_WRONLY, 0)
		if err != nil {
			fail(500, "无法写入临时副本")
			return
		}
		n, writeErr := f.WriteAt(body, offset)
		if writeErr == nil && n != len(body) {
			writeErr = io.ErrShortWrite
		}
		if writeErr == nil {
			writeErr = r.Context().Err()
		}
		if writeErr != nil {
			// A failed/partial chunk never advances the committed offset. A retry
			// overwrites it. Finalization also checks the exact physical length.
			_ = f.Truncate(offset)
		}
		closeErr := f.Close()
		if writeErr != nil || closeErr != nil {
			fail(500, "块写入未提交，请重试相同偏移或移除导入")
			return
		}
		item.meta.Offset += int64(n)
	} else {
		if item.meta.Offset != item.meta.Size {
			fail(409, "尚未上传声明的全部字节")
			return
		}
		info, err := os.Stat(item.path)
		if err != nil || !info.Mode().IsRegular() || info.Size() != item.meta.Size {
			fail(409, "临时副本长度不匹配，请重新导入")
			return
		}
		item.meta.Complete = true
	}
	item.meta.ExpiresAt = time.Now().Add(importTTL)
	sendJSON(w, r, 200, item.meta)
}
