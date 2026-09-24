package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type FormatCounts struct {
	JSON    int64 `json:"json"`
	Compact int64 `json:"compact"`
	GORM    int64 `json:"gorm"`
}

type Status struct {
	SourceMode        string       `json:"source_mode"`
	InputState        string       `json:"input_state"`
	Formats           FormatCounts `json:"formats"`
	SQLObserved       int64        `json:"sql_observed"`
	WithDuration      int64        `json:"with_duration"`
	WithSource        int64        `json:"with_source"`
	WithTrace         int64        `json:"with_trace"`
	IncompleteRecords int64        `json:"incomplete_records"`
	Path              string       `json:"path"`
	Exists            bool         `json:"exists"`
	BytesRead         int64        `json:"bytes_read"`
	LinesSeen         int64        `json:"lines_seen"`
	IgnoredLines      int64        `json:"ignored_lines"`
	ParseErrors       int64        `json:"parse_errors"`
	Rotations         int64        `json:"rotations"`
	DroppedEvents     int64        `json:"dropped_events"`
	OversizeLines     int64        `json:"oversize_lines"`
	StartedAt         string       `json:"started_at"`
	LastRead          *string      `json:"last_read"`
	Capacity          int          `json:"capacity"`
	Error             string       `json:"error"`
}

type Snapshot struct {
	Status Status  `json:"status"`
	Events []Event `json:"events"`
}

type Monitor struct {
	mu           sync.RWMutex
	summaries    *sqlSummaryCollector
	live         liveStore
	livePrefix   []byte
	logs         logAccess
	initialBytes int64
	status       Status
	events       []Event
	raw          map[uint64]RawEvent
	nextID       uint64
	file         *os.File
	identity     os.FileInfo
	offset       int64
	boundary     []byte
	pending      []byte
	discard      bool
	initialized  bool
	closed       bool
	gormPending  map[string]any
}

func newMonitor(path string, capacity int, initialBytes int64) (*Monitor, error) {
	if capacity < 1 || capacity > 2000 || initialBytes < 0 {
		return nil, errors.New("capacity must be 1..2000; initial_bytes must be nonnegative")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	return &Monitor{
		initialBytes: initialBytes,
		status:       Status{Path: absolute, Capacity: capacity, StartedAt: now(), SourceMode: "file", InputState: "reading"},
		events:       make([]Event, 0, capacity), raw: make(map[uint64]RawEvent), nextID: 1,
	}, nil
}

func (m *Monitor) StatusSnapshot() Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	status := m.status
	if status.LastRead != nil {
		value := *status.LastRead
		status.LastRead = &value
	}
	return status
}

func (m *Monitor) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snapshot := Snapshot{Status: m.status, Events: make([]Event, len(m.events))}
	if snapshot.Status.LastRead != nil {
		value := *snapshot.Status.LastRead
		snapshot.Status.LastRead = &value
	}
	copy(snapshot.Events, m.events)
	for i := range snapshot.Events {
		event := &snapshot.Events[i]
		if event.ElapsedMS != nil {
			value := *event.ElapsedMS
			event.ElapsedMS = &value
		}
		if event.Rows != nil {
			value := *event.Rows
			event.Rows = &value
		}
	}
	return snapshot
}

func (m *Monitor) RawEvent(id uint64) (RawEvent, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	raw, found := m.raw[id]
	return raw, found
}

func (m *Monitor) Consume(chunk []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.consume(chunk)
	}
}

func (m *Monitor) consume(chunk []byte) {
	for {
		part, rest, newline := bytes.Cut(chunk, []byte{'\n'})
		if !m.discard {
			if len(m.pending)+len(part) > maxLine {
				m.livePrefix = append([]byte(nil), m.pending[:min(len(m.pending), 16<<10)]...)
				m.livePrefix = append(m.livePrefix, part[:min(len(part), (16<<10)-len(m.livePrefix))]...)
				m.pending = nil
				m.discard = true
				m.status.OversizeLines++
				m.resetGORM()
			} else {
				m.pending = append(m.pending, part...)
			}
		}
		if !newline {
			return
		}
		m.status.LinesSeen++
		if !m.discard {
			m.live.record(m.pending, false)
			m.parsePhysicalLine(m.pending)
		} else {
			m.live.record(m.livePrefix, true)
			m.resetGORM()
		}
		m.livePrefix = nil
		m.pending = nil
		m.discard = false
		chunk = rest
	}
}

func (m *Monitor) open() error {
	file, err := os.OpenFile(m.status.Path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() {
		file.Close()
		if err != nil {
			return err
		}
		return errors.New("not a regular file")
	}
	offset := int64(0)
	if !m.initialized && opened.Size() > m.initialBytes {
		offset = opened.Size() - m.initialBytes
	}
	discard := false
	if offset > 0 {
		previous := make([]byte, 1)
		n, err := file.ReadAt(previous, offset-1)
		if err != nil && err != io.EOF {
			file.Close()
			return err
		}
		discard = n == 0 || previous[0] != '\n'
	}
	boundary := make([]byte, min(int64(256), offset))
	n, err := file.ReadAt(boundary, offset-int64(len(boundary)))
	if err != nil && err != io.EOF {
		file.Close()
		return err
	}
	boundary = boundary[:n]
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		file.Close()
		return err
	}
	if m.file != nil {
		m.file.Close()
	}
	m.file, m.identity = file, opened
	m.resetGORM()
	m.pending, m.discard = nil, discard
	m.livePrefix = nil
	m.offset, m.boundary = offset, boundary
	if m.initialized {
		m.status.Rotations++
	}
	m.initialized = true
	return nil
}

func (m *Monitor) Poll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	if m.status.SourceMode != "file" {
		return
	}
	if err := m.poll(); err != nil {
		m.status.InputState = "error"
		if errors.Is(err, os.ErrNotExist) {
			m.status.Exists = false
			m.status.Error = "Log file is missing; retrying"
		} else {
			m.status.Error = "Cannot read log file; retrying"
		}
	} else {
		m.status.Error = ""
		m.status.InputState = "reading"
	}
}

func (m *Monitor) poll() error {
	info, err := os.Stat(m.status.Path)
	if err != nil {
		return err
	}
	m.status.Exists = true
	if !info.Mode().IsRegular() {
		return errors.New("not a regular file")
	}
	if m.file == nil || !os.SameFile(info, m.identity) {
		if err := m.open(); err != nil {
			return err
		}
	} else {
		boundary := make([]byte, len(m.boundary))
		n, err := m.file.ReadAt(boundary, m.offset-int64(len(boundary)))
		if err != nil && err != io.EOF {
			return err
		}
		if info.Size() < m.offset || !bytes.Equal(boundary[:n], m.boundary) {
			if _, err := m.file.Seek(0, io.SeekStart); err != nil {
				return err
			}
			m.offset, m.boundary = 0, nil
			m.resetGORM()
			m.pending, m.discard = nil, false
			m.livePrefix = nil
			m.status.Rotations++
		}
	}
	chunk := make([]byte, readChunk)
	for range 32 {
		n, err := m.file.Read(chunk)
		if n > 0 {
			m.offset += int64(n)
			m.boundary = append(m.boundary, chunk[:n]...)
			if len(m.boundary) > 256 {
				m.boundary = bytes.Clone(m.boundary[len(m.boundary)-256:])
			}
			m.status.BytesRead += int64(n)
			readAt := now()
			m.status.LastRead = &readAt
			m.consume(chunk[:n])
		}
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 || err == io.EOF {
			break
		}
	}
	return nil
}

func (m *Monitor) Run(ctx context.Context) {
	defer m.Close()
	for {
		if ctx.Err() != nil {
			return
		}
		m.Poll()
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (m *Monitor) Close() {
	m.logs.imports.close()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	if m.file != nil {
		m.file.Close()
		m.file = nil
	}
	m.resetGORM()
	m.pending, m.boundary, m.livePrefix = nil, nil, nil
}

// RunInput exclusively owns input. Nonblocking reads avoid a stranded reader
// goroutine and work with Darwin FIFOs, which Go excludes from its poller.
func (m *Monitor) RunInput(ctx context.Context, input *os.File) {
	defer input.Close()
	m.mu.Lock()
	m.status.SourceMode, m.status.Path, m.status.InputState = "stdin", "", "reading"
	m.mu.Unlock()
	info, err := input.Stat()
	if err != nil {
		m.finishInput(err)
		return
	}
	fd := int(input.Fd())
	if !info.Mode().IsRegular() {
		if err := syscall.SetNonblock(fd, true); err != nil {
			m.finishInput(err)
			return
		}
	}
	chunk := make([]byte, readChunk)
	for ctx.Err() == nil {
		n, err := syscall.Read(fd, chunk)
		if n > 0 {
			m.mu.Lock()
			if m.closed {
				m.mu.Unlock()
				return
			}
			m.status.BytesRead += int64(n)
			readAt := now()
			m.status.LastRead = &readAt
			m.consume(chunk[:n])
			m.mu.Unlock()
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			timer := time.NewTimer(50 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}
		if err == nil && n == 0 {
			err = io.EOF
		}
		if err != nil {
			m.finishInput(err)
			return
		}
	}
}

func (m *Monitor) finishInput(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	if errors.Is(err, io.EOF) {
		// Only a permanent stream EOF closes the last physical line.
		if len(m.pending) > 0 || m.discard {
			m.consume([]byte{'\n'})
		}
		m.status.InputState = "eof"
		m.status.Error = ""
	} else {
		if len(m.pending) > 0 {
			m.status.IncompleteRecords++
		}
		m.status.InputState = "error"
		m.status.Error = "Cannot read stdin; input stopped (use a pipe or regular file)"
	}
	m.resetGORM()
	m.pending, m.discard = nil, false
	m.livePrefix = nil
}
