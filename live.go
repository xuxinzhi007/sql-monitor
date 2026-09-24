package main

import (
	"crypto/sha256"
	"encoding/json"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	liveCapacity          = 2000
	liveByteLimit         = 8 * 1024 * 1024
	liveRawLimit          = 64 * 1024
	liveMessageLimit      = 64 * 1024
	liveMetadataLimit     = 256
	liveInputLimit        = 1024 * 1024
	liveRelatedPerRequest = 100
	liveRelatedLimit      = 2 * liveCapacity
)

// LiveSnapshot contains sensitive literal log text, including Source and Raw.
// Its caller must enforce the explicit raw-data consent/API policy before exposure.
// All counters except Retained describe observations over the store's lifetime;
// events, anomalies and requests describe only the currently retained evidence.
type LiveSnapshot struct {
	Stats     LiveStats     `json:"stats"`
	Events    []LiveEvent   `json:"events"`
	Anomalies []LiveAnomaly `json:"anomalies"`
	Requests  []LiveRequest `json:"requests"`
}

type LiveStats struct {
	Observed  uint64     `json:"observed"`
	Retained  int        `json:"retained"`
	Evicted   uint64     `json:"evicted"`
	Truncated uint64     `json:"truncated"`
	Unparsed  uint64     `json:"unparsed"`
	WithTrace uint64     `json:"with_trace"`
	Levels    LiveLevels `json:"levels"`
	Capacity  int        `json:"capacity"`
	ByteLimit int        `json:"byte_limit"`
}

type LiveLevels struct {
	Debug   uint64 `json:"DEBUG"`
	Info    uint64 `json:"INFO"`
	Warn    uint64 `json:"WARN"`
	Error   uint64 `json:"ERROR"`
	Fatal   uint64 `json:"FATAL"`
	Unknown uint64 `json:"UNKNOWN"`
}

type LiveEvent struct {
	ID         uint64   `json:"id"`
	Time       string   `json:"time"`
	ReceivedAt string   `json:"received_at"`
	Level      string   `json:"level"`
	Service    string   `json:"service"`
	Source     string   `json:"source"`
	TraceID    string   `json:"trace_id"`
	SpanID     string   `json:"span_id"`
	Message    string   `json:"message"`
	Raw        string   `json:"raw"`
	Kind       string   `json:"kind"`
	Operation  string   `json:"operation"`
	Boundary   string   `json:"boundary"`
	DurationMS *float64 `json:"duration_ms"`
	Truncated  bool     `json:"truncated"`
}

type LiveAnomaly struct {
	ID        uint64   `json:"id"`
	Level     string   `json:"level"`
	Source    string   `json:"source"`
	Message   string   `json:"message"`
	Count     uint64   `json:"count"`
	FirstTime string   `json:"first_time"`
	LastTime  string   `json:"last_time"`
	EventIDs  []uint64 `json:"event_ids"`
}

type LiveRequest struct {
	ID         uint64   `json:"id"`
	StartID    uint64   `json:"start_id"`
	EndID      uint64   `json:"end_id"`
	Operation  string   `json:"operation"`
	TraceID    string   `json:"trace_id"`
	SpanID     string   `json:"span_id"`
	State      string   `json:"state"`
	DurationMS *float64 `json:"duration_ms"`
	EventIDs   []uint64 `json:"event_ids"`
	RelatedIDs []uint64 `json:"related_ids"`
	SQLCount   int      `json:"sql_count"`
	Warnings   []string `json:"warnings"`
}

// Digests preserve exact grouping of complete messages/sources even when their
// display strings are capped. Incomplete physical records are never coalesced.
type liveAnomalyKey struct {
	level      string
	source     [32]byte
	message    [32]byte
	incomplete uint64
}

type liveEntry struct {
	event  LiveEvent
	key    liveAnomalyKey
	safe   bool
	failed bool
	bytes  int
}

// liveStore is independently synchronized, bounded, and usable as a zero value.
// As with sync.Mutex, it must not be copied after first use.
type liveStore struct {
	mu      sync.Mutex
	entries [liveCapacity]liveEntry
	head    int
	count   int
	bytes   int
	stats   LiveStats
}

func (s *liveStore) record(line []byte, truncated bool) {
	entry := liveParse(line, truncated)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.Observed++
	entry.event.ID = s.stats.Observed
	if !entry.safe {
		entry.key.incomplete = entry.event.ID
	}
	if entry.event.Truncated {
		s.stats.Truncated++
	}
	if entry.event.Kind == "unparsed" {
		s.stats.Unparsed++
	}
	if entry.event.TraceID != "" {
		s.stats.WithTrace++
	}
	switch entry.event.Level {
	case "DEBUG":
		s.stats.Levels.Debug++
	case "INFO":
		s.stats.Levels.Info++
	case "WARN":
		s.stats.Levels.Warn++
	case "ERROR":
		s.stats.Levels.Error++
	case "FATAL":
		s.stats.Levels.Fatal++
	default:
		s.stats.Levels.Unknown++
	}
	for s.count > 0 && (s.count == liveCapacity || s.bytes+entry.bytes > liveByteLimit) {
		s.bytes -= s.entries[s.head].bytes
		s.entries[s.head] = liveEntry{}
		s.head = (s.head + 1) % liveCapacity
		s.count--
		s.stats.Evicted++
	}
	s.entries[(s.head+s.count)%liveCapacity] = entry
	s.count++
	s.bytes += entry.bytes
}

func (s *liveStore) snapshot() LiveSnapshot {
	s.mu.Lock()
	entries := make([]liveEntry, s.count)
	for i := range entries {
		entries[i] = s.entries[(s.head+i)%liveCapacity]
		entries[i].event.DurationMS = liveCopyDuration(entries[i].event.DurationMS)
	}
	stats := s.stats
	stats.Retained, stats.Capacity, stats.ByteLimit = s.count, liveCapacity, liveByteLimit
	s.mu.Unlock()

	result := LiveSnapshot{Stats: stats, Events: make([]LiveEvent, len(entries))}
	for i := range entries {
		result.Events[i] = entries[i].event
	}
	result.Anomalies = liveAnomalies(entries)
	result.Requests = liveRequests(entries)
	return result
}

var (
	liveLeadingLevel = regexp.MustCompile(`^(?:\[(DEBUG|INFO|WARN|ERROR|FATAL)\]|(DEBUG|INFO|WARN|ERROR|FATAL))(?::[ \t]*|[ \t]+|$)`)
	liveStart        = regexp.MustCompile(`^\[([^\[\]\r\n]+)\] req=([^\r\n]*)$`)
	liveEnd          = regexp.MustCompile(`^\[([^\[\]\r\n]+)\] ([0-9][^ \t\r\n]*) resp=([^\r\n]*)$`)
)

func liveParse(line []byte, truncated bool) liveEntry {
	if len(line) > liveInputLimit {
		line, truncated = line[:liveInputLimit], true
	}
	text := strings.TrimRight(string(line), "\r\n")
	e := LiveEvent{ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano), Level: "UNKNOWN", Kind: "unparsed", Message: text, Raw: text}
	complete := !truncated && utf8.ValidString(text)
	var fields map[string]json.RawMessage
	if utf8.ValidString(text) && json.Unmarshal([]byte(text), &fields) == nil && fields != nil {
		e.Kind = "log"
		e.Time = liveField(fields, "ts", "time", "timestamp")
		e.Level = liveLevel(liveField(fields, "level"))
		e.Service = liveField(fields, "service.name")
		e.Source = liveField(fields, "caller", "source")
		e.TraceID = liveID(liveField(fields, "trace.id", "trace_id"), 32)
		e.SpanID = liveID(liveField(fields, "span.id", "span_id"), 16)
		e.Message = liveField(fields, "msg", "message")
		e.DurationMS = liveDuration(liveField(fields, "duration"))
		var explicit bool
		if json.Unmarshal(fields["truncated"], &explicit) == nil && explicit {
			truncated, complete = true, false
		}
		// Only classify SQL; never call the SQL parser or its raw-data paths.
		if sqlKind(e.Message) != "" {
			e.Kind = "sql"
			e.Source = liveField(fields, "source", "caller")
			e.DurationMS = liveDuration(liveField(fields, "elapsed", "duration"))
		}
	} else if match := liveLeadingLevel.FindStringSubmatch(text); match != nil {
		e.Level = match[1] + match[2]
		e.Message = text[len(match[0]):]
		e.Kind = "log"
	}
	entry := liveEntry{safe: complete}
	if complete {
		var boundaryDuration *float64
		e.Operation, e.Boundary, entry.failed, boundaryDuration = liveBoundary(e.Message)
		if e.Boundary != "" {
			e.Kind = "request"
			if boundaryDuration != nil {
				e.DurationMS = boundaryDuration
			}
		}
	}
	entry.key = liveAnomalyKey{level: e.Level, source: sha256.Sum256([]byte(e.Source)), message: sha256.Sum256([]byte(e.Message))}
	e.Truncated = truncated || !utf8.ValidString(text)
	for _, field := range []*string{&e.Time, &e.ReceivedAt, &e.Level, &e.Service, &e.Source, &e.TraceID, &e.SpanID, &e.Kind, &e.Operation, &e.Boundary} {
		*field = liveCap(*field, liveMetadataLimit, &e.Truncated)
	}
	e.Message = liveCap(e.Message, liveMessageLimit, &e.Truncated)
	e.Raw = liveCap(e.Raw, liveRawLimit, &e.Truncated)
	entry.event = e
	entry.bytes = liveEventBytes(e)
	return entry
}

func liveCap(s string, limit int, truncated *bool) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "\uFFFD")
		*truncated = true
	}
	if len(s) > limit {
		n := limit
		for n > 0 && !utf8.RuneStart(s[n]) {
			n--
		}
		s = s[:n]
		*truncated = true
	}
	// A short slice of a 1 MiB input must not keep the input's backing store.
	return strings.Clone(s)
}

func liveEventBytes(e LiveEvent) int {
	return len(e.Time) + len(e.ReceivedAt) + len(e.Level) + len(e.Service) + len(e.Source) + len(e.TraceID) + len(e.SpanID) + len(e.Message) + len(e.Raw) + len(e.Kind) + len(e.Operation) + len(e.Boundary)
}

func liveField(fields map[string]json.RawMessage, names ...string) string {
	for _, name := range names {
		v, exists := fields[name]
		if !exists {
			if parent, child, ok := strings.Cut(name, "."); ok {
				var nested map[string]json.RawMessage
				if json.Unmarshal(fields[parent], &nested) == nil {
					v = nested[child]
				}
			}
		}
		var text string
		if json.Unmarshal(v, &text) == nil && text != "" {
			return text
		}
		var number json.Number
		if len(v) > 0 && v[0] != '"' && json.Unmarshal(v, &number) == nil && number != "" {
			return string(number)
		}
	}
	return ""
}

func liveLevel(s string) string {
	switch s = strings.ToUpper(s); s {
	case "DEBUG", "INFO", "WARN", "ERROR", "FATAL":
		return s
	default:
		return "UNKNOWN"
	}
}

func liveID(s string, size int) string {
	if len(s) != size {
		return ""
	}
	nonzero := false
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return ""
		}
		nonzero = nonzero || c != '0'
	}
	if !nonzero {
		return ""
	}
	return strings.ToLower(s)
}

// Numeric durations are milliseconds; unit-bearing strings use Go durations.
func liveDuration(s string) *float64 {
	if n, err := strconv.ParseFloat(s, 64); err == nil && n >= 0 && !math.IsInf(n, 0) && !math.IsNaN(n) {
		return &n
	}
	if d, err := time.ParseDuration(s); err == nil && d >= 0 {
		n := float64(d) / float64(time.Millisecond)
		return &n
	}
	return nil
}

func liveCopyDuration(d *float64) *float64 {
	if d == nil {
		return nil
	}
	n := *d
	return &n
}

func liveBoundary(message string) (operation, boundary string, failed bool, duration *float64) {
	if m := liveStart.FindStringSubmatch(message); m != nil && liveOperation(m[1]) && (m[2] == "" || livePayload(m[2])) {
		return m[1], "start", false, nil
	}
	if m := liveEnd.FindStringSubmatch(message); m != nil && liveOperation(m[1]) {
		body, errText := liveResponse(m[3])
		if (body == "" || livePayload(body)) && (errText == "" || livePayload(errText)) {
			d, err := time.ParseDuration(m[2])
			if err == nil && d >= 0 {
				ms := float64(d) / float64(time.Millisecond)
				failed = errText != "" && errText != "<nil>" && errText != "nil" && errText != "null"
				return m[1], "end", failed, &ms
			}
		}
	}
	return "", "", false, nil
}

func liveOperation(s string) bool {
	return s != "" && len(s) <= liveMetadataLimit && strings.TrimSpace(s) == s
}

// Walk complete balanced payloads (JSON, Go struct/map formatting, or plain
// text). Only a top-level error suffix is evidence of failure, not "err=" in
// quoted response data. This is not an application payload/schema parser.
func liveWalk(s string, visit func(int) bool) bool {
	var stack []byte
	var quote byte
	escaped := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if escaped {
				escaped = false
			} else if c == '\\' && quote != '`' {
				escaped = true
			} else if c == quote {
				quote = 0
			}
			continue
		}
		if len(stack) == 0 && visit != nil && !visit(i) {
			return true
		}
		switch c {
		case '"', '`':
			quote = c
		case '{':
			stack = append(stack, '}')
		case '[':
			stack = append(stack, ']')
		case '(':
			stack = append(stack, ')')
		case '}', ']', ')':
			if len(stack) == 0 || stack[len(stack)-1] != c {
				return false
			}
			stack = stack[:len(stack)-1]
		}
	}
	return len(stack) == 0 && quote == 0
}

func livePayload(s string) bool {
	return strings.TrimSpace(s) != "" && liveWalk(s, nil)
}

func liveResponse(s string) (body, errText string) {
	body = s
	liveWalk(s, func(i int) bool {
		if strings.HasPrefix(s[i:], " err=") {
			body, errText = s[:i], s[i+5:]
			if errText == "" {
				body = "[" // Reject an incomplete error suffix.
			}
			return false
		}
		if strings.HasPrefix(s[i:], " [err=") && strings.HasSuffix(s, "]") {
			body, errText = s[:i], s[i+6:len(s)-1]
			if errText == "" {
				body = "["
			}
			return false
		}
		return true
	})
	return body, errText
}

func liveAnomalies(entries []liveEntry) []LiveAnomaly {
	result := make([]LiveAnomaly, 0)
	indices := make(map[liveAnomalyKey]int)
	for _, entry := range entries {
		e := entry.event
		if e.Level != "WARN" && e.Level != "ERROR" && e.Level != "FATAL" {
			continue
		}
		stamp := e.Time
		if stamp == "" {
			stamp = e.ReceivedAt
		}
		i, exists := indices[entry.key]
		if !exists {
			i = len(result)
			indices[entry.key] = i
			result = append(result, LiveAnomaly{ID: e.ID, Level: e.Level, Source: e.Source, Message: e.Message, FirstTime: stamp, EventIDs: make([]uint64, 0)})
		}
		a := &result[i]
		a.Count++
		a.LastTime = stamp
		a.EventIDs = append(a.EventIDs, e.ID)
	}
	return result
}

type liveIdentity struct{ trace, span, operation string }
type liveSpan struct{ trace, span string }

func liveRequests(entries []liveEntry) []LiveRequest {
	result := make([]LiveRequest, 0)
	local := liveLocalPairs(entries)
	groups := make(map[liveIdentity][]int)
	spanOperations := make(map[liveSpan]map[string]bool)
	for i, entry := range entries {
		e := entry.event
		if !entry.safe || e.Boundary == "" || e.TraceID == "" || e.SpanID == "" {
			continue
		}
		key := liveIdentity{e.TraceID, e.SpanID, e.Operation}
		groups[key] = append(groups[key], i)
		span := liveSpan{e.TraceID, e.SpanID}
		if spanOperations[span] == nil {
			spanOperations[span] = make(map[string]bool)
		}
		spanOperations[span][e.Operation] = true
	}
	// Each row is emitted in earliest-boundary order, never map iteration order.
	for i, entry := range entries {
		e := entry.event
		if !entry.safe || e.Boundary == "" {
			continue
		}
		r := LiveRequest{ID: e.ID, Operation: e.Operation, TraceID: e.TraceID, SpanID: e.SpanID, DurationMS: liveCopyDuration(e.DurationMS), EventIDs: []uint64{e.ID}, RelatedIDs: make([]uint64, 0), Warnings: make([]string, 0)}
		if e.Boundary == "start" {
			r.StartID = e.ID
		} else {
			r.EndID = e.ID
		}
		g := groups[liveIdentity{e.TraceID, e.SpanID, e.Operation}]
		switch {
		case e.TraceID == "" && e.SpanID == "":
			liveLocalRequest(&r, entries, i, local)
		case e.TraceID == "" || e.SpanID == "":
			r.State = "unlinked"
			r.Warnings = append(r.Warnings, "Missing valid trace and span identity; boundaries are not paired.")
		case len(spanOperations[liveSpan{e.TraceID, e.SpanID}]) > 1 || len(g) > 2:
			r.State = "ambiguous"
			r.Warnings = append(r.Warnings, "Multiple request boundaries share an identity; no SQL attribution.")
		case len(g) == 2:
			start, end := entries[g[0]].event, entries[g[1]].event
			if start.Boundary != "start" || end.Boundary != "end" {
				r.State = "ambiguous"
				r.Warnings = append(r.Warnings, "Boundary order or multiplicity is ambiguous; no SQL attribution.")
				break
			}
			if i != g[0] {
				continue
			}
			r.StartID, r.EndID = start.ID, end.ID
			r.DurationMS = liveCopyDuration(end.DurationMS)
			r.State = "completed"
			if entries[g[1]].failed {
				r.State = "failed"
			}
			r.EventIDs = make([]uint64, 0)
			for _, candidate := range entries[g[0] : g[1]+1] {
				c := candidate.event
				if candidate.safe && c.TraceID == e.TraceID && c.SpanID == e.SpanID {
					r.EventIDs = append(r.EventIDs, c.ID)
					if c.Kind == "sql" {
						r.SQLCount++
					}
				}
			}
		default:
			if e.Boundary == "start" {
				r.State = "missing_end"
			} else {
				r.State = "missing_start"
			}
			r.Warnings = append(r.Warnings, "Missing boundary evidence in retained events; not evidence of a stuck request.")
		}
		if r.State == "skip" {
			continue
		}
		result = append(result, r)
	}
	// Attribution lists total at most len(entries). Related evidence has both a
	// per-row and a global cap, avoiding quadratic output for a hot trace.
	budget := liveRelatedLimit
	for i := range result {
		r := &result[i]
		if r.TraceID == "" {
			continue
		}
		included := make(map[uint64]bool, len(r.EventIDs))
		for _, id := range r.EventIDs {
			included[id] = true
		}
		for _, entry := range entries {
			e := entry.event
			if !entry.safe || e.TraceID != r.TraceID || included[e.ID] {
				continue
			}
			if len(r.RelatedIDs) == liveRelatedPerRequest || budget == 0 {
				r.Warnings = append(r.Warnings, "Related evidence capped.")
				break
			}
			r.RelatedIDs = append(r.RelatedIDs, e.ID)
			budget--
		}
		if len(r.RelatedIDs) > 0 {
			r.Warnings = append(r.Warnings, "Same trace evidence, not certain same request.")
		}
	}
	return result
}

// liveLocalPairs pairs request boundaries that have no trace and no span.
// A local data-service run logs [operation] req, then SQL Query, then
// [operation] duration resp, often with empty trace.id and span.id.
// One open call owns the SQL between its start and end. Overlap is unpaired.
func liveLocalPairs(entries []liveEntry) map[int]int {
	pairs := make(map[int]int)
	var open []int
	clear := func(extra int) {
		for _, i := range open {
			pairs[i] = -1
		}
		if extra >= 0 {
			pairs[extra] = -1
		}
		open = nil
	}
	for i, entry := range entries {
		e := entry.event
		if !entry.safe || e.Boundary == "" || e.TraceID != "" || e.SpanID != "" {
			continue
		}
		if e.Boundary == "start" {
			if len(open) > 0 {
				clear(i)
				continue
			}
			open = []int{i}
			continue
		}
		if len(open) != 1 || entries[open[0]].event.Operation != e.Operation {
			clear(i)
			continue
		}
		pairs[open[0]] = i
		pairs[i] = open[0]
		open = nil
	}
	for _, i := range open {
		if _, seen := pairs[i]; !seen {
			pairs[i] = -2
		}
	}
	return pairs
}

func liveServiceModule(operation string) string {
	i := strings.LastIndex(operation, "Service/")
	if i < 0 {
		i = strings.LastIndex(operation, "Service")
	}
	if i <= 0 {
		return ""
	}
	name := operation[:i]
	if j := strings.LastIndexAny(name, "./"); j >= 0 {
		name = name[j+1:]
	}
	return strings.ToLower(name)
}

func liveSourceModule(source, module string) bool {
	if module == "" || source == "" {
		return false
	}
	file := source
	if i := strings.LastIndex(file, ":"); i >= 0 {
		file = file[:i]
	}
	base := strings.ToLower(file)
	if i := strings.LastIndex(base, "/"); i >= 0 {
		base = base[i+1:]
	}
	if !strings.HasPrefix(base, module) || len(base) == len(module) {
		return false
	}
	next := base[len(module)]
	return next == '.' || next == '_'
}

func liveLocalRequest(r *LiveRequest, entries []liveEntry, i int, pairs map[int]int) {
	other, ok := pairs[i]
	switch {
	case !ok:
		r.State = "missing_start"
		r.Warnings = append(r.Warnings, "No local request start before this end.")
	case other == -1:
		r.State = "ambiguous"
		r.Warnings = append(r.Warnings, "Overlapping local requests; SQL is not attributed.")
	case other < 0:
		r.State = "missing_end"
		r.Warnings = append(r.Warnings, "Local request has no end in the retained window.")
	case i > other:
		r.State = "skip"
	default:
		end := entries[other].event
		r.StartID, r.EndID = r.ID, end.ID
		r.DurationMS = liveCopyDuration(end.DurationMS)
		r.State = "completed"
		if entries[other].failed {
			r.State = "failed"
		}
		r.EventIDs = []uint64{r.StartID}
		module := liveServiceModule(r.Operation)
		for _, candidate := range entries[i+1 : other] {
			c := candidate.event
			if !candidate.safe || c.Kind != "sql" || c.TraceID != "" || c.SpanID != "" {
				continue
			}
			if !liveSourceModule(c.Source, module) {
				continue
			}
			r.EventIDs = append(r.EventIDs, c.ID)
			r.SQLCount++
		}
		r.EventIDs = append(r.EventIDs, r.EndID)
	}
}
