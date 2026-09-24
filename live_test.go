package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"
)

const (
	liveTestTrace = "1234567890abcdef1234567890abcdef"
	liveTestSpanA = "1234567890abcdef"
	liveTestSpanB = "fedcba0987654321"
)

func liveTestJSON(t *testing.T, s *liveStore, fields map[string]any) {
	t.Helper()
	line, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	s.record(line, false)
}

func liveTestMessage(t *testing.T, s *liveStore, message, trace, span string) {
	t.Helper()
	liveTestJSON(t, s, map[string]any{"level": "INFO", "msg": message, "trace.id": trace, "span.id": span})
}

func TestLiveSQLMetadata(t *testing.T) {
	e := liveParse([]byte(`{"level":"DEBUG","msg":"SQL Query","source":"query.go:42","caller":"logger.go:10","elapsed":250.5}`), false).event
	if e.Kind != "sql" || e.Source != "query.go:42" || e.DurationMS == nil || *e.DurationMS != 250.5 {
		t.Fatalf("SQL metadata: %+v", e)
	}
}

func TestLiveZeroValueAndJSONContract(t *testing.T) {
	var s liveStore
	x := s.snapshot()
	if x.Stats.Capacity != 2000 || x.Stats.ByteLimit != 8388608 || x.Stats.Observed != 0 || x.Stats.Retained != 0 {
		t.Fatalf("zero snapshot: %+v", x)
	}
	encoded, err := json.Marshal(x)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"events", "anomalies", "requests"} {
		if string(object[name]) != "[]" {
			t.Fatalf("%s must be [], got %s", name, object[name])
		}
	}
	var stats map[string]json.RawMessage
	if err := json.Unmarshal(object["stats"], &stats); err != nil {
		t.Fatal(err)
	}
	if len(stats) != 9 {
		t.Fatalf("unexpected stats keys: %s", object["stats"])
	}
	for _, name := range []string{"observed", "retained", "evicted", "truncated", "unparsed", "with_trace", "levels", "capacity", "byte_limit"} {
		if _, exists := stats[name]; !exists {
			t.Errorf("missing stats field %s", name)
		}
	}
	var levels map[string]uint64
	if err := json.Unmarshal(stats["levels"], &levels); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(levels, map[string]uint64{"DEBUG": 0, "INFO": 0, "WARN": 0, "ERROR": 0, "FATAL": 0, "UNKNOWN": 0}) {
		t.Fatalf("levels: %v", levels)
	}
	liveTestMessage(t, &s, "[GetTask] req={}", "", "")
	e := s.snapshot().Events[0]
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	// Unmarshal into a fresh map: JSON decoding does not clear existing keys.
	object = nil
	if err := json.Unmarshal(b, &object); err != nil {
		t.Fatal(err)
	}
	if len(object) != 15 {
		t.Fatalf("event keys: %s", b)
	}
	for _, name := range []string{"id", "time", "received_at", "level", "service", "source", "trace_id", "span_id", "message", "raw", "kind", "operation", "boundary", "duration_ms", "truncated"} {
		if _, exists := object[name]; !exists {
			t.Errorf("missing event field %s", name)
		}
	}
	r := s.snapshot().Requests[0]
	b, err = json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"related_ids":[]`) || strings.Contains(string(b), `"warnings":null`) {
		t.Fatalf("request arrays: %s", b)
	}
}

func TestLiveJSONPlaintextAndLiteralText(t *testing.T) {
	var s liveStore
	literal := `<script>alert("x")</script><img src=x onerror=alert(1)>`
	liveTestJSON(t, &s, map[string]any{
		"ts": "2026-01-01T00:00:00Z", "level": "warn", "service": map[string]any{"name": "data-service"},
		"caller": "service/task.go:42", "trace": map[string]any{"id": liveTestTrace}, "span": map[string]any{"id": liveTestSpanA},
		"msg": literal, "duration": "0ms",
	})
	liveTestJSON(t, &s, map[string]any{"timestamp": 123, "level": "INFO", "service.name": "worker", "source": "query.go:9", "trace_id": liveTestTrace, "span_id": liveTestSpanB, "message": "SQL Query", "duration": 1.25})
	for _, line := range []string{"plain words mentioning WARN and ERROR", "DEBUG repeated", "[ERROR] failure", "WARN: alert", `{"msg":`, "INFOGRAPHIC ERROR", "", "[GetTask] req={}"} {
		s.record([]byte(line), false)
	}
	x := s.snapshot()
	e := x.Events[0]
	if e.Level != "WARN" || e.Service != "data-service" || e.Source != "service/task.go:42" || e.TraceID != liveTestTrace || e.SpanID != liveTestSpanA || e.Time != "2026-01-01T00:00:00Z" || e.Message != literal || e.Kind != "log" || e.DurationMS == nil || *e.DurationMS != 0 || e.ReceivedAt == "" {
		t.Fatalf("JSON event: %+v", e)
	}
	if e = x.Events[1]; e.Time != "123" || e.Kind != "sql" || e.Source != "query.go:9" || e.Service != "worker" || e.DurationMS == nil || *e.DurationMS != 1.25 {
		t.Fatalf("alternate fields: %+v", e)
	}
	wantKinds := []string{"unparsed", "log", "log", "log", "unparsed", "unparsed", "unparsed", "request"}
	for i, want := range wantKinds {
		if x.Events[i+2].Kind != want {
			t.Errorf("event %d kind = %q, want %q", i+2, x.Events[i+2].Kind, want)
		}
	}
	if x.Stats.Observed != 10 || x.Stats.Unparsed != 4 || x.Stats.WithTrace != 2 || len(x.Anomalies) != 3 {
		t.Fatalf("stats or anomalies: %+v", x)
	}
	b, err := json.Marshal(x)
	if err != nil {
		t.Fatal(err)
	}
	var decoded LiveSnapshot
	if err := json.Unmarshal(b, &decoded); err != nil || decoded.Events[0].Message != literal {
		t.Fatalf("literal XSS text changed: %v", err)
	}
}

func TestLiveCountLimitAndIndependentCounters(t *testing.T) {
	var s liveStore
	s.record([]byte("unparsed"), true)
	s.record([]byte("ERROR old anomaly"), false)
	liveTestMessage(t, &s, "[GetTask] req={}", liveTestTrace, liveTestSpanA)
	for i := 0; i < liveCapacity; i++ {
		s.record([]byte("DEBUG repeated"), false)
	}
	x := s.snapshot()
	if x.Stats.Observed != 2003 || x.Stats.Retained != 2000 || x.Stats.Evicted != 3 || x.Stats.Truncated != 1 || x.Stats.Unparsed != 1 || x.Stats.WithTrace != 1 {
		t.Fatalf("counters: %+v", x.Stats)
	}
	if x.Stats.Levels.Debug != 2000 || x.Stats.Levels.Error != 1 || x.Stats.Levels.Info != 1 || x.Stats.Levels.Unknown != 1 {
		t.Fatalf("level counters: %+v", x.Stats.Levels)
	}
	if x.Events[0].ID != 4 || x.Events[len(x.Events)-1].ID != 2003 || len(x.Anomalies) != 0 || len(x.Requests) != 0 {
		t.Fatalf("evicted evidence survived or ordering failed")
	}
}

func TestLiveByteLimitAndCaps(t *testing.T) {
	var s liveStore
	message := strings.Repeat("界", 9000)
	metadata := strings.Repeat("界", 200)
	for i := 0; i < 500; i++ {
		liveTestJSON(t, &s, map[string]any{"msg": message, "time": metadata, "source": metadata, "service.name": metadata, "level": "INFO"})
	}
	x := s.snapshot()
	bytes := 0
	for i, e := range x.Events {
		bytes += liveEventBytes(e)
		if !e.Truncated || len(e.Raw) > liveRawLimit || len(e.Message) > liveMessageLimit || len(e.Source) > liveMetadataLimit || len(e.Time) > liveMetadataLimit || len(e.Service) > liveMetadataLimit || !utf8.ValidString(e.Raw) || !utf8.ValidString(e.Message) || !utf8.ValidString(e.Source) {
			t.Fatalf("caps: %+v", e)
		}
		if i > 0 && e.ID != x.Events[i-1].ID+1 {
			t.Fatal("events are not oldest-first")
		}
	}
	if bytes > liveByteLimit || bytes != s.bytes || len(x.Events) >= 500 || len(x.Events) == 0 || bytes+liveEventBytes(x.Events[0]) <= liveByteLimit {
		t.Fatalf("byte accounting: bytes=%d stored=%d retained=%d", bytes, s.bytes, len(x.Events))
	}
	if x.Stats.Observed != 500 || x.Stats.Truncated != 500 || x.Stats.Unparsed != 0 || x.Stats.Levels.Info != 500 || x.Stats.Evicted+uint64(x.Stats.Retained) != 500 {
		t.Fatalf("byte eviction counters: %+v", x.Stats)
	}
}

func TestLiveParseBeforeDisplayCapsButNotIncompleteEvidence(t *testing.T) {
	var s liveStore
	payload := strings.Repeat("x", liveMessageLimit)
	liveTestMessage(t, &s, `[GetTask] req={"payload":"`+payload+`"}`, liveTestTrace, liveTestSpanA)
	liveTestMessage(t, &s, "[GetTask] 0ms resp={}", liveTestTrace, liveTestSpanA)
	x := s.snapshot()
	if !x.Events[0].Truncated || x.Events[0].Boundary != "start" || x.Events[0].TraceID != liveTestTrace || len(x.Requests) != 1 || x.Requests[0].State != "completed" {
		t.Fatalf("full JSON was not parsed before caps: %+v", x.Requests)
	}
	for _, line := range []string{
		`{"msg":"[GetTask] req={}","trace.id":"` + liveTestTrace + `","span.id":"` + liveTestSpanA + `"}`,
		`{"msg":"[GetTask] req={}","trace.id":"` + liveTestTrace,
		`[GetTask] req={}`,
	} {
		s.record([]byte(line), true)
	}
	liveTestJSON(t, &s, map[string]any{"msg": "SQL Query", "trace.id": liveTestTrace, "span.id": liveTestSpanA, "truncated": true})
	x = s.snapshot()
	if len(x.Requests) != 1 || len(x.Requests[0].RelatedIDs) != 0 {
		t.Fatalf("incomplete records supplied associations: %+v", x.Requests)
	}
	for _, e := range x.Events[2:] {
		if e.Boundary != "" || !e.Truncated {
			t.Fatalf("incomplete boundary: %+v", e)
		}
	}
	var oversized liveStore
	oversized.record([]byte("[GetTask] req="+strings.Repeat("x", liveInputLimit)), false)
	if e := oversized.snapshot().Events[0]; e.Boundary != "" || !e.Truncated {
		t.Fatalf("oversize guard: %+v", e)
	}
}

func TestLiveIncompleteSQLIsNotAttributed(t *testing.T) {
	var s liveStore
	liveTestMessage(t, &s, "[GetTask] req={}", liveTestTrace, liveTestSpanA)
	line := []byte(`{"msg":"SQL Query","trace.id":"` + liveTestTrace + `","span.id":"` + liveTestSpanA + `"}`)
	s.record(line, true)
	liveTestMessage(t, &s, "[GetTask] 0ms resp={}", liveTestTrace, liveTestSpanA)
	r := s.snapshot().Requests[0]
	if r.State != "completed" || r.SQLCount != 0 || !reflect.DeepEqual(r.EventIDs, []uint64{1, 3}) || len(r.RelatedIDs) != 0 {
		t.Fatalf("incomplete SQL supplied attribution: %+v", r)
	}
}

func TestLiveDurations(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  float64
	}{
		{"0", 0}, {"0ms", 0}, {"1.5", 1.5}, {"1.5ms", 1.5}, {"2s", 2000}, {"250us", 0.25},
	} {
		if d := liveDuration(tt.input); d == nil || *d != tt.want {
			t.Errorf("duration %q = %v, want %v", tt.input, d, tt.want)
		}
	}
	for _, input := range []string{"", "unknown", "NaN", "Inf", "1e999", "-1", "-1ms"} {
		if d := liveDuration(input); d != nil {
			t.Errorf("invalid duration %q = %v", input, *d)
		}
	}
}

func TestLiveRequestLoggingBoundaries(t *testing.T) {
	tests := []struct {
		message  string
		boundary string
		failed   bool
		duration float64
	}{
		{`[TaskService.GetTask] req={"id":42}`, "start", false, 0},
		{`[TaskService.GetTask] 123ms resp={"id":42}`, "end", false, 123},
		{`[TaskService.GetTask] 0ms resp=<nil> err=database unavailable`, "end", true, 0},
		{`[TaskService.GetTask] 1.25ms resp={} [err=database unavailable]`, "end", true, 1.25},
		{`[TaskService.GetTask] 2ms resp={} err=<nil>`, "end", false, 2},
		{`[TaskService.GetTask] 2ms resp={"text":"contains err=not a failure"}`, "end", false, 2},
		{`[TaskService.GetTask] req=&{ID:42 Tags:map[a:b]}`, "start", false, 0},
		{`prefix [GetTask] req={}`, "", false, 0},
		{`[GetTask] req=`, "start", false, 0},
		{`[GetTask] req={"id":`, "", false, 0},
		{`[GetTask] 12ms resp=`, "end", false, 12},
		{`[GetTask] 0s resp= err=failed`, "end", true, 0},
		{`[GetTask] 250µs resp={}`, "end", false, 0.25},
		{`[GetTask] 1m2.3s resp={}`, "end", false, 62300},
		{`[GetTask] 12 resp={}`, "", false, 0},
		{`[GetTask] 12ms resp={} err=`, "", false, 0},
		{`[GetTask] 12ms resp={} [err=]`, "", false, 0},
		{`[GetTask] took 12ms resp={}`, "", false, 0},
		{`[GetTask] -1ms resp={}`, "", false, 0},
		{`[GetTask] 1s resp={}`, "end", false, 1000},
		{"[GetTask] req={}\nextra", "", false, 0},
		{`[] req={}`, "", false, 0},
		{`[ GetTask] req={}`, "", false, 0},
		{"[" + strings.Repeat("a", 257) + "] req={}", "", false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.message, func(t *testing.T) {
			op, boundary, failed, d := liveBoundary(tt.message)
			if boundary != tt.boundary || failed != tt.failed || (boundary == "" && op != "") || (boundary == "end" && (d == nil || *d != tt.duration)) {
				t.Fatalf("got op=%q boundary=%q failed=%v duration=%v", op, boundary, failed, d)
			}
		})
	}
}

func TestLiveLocalRequestWithoutTrace(t *testing.T) {
	var s liveStore
	liveTestMessage(t, &s, "[/api.tag.v1.TagService/ListTags] req={}", "", "")
	liveTestJSON(t, &s, map[string]any{"level": "DEBUG", "msg": "SQL Query", "sql": "SELECT id FROM tag", "elapsed": "3ms", "rows": 2, "source": "internal/data/tag.go:10"})
	liveTestJSON(t, &s, map[string]any{"level": "DEBUG", "msg": "SQL Query", "sql": "UPDATE tb_episode SET status=1", "elapsed": "1ms", "rows": 1, "source": "internal/data/episode.go:20"})
	liveTestMessage(t, &s, "Lock already held", "", "")
	liveTestMessage(t, &s, "[/api.tag.v1.TagService/ListTags] 1440.02ms resp={}", "", "")
	x := s.snapshot()
	if len(x.Requests) != 1 {
		t.Fatalf("requests: %+v", x.Requests)
	}
	r := x.Requests[0]
	if r.State != "completed" || r.SQLCount != 1 || r.DurationMS == nil || *r.DurationMS != 1440.02 || !reflect.DeepEqual(r.EventIDs, []uint64{1, 2, 5}) || len(r.RelatedIDs) != 0 {
		t.Fatalf("local request: %+v", r)
	}
	var overlap liveStore
	for _, message := range []string{"[A] req={}", "[B] req={}", "SQL Query", "[B] 1ms resp={}", "[A] 2ms resp={}"} {
		liveTestMessage(t, &overlap, message, "", "")
	}
	for _, r := range overlap.snapshot().Requests {
		if r.SQLCount != 0 {
			t.Fatalf("overlapping local SQL attributed: %+v", r)
		}
	}
}

func TestLiveRequestsSameTraceDifferentSpans(t *testing.T) {
	var s liveStore
	liveTestMessage(t, &s, "SQL Query", liveTestTrace, liveTestSpanA) // outside either request
	liveTestMessage(t, &s, "[GetTask] req={}", liveTestTrace, liveTestSpanA)
	liveTestMessage(t, &s, "[GetTask] req={}", liveTestTrace, liveTestSpanB)
	liveTestMessage(t, &s, "SQL Query", liveTestTrace, liveTestSpanA)
	liveTestMessage(t, &s, "Slow SQL", liveTestTrace, liveTestSpanB)
	liveTestMessage(t, &s, "SQL Query", liveTestTrace, "") // trace-only is not attribution
	liveTestMessage(t, &s, "[GetTask] 0ms resp={}", liveTestTrace, liveTestSpanA)
	liveTestMessage(t, &s, "[GetTask] 12ms resp=<nil> err=failed", liveTestTrace, liveTestSpanB)
	liveTestMessage(t, &s, "SQL Query", liveTestTrace, liveTestSpanA)
	x := s.snapshot()
	if len(x.Requests) != 2 {
		t.Fatalf("requests: %+v", x.Requests)
	}
	a, b := x.Requests[0], x.Requests[1]
	if a.ID != 2 || a.StartID != 2 || a.EndID != 7 || a.State != "completed" || a.DurationMS == nil || *a.DurationMS != 0 || a.SQLCount != 1 || !reflect.DeepEqual(a.EventIDs, []uint64{2, 4, 7}) {
		t.Fatalf("span A: %+v", a)
	}
	if b.State != "failed" || b.SQLCount != 1 || b.DurationMS == nil || *b.DurationMS != 12 || !reflect.DeepEqual(b.EventIDs, []uint64{3, 5, 8}) {
		t.Fatalf("span B: %+v", b)
	}
	if !reflect.DeepEqual(a.RelatedIDs, []uint64{1, 3, 5, 6, 8, 9}) || len(a.Warnings) == 0 {
		t.Fatalf("same-trace evidence: %+v", a)
	}
}

func TestLiveInvalidIDsAndNoTraceNonPairing(t *testing.T) {
	for _, ids := range [][2]string{
		{liveTestTrace, ""}, {strings.Repeat("0", 32), liveTestSpanA},
		{liveTestTrace, strings.Repeat("0", 16)}, {"short", liveTestSpanA},
		{strings.Repeat("g", 32), liveTestSpanA}, {liveTestTrace, strings.Repeat("z", 16)},
	} {
		var s liveStore
		liveTestMessage(t, &s, "[GetTask] req={}", ids[0], ids[1])
		liveTestMessage(t, &s, "SQL Query", ids[0], ids[1])
		liveTestMessage(t, &s, "[GetTask] 0ms resp={}", ids[0], ids[1])
		x := s.snapshot()
		if len(x.Requests) != 2 {
			t.Fatalf("invalid identity %v paired: %+v", ids, x.Requests)
		}
		for _, r := range x.Requests {
			if r.State != "unlinked" || r.SQLCount != 0 || len(r.EventIDs) != 1 || (r.StartID != 0 && r.EndID != 0) {
				t.Fatalf("invalid identity %v: %+v", ids, r)
			}
		}
	}
	var s liveStore
	liveTestMessage(t, &s, "[GetTask] req={}", liveTestTrace, liveTestSpanA)
	liveTestMessage(t, &s, "[GetTask] 1ms resp={}", strings.ToUpper(liveTestTrace), liveTestSpanA)
	if requests := s.snapshot().Requests; len(requests) != 1 || requests[0].State != "completed" {
		t.Fatalf("hex IDs were not normalized: %+v", requests)
	}
}

func TestLiveAmbiguousAndMissingBoundaries(t *testing.T) {
	for _, messages := range [][]string{
		{"[GetTask] req={}", "[GetTask] req={}", "SQL Query", "[GetTask] 1ms resp={}", "[GetTask] 2ms resp={}"},
		{"[GetTask] 1ms resp={}", "[GetTask] req={}"},
		{"[GetTask] req={}", "[Other] req={}", "SQL Query", "[Other] 1ms resp={}", "[GetTask] 1ms resp={}"},
		{"[GetTask] req={}", "[GetTask] req={}"},
		{"[GetTask] 1ms resp={}", "[GetTask] 2ms resp={}"},
	} {
		var s liveStore
		for _, message := range messages {
			liveTestMessage(t, &s, message, liveTestTrace, liveTestSpanA)
		}
		for _, r := range s.snapshot().Requests {
			if r.State != "ambiguous" || r.SQLCount != 0 || len(r.EventIDs) != 1 || len(r.Warnings) == 0 {
				t.Fatalf("overlap/multiplicity attributed: %+v", r)
			}
		}
	}
	var s liveStore
	liveTestMessage(t, &s, "[GetTask] req={}", liveTestTrace, liveTestSpanA)
	if r := s.snapshot().Requests[0]; r.State != "missing_end" || r.EndID != 0 || r.DurationMS != nil {
		t.Fatalf("missing end: %+v", r)
	}
	liveTestMessage(t, &s, "[GetTask] 0ms resp={}", liveTestTrace, liveTestSpanA)
	for i := 0; i < liveCapacity-1; i++ {
		s.record([]byte("INFO filler"), false)
	}
	if r := s.snapshot().Requests[0]; r.State != "missing_start" || r.StartID != 0 || r.DurationMS == nil || *r.DurationMS != 0 || !strings.Contains(strings.Join(r.Warnings, " "), "Missing boundary evidence") {
		t.Fatalf("retained-only request: %+v", r)
	}
}

func TestLiveAnomaliesExactRetainedGrouping(t *testing.T) {
	var s liveStore
	for _, f := range []struct{ level, source, message, stamp string }{
		{"WARN", "a", "failed id=1", "first"}, {"WARN", "a", "failed id=1", "last"},
		{"WARN", "a", "failed id=2", "other"}, {"WARN", "b", "failed id=1", "other"},
		{"ERROR", "a", "failed id=1", "other"}, {"FATAL", "a", "fatal", "other"},
		{"DEBUG", "a", "repeat", "other"}, {"DEBUG", "a", "repeat", "other"},
	} {
		liveTestJSON(t, &s, map[string]any{"level": f.level, "source": f.source, "msg": f.message, "time": f.stamp})
	}
	x := s.snapshot()
	if len(x.Anomalies) != 5 || x.Anomalies[0].Count != 2 || x.Anomalies[0].ID != 1 || x.Anomalies[0].FirstTime != "first" || x.Anomalies[0].LastTime != "last" || !reflect.DeepEqual(x.Anomalies[0].EventIDs, []uint64{1, 2}) {
		t.Fatalf("exact anomaly grouping: %+v", x.Anomalies)
	}
	// Display truncation must not merge distinct full messages or sources.
	for _, suffix := range []string{"a", "b", "a"} {
		liveTestJSON(t, &s, map[string]any{"level": "ERROR", "msg": strings.Repeat("x", liveMessageLimit) + suffix})
		liveTestJSON(t, &s, map[string]any{"level": "ERROR", "msg": "same", "source": strings.Repeat("s", liveMetadataLimit) + suffix})
	}
	x = s.snapshot()
	if len(x.Anomalies) != 9 || x.Anomalies[5].Count != 2 || x.Anomalies[6].Count != 2 {
		t.Fatalf("truncated grouping: %+v", x.Anomalies)
	}
	for i := 0; i < liveCapacity-13; i++ {
		s.record([]byte("INFO filler"), false)
	}
	x = s.snapshot()
	if x.Anomalies[0].ID != 2 || x.Anomalies[0].Count != 1 || x.Anomalies[0].FirstTime != "last" {
		t.Fatalf("eviction did not update anomaly evidence: %+v", x.Anomalies[0])
	}
}

func TestLiveRelatedEvidenceBounded(t *testing.T) {
	var s liveStore
	for i := 0; i < liveCapacity; i++ {
		liveTestMessage(t, &s, "[GetTask] req={}", liveTestTrace, fmt.Sprintf("%016x", i+1))
	}
	x := s.snapshot()
	total := 0
	for _, r := range x.Requests {
		total += len(r.RelatedIDs)
		if len(r.RelatedIDs) > liveRelatedPerRequest || len(r.EventIDs) != 1 || r.SQLCount != 0 || r.State != "missing_end" {
			t.Fatalf("bounded request: %+v", r)
		}
	}
	if len(x.Requests) != liveCapacity || total != liveRelatedLimit {
		t.Fatalf("related output cap: rows=%d references=%d", len(x.Requests), total)
	}
}

func TestLiveSnapshotsOwnMutableDataAndInput(t *testing.T) {
	var s liveStore
	line := []byte("ERROR literal")
	s.record(line, false)
	line[0] = 'X'
	liveTestMessage(t, &s, "[GetTask] req={}", liveTestTrace, liveTestSpanA)
	liveTestMessage(t, &s, "[GetTask] 0ms resp={}", liveTestTrace, liveTestSpanA)
	before := s.snapshot()
	x := s.snapshot()
	x.Events[0].Raw = "changed"
	x.Events[0].Message = "changed"
	*x.Events[2].DurationMS = 999
	x.Anomalies[0].EventIDs[0] = 999
	x.Requests[0].EventIDs[0] = 999
	x.Requests[0].Warnings = append(x.Requests[0].Warnings, "changed")
	*x.Requests[0].DurationMS = 999
	x.Stats.Levels.Error = 999
	if !reflect.DeepEqual(before, s.snapshot()) || before.Events[0].Raw != "ERROR literal" {
		t.Fatal("snapshot or input aliases the store")
	}
	var invalid liveStore
	invalid.record([]byte{'E', 'R', 'R', 'O', 'R', ' ', 0xff}, false)
	if e := invalid.snapshot().Events[0]; !e.Truncated || !utf8.ValidString(e.Message) || !utf8.ValidString(e.Raw) {
		t.Fatalf("invalid UTF-8 not safely retained: %+v", e)
	}
}

func TestLiveConcurrentRecordSnapshot(t *testing.T) {
	var s liveStore
	var wg sync.WaitGroup
	for worker := 0; worker < 6; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				s.record([]byte(`{"level":"ERROR","msg":"same","duration":0}`), false)
				x := s.snapshot()
				if x.Stats.Observed != x.Stats.Evicted+uint64(x.Stats.Retained) || len(x.Events) != x.Stats.Retained {
					t.Errorf("incoherent concurrent snapshot: %+v", x.Stats)
					return
				}
				if len(x.Events) > 0 {
					*x.Events[0].DurationMS = 42
					x.Events[0].Raw = "mutated"
					x.Anomalies[0].EventIDs[0] = 42
				}
			}
		}()
	}
	wg.Wait()
	x := s.snapshot()
	if x.Stats.Observed != 600 || x.Stats.Levels.Error != 600 || x.Anomalies[0].Count != 600 {
		t.Fatalf("lost concurrent records: %+v", x.Stats)
	}
	for i, e := range x.Events {
		if e.ID != uint64(i+1) || e.DurationMS == nil || *e.DurationMS != 0 {
			t.Fatalf("concurrent mutation or non-monotonic ID: %+v", e)
		}
	}
}
