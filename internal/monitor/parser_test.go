package monitor

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func record(t *testing.T, sql string, fields map[string]any) []byte {
	t.Helper()
	value := map[string]any{"level": "DEBUG", "ts": "2026-09-23T10:00:00Z", "source": "internal/data/tag.go:549", "elapsed": "38.01ms", "rows": 1, "sql": sql, "msg": "SQL Query"}
	for key, field := range fields {
		value[key] = field
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func mustParse(t *testing.T, line []byte) (Event, RawEvent) {
	t.Helper()
	event, raw, err := parseLine(line)
	if err != nil || event == nil {
		t.Fatalf("parse failed: event=%v error=%v", event, err)
	}
	return *event, raw
}

func TestJSONContractAndIsolation(t *testing.T) {
	event, raw := mustParse(t, record(t, "SELECT item7 FROM sample WHERE id=42 AND name='private-value'", nil))
	if event.SQL != "SELECT item7 FROM sample WHERE id=? AND name=?" || event.Fingerprint != "06410f9363d7a9f6" {
		t.Fatalf("unexpected template: %+v", event)
	}
	if event.ElapsedMS == nil || *event.ElapsedMS != 38.01 || event.Rows == nil || *event.Rows != 1 {
		t.Fatalf("unexpected metrics: %+v", event)
	}
	if event.Source != "internal/data/tag.go:549" || event.Kind != "query" || event.TraceID != "" || event.SpanID != "" || event.Truncated {
		t.Fatalf("unexpected metadata: %+v", event)
	}
	data, _ := json.Marshal(event)
	if strings.Contains(string(data), "private-value") || !strings.Contains(raw.SQL, "private-value") {
		t.Fatal("raw isolation failed")
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000000+00:00", event.ReceivedAt); err != nil {
		t.Fatal(err)
	}
}

func TestPythonTemplatesAndFingerprints(t *testing.T) {
	cases := []struct{ sql, template, fingerprint string }{
		{"SELECT col2 FROM t WHERE a=1 AND b='first'", "SELECT col2 FROM t WHERE a=? AND b=?", "0765c99b1d5d5643"},
		{"SELECT col2 FROM t WHERE a=300 AND b='second'", "SELECT col2 FROM t WHERE a=? AND b=?", "0765c99b1d5d5643"},
		{"SELECT col12, x2y, `table42`.`a``b`, \"a\"\"3\", [col]]2], 123abc, $1 FROM t9 WHERE n=-12.3e-4 AND h=0xDEAD AND b=0b101", "SELECT col12, x2y, `table42`.`a``b`, ?, [col]]2], 123abc, $1 FROM t9 WHERE n=-? AND h=? AND b=?", "a76bd0ff6d31c0d8"},
		{"SELECT a-1, .5, 5., +2, 1e3", "SELECT a-?, ?, ?, +?, ?", "602f80b82b5380de"},
		{"SELECT 'x''y; -- private', E'a\\'b', N'c', X'DEAD', B'101', $$dollar secret$$, $tag$other;--$tag$ /* comment secret */ -- last secret\nFROM `t1` WHERE n=17 # hidden\n", "SELECT ?, ?, ?, ?, ?, ?, ? /* redacted */ /* redacted */ FROM `t1` WHERE n=? /* redacted */", "effe93b3bbc3f189"},
		{"SELECT 用户 FROM `表42` WHERE 名称='私密'", "SELECT 用户 FROM `表42` WHERE 名称=?", "7b25fd1af11d3985"},
		{"SELECT 1 /* outer /* nested */ secret */", "SELECT ? /* redacted */", "d3eb400f7885ab08"},
	}
	for _, tc := range cases {
		t.Run(tc.fingerprint+tc.sql[:6], func(t *testing.T) {
			template, fingerprint, truncated := redactSQL(tc.sql)
			if template != tc.template || fingerprint != tc.fingerprint || truncated {
				t.Fatalf("got %q %q truncated=%v", template, fingerprint, truncated)
			}
		})
	}
}

func TestDoubleQuotedSecrets(t *testing.T) {
	sql := "SELECT \"double-secret\", \"escaped\\\"secret\", \"doubled\"\"secret\", N\"national-secret\", \"literal | error:secret\" FROM `table42` WHERE [col2]=\"filter-secret\""
	expected := "SELECT ?, ?, ?, ?, ? FROM `table42` WHERE [col2]=?"
	for _, line := range [][]byte{record(t, sql, nil), []byte("[t] DEBUG [svc] [] caller.go:9 - SQL Query | sql=" + sql)} {
		event, raw := mustParse(t, line)
		if event.SQL != expected || raw.SQL != sql || raw.Error != "" {
			t.Fatalf("unexpected parse: %+v %+v", event, raw)
		}
		data, _ := json.Marshal(event)
		if strings.Contains(string(data), "secret") {
			t.Fatal("secret leaked")
		}
		other, _ := mustParse(t, record(t, "SELECT \"different\", \"values\", \"here\", N\"too\", \"changed\" FROM `table42` WHERE [col2]=\"other\"", nil))
		if other.Fingerprint != event.Fingerprint {
			t.Fatal("fingerprint is value dependent")
		}
	}
}

func TestTruncationAndIncompleteSQL(t *testing.T) {
	for _, sql := range []string{"SELECT 'private...", "SELECT E'private\\", "SELECT $tag$private...", "SELECT /* private...", "SELECT \"private...", "SELECT `private...", "SELECT [private..."} {
		template, _, truncated := redactSQL(sql)
		if strings.Contains(template, "private") || !truncated {
			t.Fatalf("unsafe incomplete template %q", template)
		}
	}
	for _, sql := range []string{"SELECT", "SELECT a FROM", "SELECT a FROM t WHERE id=", "SELECT a FROM t WHERE id IN (1, 2", "SELECT a FROM t ...", "SELECT 1)"} {
		_, _, truncated := redactSQL(sql)
		if !truncated {
			t.Fatalf("not truncated: %q", sql)
		}
	}
	for _, sql := range []string{"SELECT '(private)'", "SELECT (1 + 2)", "SELECT `from` FROM `t9`", "SELECT 1;"} {
		_, _, truncated := redactSQL(sql)
		if truncated {
			t.Fatalf("incorrectly truncated: %q", sql)
		}
		event, _ := mustParse(t, []byte("[t] DEBUG [svc] caller - SQL Query | sql="+sql))
		if !event.Truncated {
			t.Fatal("physical compact line must be conservative")
		}
	}
	for _, body := range []string{"SQL Query | elapsed=bad | rows=no | request-body", "SQL Query | sql=request-body", "Slow SQL | 203ms | 5 rows | threshold:200ms"} {
		if _, _, err := parseLine([]byte("[t] WARN [svc] caller - " + body)); err == nil {
			t.Fatalf("accepted malformed body: %q", body)
		}
	}
}

func TestDurationMS(t *testing.T) {
	valid := map[string]float64{"38.01ms": 38.01, "203ms": 203, "1us": .001, "2µs": .002, "3μs": .003, "4ns": .000004, "1m": 60000, "1m2.5s": 62500, "1h2m3s4ms5us6ns": 3723004.005006, "0": 0, "-1.5ms": -1.5, "+2s": 2000, ".5s": 500, "1.s": 1000, " 2s ": 2000}
	for value, expected := range valid {
		got := durationMS(value)
		if got == nil || math.Abs(*got-expected) > 1e-9 {
			t.Errorf("duration %q: got %v, want %v", value, got, expected)
		}
	}
	for _, value := range []any{nil, true, 12, "", "ms", "12", "1ms garbage", "NaN", "1d", "1e3ms", "--1s", "+0", strings.Repeat("9", 400) + "h"} {
		if got := durationMS(value); got != nil {
			t.Errorf("invalid duration %v returned %v", value, *got)
		}
	}
}

func TestRowsAndMetadata(t *testing.T) {
	for _, value := range []any{json.Number("0"), json.Number("-1"), json.Number("9007199254740992"), " -123 ", "00001"} {
		if rowsValue(value) == nil {
			t.Errorf("valid rows rejected: %v", value)
		}
	}
	for _, value := range []any{nil, true, 1.5, json.Number("1.0"), json.Number("1e2"), json.Number("9007199254740993"), json.Number("-9007199254740993"), "12345678901234567", "+1", "1.0"} {
		if rowsValue(value) != nil {
			t.Errorf("invalid rows accepted: %v", value)
		}
	}
	event, raw := mustParse(t, record(t, "SELECT 1", map[string]any{"msg": "SQL Error", "error": "sensitive-db-error", "trace.id": strings.Repeat("A", 32), "span.id": strings.Repeat("B", 16)}))
	if event.Kind != "error" || event.TraceID != strings.Repeat("a", 32) || event.SpanID != strings.Repeat("b", 16) || raw.Error != "sensitive-db-error" {
		t.Fatalf("unexpected error event: %+v", event)
	}
	data, _ := json.Marshal(event)
	if strings.Contains(string(data), "sensitive-db-error") {
		t.Fatal("raw error leaked")
	}
	event, _ = mustParse(t, record(t, "SELECT 1", map[string]any{"msg": "Slow SQL", "elapsed": "1m2s", "trace.id": strings.Repeat("0", 32), "span.id": strings.Repeat("0", 16)}))
	if event.Kind != "slow" || *event.ElapsedMS != 62000 || event.TraceID != "" || event.SpanID != "" {
		t.Fatalf("unexpected slow event: %+v", event)
	}
	event, raw = mustParse(t, record(t, "SELECT 1", map[string]any{"source": "\x1b[36mfile\n.go\x7f\x1b[0m", "ts": true, "level": []string{"private"}, "elapsed": false, "rows": true, "error": []string{"private"}}))
	if event.Source != "file.go" || event.Time != "" || event.Level != "" || event.ElapsedMS != nil || event.Rows != nil || raw.Error != "" {
		t.Fatalf("unclean metadata: %+v %+v", event, raw)
	}
	if len(cleanText(strings.Repeat("x", 1000))) != 512 {
		t.Fatal("metadata limit")
	}
}

func TestCompactMetadataVariants(t *testing.T) {
	trace, span := strings.Repeat("A", 32), strings.Repeat("B", 16)
	cases := []struct{ metadata, trace, span string }{
		{"[trace:" + trace + " span:" + span + " uid:private-user tenant:private-tenant] ", strings.ToLower(trace), strings.ToLower(span)},
		{"[] ", "", ""},
		{"[uid:private-user tenant:private-tenant] ", "", ""},
		{"[trace:" + trace + " uid:private-user tenant:private-tenant] ", strings.ToLower(trace), ""},
		{"[tenant:private-tenant span:" + span + " uid:private-user trace:" + trace + "] ", strings.ToLower(trace), strings.ToLower(span)},
		{"[span:" + span + "] ", "", strings.ToLower(span)},
		{"[trace:" + strings.Repeat("0", 32) + " span:" + strings.Repeat("0", 16) + "] ", "", ""},
		{"[trace:" + trace + "x span:" + span + "x] ", "", ""},
		{"[trace:short span:not-hex unknown:value] ", "", ""},
		{"[uid:trace:" + trace + " tenant:span:" + span + "] ", "", ""},
		{"[arbitrary metadata] ", "", ""},
		{"", "", ""},
	}
	for _, tc := range cases {
		for _, prefix := range []string{"", "source=internal/data/tag.go:549 | "} {
			line := "[2026-09-23 10:20:30.123] DEBUG [data-service] " + tc.metadata + "caller.go:9 - " + prefix + "SQL Query | elapsed=38.01ms | rows=1 | sql=SELECT col7 FROM t WHERE id=42"
			event, raw := mustParse(t, []byte(line))
			source := "caller.go:9"
			if prefix != "" {
				source = "internal/data/tag.go:549"
			}
			if event.Source != source || event.TraceID != tc.trace || event.SpanID != tc.span || event.Kind != "query" || *event.ElapsedMS != 38.01 || *event.Rows != 1 || !event.Truncated {
				t.Fatalf("metadata %q produced %+v", tc.metadata, event)
			}
			if event.SQL != "SELECT col7 FROM t WHERE id=?" || raw.SQL != "SELECT col7 FROM t WHERE id=42" {
				t.Fatal("SQL mismatch")
			}
			data, _ := json.Marshal(event)
			if strings.Contains(string(data), "private-") {
				t.Fatal("private metadata leaked")
			}
		}
	}
}

func TestANSIAndCompactErrorSuffix(t *testing.T) {
	line := append([]byte("\x1b[36m"), record(t, "SELECT 1", nil)...)
	line = append(line[:len(line)-1], []byte("\x1b[0m\n")...)
	mustParse(t, line)
	mustParse(t, append([]byte("\x1b]0;title\x07"), record(t, "SELECT 1", nil)...))
	mustParse(t, append([]byte("\x1b]0;title\x1b\\"), record(t, "SELECT 1", nil)...))
	event, raw := mustParse(t, []byte("\x1b[36m[10:20:30] WARN [data-service] caller.go:9 - Slow SQL | 203ms | 5 rows | threshold:200ms | SELECT 'literal | error:inside' FROM t WHERE n=22 | error:sensitive error\x1b[0m"))
	if event.Kind != "slow" || *event.ElapsedMS != 203 || *event.Rows != 5 || event.SQL != "SELECT ? FROM t WHERE n=?" || raw.Error != "sensitive error" || !strings.Contains(raw.SQL, "literal | error:inside") {
		t.Fatalf("bad suffix parse: %+v %+v", event, raw)
	}
	for _, sql := range []string{"SELECT `literal | error:inside`", "SELECT [literal | error:inside]", "SELECT 1 /* | error:inside */", "SELECT 1 -- | error:inside", "SELECT $$literal | error:inside$$"} {
		_, raw := mustParse(t, []byte("[t] DEBUG [svc] caller - SQL Query | sql="+sql))
		if raw.SQL != sql || raw.Error != "" {
			t.Fatalf("suffix inside token split: %+v", raw)
		}
	}
	event, _ = mustParse(t, []byte("[t] ERROR [svc] caller - SQL Error | 1ms | 0 rows | SELECT 'private..."))
	if event.Kind != "error" || event.Error == "" || !event.Truncated || strings.Contains(event.SQL, "private") {
		t.Fatalf("unsafe error: %+v", event)
	}
}

func TestIgnoredVersusMalformed(t *testing.T) {
	for _, line := range []string{"", "request payload SQL Query", `{"msg":"HTTP request", "sql":"private"}`, `{"msg":[]}`, `{"msg":{}}`, "FROM t", "[t] INFO [svc] caller - unrelated", "[t] INFO [svc] caller - source=file"} {
		event, _, err := parseLine([]byte(line))
		if event != nil || err != nil {
			t.Errorf("should ignore %q: %v %v", line, event, err)
		}
	}
	for _, line := range [][]byte{[]byte(`{"msg":"SQL Query",`), record(t, "", map[string]any{"sql": nil}), record(t, "", nil), {0xff}, []byte("[broken - SQL Query | sql=SELECT 1"), []byte("[t] DEBUG [svc] caller - SQL Query | elapsed=1ms | rows=1 | sql="), []byte("[t] DEBUG [svc] caller - SQL Query | sql=SELECT 1\nFROM t"), []byte(`{"msg":"SQL Query","sql":"SELECT 1"} {}`)} {
		if event, _, err := parseLine(line); err == nil || event != nil {
			t.Errorf("should reject %q: %v %v", line, event, err)
		}
	}
}

func TestNewlinesSurrogatesAndSizeLimits(t *testing.T) {
	event, raw := mustParse(t, record(t, "SELECT 'first\nsecond'\nFROM t WHERE id=1", nil))
	if event.SQL != "SELECT ? FROM t WHERE id=?" || !strings.Contains(raw.SQL, "first\nsecond") || event.Truncated {
		t.Fatal("escaped newline mismatch")
	}
	for _, line := range []string{`{"msg":"SQL Query","sql":"SELECT '\ud800'"}`, `{"msg":"SQL Query","sql":"SELECT \"\ud800\""}`} {
		event, _ := mustParse(t, []byte(line))
		if event.SQL != "SELECT ?" {
			t.Fatalf("surrogate literal not redacted: %q", event.SQL)
		}
	}
	for _, char := range []string{"x", "私"} {
		event, raw := mustParse(t, record(t, "SELECT '"+strings.Repeat(char, 90000)+"'", map[string]any{"error": strings.Repeat(char, 10000)}))
		if event.SQL != "SELECT ?" || !event.Truncated || utf8.RuneCountInString(raw.SQL) != maxSQL || utf8.RuneCountInString(raw.Error) != maxError {
			t.Fatal("size limits not enforced")
		}
	}
	template, fingerprint, truncated := redactSQL("SELECT " + strings.Repeat("identifier, ", 4000))
	if utf8.RuneCountInString(template) != maxTemplate+1 || !truncated || fingerprint == "" {
		t.Fatal("template limit not enforced")
	}
	sql := "SELECT " + strings.Repeat("a", maxTemplate-7)
	template, _, truncated = redactSQL(sql)
	if template != sql || truncated {
		t.Fatal("exact template limit truncated")
	}
	_, raw = mustParse(t, record(t, strings.Repeat("a", maxSQL), map[string]any{"error": strings.Repeat("e", maxError)}))
	if len(raw.SQL) != maxSQL || len(raw.Error) != maxError {
		t.Fatal("exact raw limits")
	}
}

func TestUnicodeDecimalsAndNumericMetadata(t *testing.T) {
	template, fingerprint, truncated := redactSQL("SELECT １２.٣e４, .٥, １２abc, 𝟙𝟚")
	if template != "SELECT ?, ?, １２abc, ?" || fingerprint != "cc3ddb732200c456" || truncated {
		t.Fatalf("Unicode number token mismatch: %q %s %v", template, fingerprint, truncated)
	}
	for input, expected := range map[string]float64{"１２.٥ms": 12.5, "𝟙𝟚s": 12000, "۰ms": 0} {
		if got := durationMS(input); got == nil || *got != expected {
			t.Errorf("Unicode duration mismatch: %q", input)
		}
	}
	if durationMS("０") != nil {
		t.Fatal("only ASCII zero may omit a duration unit")
	}
	if got := rowsValue(" -１２٣ "); got == nil || *got != -123 {
		t.Fatal("Unicode rows mismatch")
	}
	event, _ := mustParse(t, []byte("[t] INFO [svc] caller - SQL Query | １２ms | １２ rows | SELECT 1"))
	if *event.ElapsedMS != 12 || *event.Rows != 12 {
		t.Fatal("compact Unicode metrics mismatch")
	}
	for input, expected := range map[string]string{"1e3": "1000.0", "1.0": "1.0", "-0": "0", "-0.0": "-0.0", "1e16": "1e+16", "1e-5": "1e-05", "1e-4": "0.0001", "1e400": "inf", "-1e400": "-inf"} {
		if got := cleanText(json.Number(input)); got != expected {
			t.Errorf("numeric metadata %s: got %q, want %q", input, got, expected)
		}
	}
}

func FuzzParseLine(f *testing.F) {
	for _, seed := range []string{`{"msg":"SQL Query","sql":"SELECT 'secret'"}`, "[t] INFO [svc] [] caller - SQL Query | sql=SELECT 1", "\xff", `{"msg":[]}`, "SELECT '"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, line []byte) {
		if len(line) > maxLine {
			t.Skip()
		}
		event, raw, err := parseLine(line)
		if err != nil || event == nil {
			return
		}
		if _, err := json.Marshal(event); err != nil {
			t.Fatal(err)
		}
		if utf8.RuneCountInString(raw.SQL) > maxSQL || utf8.RuneCountInString(raw.Error) > maxError || utf8.RuneCountInString(event.SQL) > maxTemplate+1 {
			t.Fatal("unbounded event")
		}
	})
}
