package monitor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	maxLine     = 1024 * 1024
	maxSQL      = 64 * 1024
	maxTemplate = 16 * 1024
	maxError    = 8192
	readChunk   = 64 * 1024
)

var (
	ansiPattern             = regexp.MustCompile("\x1b(?:\\[[0-?]*[ -/]*[@-~]|\\][^\x07\x1b]*(?:\x07|\x1b\\\\))")
	durationPattern         = regexp.MustCompile(`^(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|μs|ms|s|m|h)`)
	numberPattern           = regexp.MustCompile(`^(?:0[xX][0-9a-fA-F]+|0[bB][01]+|(?:\p{Nd}+(?:\.\p{Nd}*)?|\.\p{Nd}+)(?:[eE][+-]?\p{Nd}+)?)`)
	dollarPattern           = regexp.MustCompile(`^\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$`)
	compactPattern          = regexp.MustCompile(`^\[([^\]\r\n]+)\][ \t]+(DEBUG|INFO|WARN|WARNING|ERROR|FATAL|TRACE)[ \t]+\[([^\]\r\n]*)\][ \t]+(?:\[([^\]\r\n]*)\][ \t]+)?([^\s\[][^\r\n]*?)[ \t]+-[ \t]+([^\r\n]*)$`)
	malformedCompactPattern = regexp.MustCompile(`(?: - | \| )(?:SQL Query|Slow SQL|SQL Error)(?: \||$)`)
	rowsPattern             = regexp.MustCompile(`^-?\d{1,16}$`)
	compactRowsPattern      = regexp.MustCompile(`^-?\p{Nd}+ rows$`)
)

type Event struct {
	ID          uint64   `json:"id"`
	Time        string   `json:"time"`
	ReceivedAt  string   `json:"received_at"`
	Level       string   `json:"level"`
	Kind        string   `json:"kind"`
	SQL         string   `json:"sql"`
	Fingerprint string   `json:"fingerprint"`
	ElapsedMS   *float64 `json:"elapsed_ms"`
	Rows        *int64   `json:"rows"`
	Source      string   `json:"source"`
	TraceID     string   `json:"trace_id"`
	SpanID      string   `json:"span_id"`
	Error       string   `json:"error"`
	Truncated   bool     `json:"truncated"`
}

type RawEvent struct {
	SQL   string `json:"sql"`
	Error string `json:"error"`
}

func now() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000000+00:00")
}

func durationMS(value any) *float64 {
	s, ok := value.(string)
	if !ok {
		return nil
	}
	s = strings.TrimFunc(s, sqlSpace)
	total := 0.0
	if s == "0" {
		return &total
	}
	s = decimalASCII(s)
	sign := 1.0
	if strings.HasPrefix(s, "-") || strings.HasPrefix(s, "+") {
		if s[0] == '-' {
			sign = -1
		}
		s = s[1:]
	}
	if s == "" {
		return nil
	}
	for s != "" {
		match := durationPattern.FindStringSubmatch(s)
		if match == nil {
			return nil
		}
		n, err := strconv.ParseFloat(match[1], 64)
		if err != nil {
			return nil
		}
		factor := 1.0
		switch match[2] {
		case "ns":
			factor = 0.000001
		case "us", "µs", "μs":
			factor = 0.001
		case "s":
			factor = 1000
		case "m":
			factor = 60000
		case "h":
			factor = 3600000
		}
		total += n * factor
		s = s[len(match[0]):]
	}
	if math.IsInf(total, 0) || math.IsNaN(total) {
		return nil
	}
	total *= sign
	return &total
}

func sqlSpace(r rune) bool {
	return unicode.IsSpace(r) || r >= '\x1c' && r <= '\x1f'
}

func decimalASCII(s string) string {
	return strings.Map(func(r rune) rune {
		if r < utf8.RuneSelf || !unicode.IsDigit(r) {
			return r
		}
		for _, span := range unicode.Digit.R16 {
			if r >= rune(span.Lo) && r <= rune(span.Hi) {
				return '0' + (r-rune(span.Lo))/rune(span.Stride)%10
			}
		}
		for _, span := range unicode.Digit.R32 {
			if r >= rune(span.Lo) && r <= rune(span.Hi) {
				return '0' + (r-rune(span.Lo))/rune(span.Stride)%10
			}
		}
		return r
	}, s)
}

func limitText(s string, limit int) (string, bool) {
	count := 0
	for i := range s {
		if count == limit {
			return s[:i], true
		}
		count++
	}
	return s, false
}

func cleanText(value any) string {
	var s string
	switch v := value.(type) {
	case string:
		s = v
	case json.Number:
		s = string(v)
		if strings.ContainsAny(s, ".eE") {
			n, _ := v.Float64()
			if math.IsInf(n, 1) {
				s = "inf"
			} else if math.IsInf(n, -1) {
				s = "-inf"
			} else {
				format := byte('g')
				if n == 0 || math.Abs(n) >= 1e-4 && math.Abs(n) < 1e16 {
					format = 'f'
				}
				s = strconv.FormatFloat(n, format, -1, 64)
				if !strings.ContainsAny(s, ".e") {
					s += ".0"
				}
			}
		} else if s == "-0" {
			s = "0"
		}
	default:
		return ""
	}
	s = ansiPattern.ReplaceAllString(s, "")
	s = strings.Map(func(r rune) rune {
		if r < ' ' || r == '\x7f' {
			return -1
		}
		return r
	}, s)
	s, _ = limitText(s, 512)
	return strings.Clone(s)
}

func traceValue(value any, size int) string {
	s, ok := value.(string)
	if !ok || len(s) != size {
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
	return strings.Clone(strings.ToLower(s))
}

func rowsValue(value any) *int64 {
	var s string
	switch v := value.(type) {
	case json.Number:
		s = string(v)
	case string:
		s = decimalASCII(strings.TrimFunc(v, sqlSpace))
		if !rowsPattern.MatchString(s) {
			return nil
		}
	default:
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < -(1<<53) || n > 1<<53 {
		return nil
	}
	return &n
}

type tokenKind uint8

const (
	tokenSpace tokenKind = iota
	tokenComment
	tokenLiteral
	tokenIdentifier
	tokenSymbol
)

type sqlToken struct {
	kind   tokenKind
	text   string
	closed bool
	start  int
}

func quotedEnd(sql string, start int) (int, bool) {
	endQuote := sql[start]
	if endQuote == '[' {
		endQuote = ']'
	}
	for i := start + 1; i < len(sql); {
		if sql[i] == '\\' {
			i++
			if i < len(sql) {
				_, size := utf8.DecodeRuneInString(sql[i:])
				i += size
			}
		} else if sql[i] == endQuote {
			if i+1 < len(sql) && sql[i+1] == endQuote {
				i += 2
			} else {
				return i + 1, true
			}
		} else {
			i++
		}
	}
	return len(sql), false
}

func identifierRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_' || r == '$'
}

func sqlTokens(sql string, yield func(sqlToken) bool) {
	for i := 0; i < len(sql); {
		start := i
		r, size := utf8.DecodeRuneInString(sql[i:])
		next, _ := utf8.DecodeRuneInString(sql[i+size:])
		kind, closed := tokenSymbol, true
		switch {
		case sqlSpace(r):
			kind = tokenSpace
			for i < len(sql) {
				c, n := utf8.DecodeRuneInString(sql[i:])
				if !sqlSpace(c) {
					break
				}
				i += n
			}
		case strings.HasPrefix(sql[i:], "--") || r == '#':
			kind = tokenComment
			end := strings.IndexByte(sql[i:], '\n')
			if end < 0 {
				i = len(sql)
			} else {
				i += end
			}
		case strings.HasPrefix(sql[i:], "/*"):
			kind = tokenComment
			i += 2
			depth := 1
			for i < len(sql) && depth != 0 {
				switch {
				case strings.HasPrefix(sql[i:], "/*"):
					depth++
					i += 2
				case strings.HasPrefix(sql[i:], "*/"):
					depth--
					i += 2
				default:
					i++
				}
			}
			closed = depth == 0
		case r == '\'' || r == '"' || strings.ContainsRune("eEnNxXbB", r) && i+1 < len(sql) && (sql[i+1] == '\'' || sql[i+1] == '"'):
			kind = tokenLiteral
			quoteStart := i
			if r != '\'' && r != '"' {
				quoteStart++
			}
			i, closed = quotedEnd(sql, quoteStart)
		case r == '`' || r == '[':
			kind = tokenIdentifier
			i, closed = quotedEnd(sql, i)
		case r == '$' && dollarPattern.MatchString(sql[i:]):
			kind = tokenLiteral
			delimiter := dollarPattern.FindString(sql[i:])
			end := strings.Index(sql[i+len(delimiter):], delimiter)
			if end < 0 {
				i, closed = len(sql), false
			} else {
				i += len(delimiter)*2 + end
			}
		case unicode.IsLetter(r) || r == '_' || r == '$':
			kind = tokenIdentifier
			i += size
			for i < len(sql) {
				c, n := utf8.DecodeRuneInString(sql[i:])
				if !identifierRune(c) {
					break
				}
				i += n
			}
		case unicode.IsDigit(r) || r == '.' && unicode.IsDigit(next):
			kind = tokenLiteral
			match := numberPattern.FindString(sql[i:])
			if match == "" {
				i += size
			} else {
				i += len(match)
				if i < len(sql) {
					c, _ := utf8.DecodeRuneInString(sql[i:])
					if unicode.IsLetter(c) || c == '_' || c == '$' {
						kind = tokenIdentifier
						for i < len(sql) {
							c, n := utf8.DecodeRuneInString(sql[i:])
							if !identifierRune(c) {
								break
							}
							i += n
						}
					}
				}
			}
		default:
			i += size
		}
		text := sql[start:i]
		if kind == tokenSpace {
			text = " "
		}
		if !yield(sqlToken{kind: kind, text: text, closed: closed, start: start}) {
			return
		}
	}
}

func redactSQL(sql string) (string, string, bool) {
	var pieces strings.Builder
	truncated := strings.HasSuffix(strings.TrimRightFunc(sql, sqlSpace), "...")
	depth, last := 0, ""
	sqlTokens(sql, func(token sqlToken) bool {
		truncated = truncated || !token.closed
		if token.kind == tokenSymbol {
			if token.text == "(" {
				depth++
			}
			if token.text == ")" {
				depth--
				truncated = truncated || depth < 0
			}
		}
		if token.kind != tokenSpace && token.kind != tokenComment && token.text != ";" {
			last = token.text
			if token.kind == tokenIdentifier {
				last = strings.ToUpper(last)
			}
			if token.kind == tokenLiteral {
				last = "?"
			}
		}
		switch {
		case token.kind == tokenLiteral || !token.closed:
			pieces.WriteByte('?')
		case token.kind == tokenComment:
			pieces.WriteString("/* redacted */")
		default:
			pieces.WriteString(token.text)
		}
		return true
	})
	switch last {
	case "SELECT", "FROM", "WHERE", "JOIN", "ON", "AND", "OR", "BY", "HAVING", "INTO", "VALUES", "SET", "AS", "UNION", "WHEN", "THEN", "ELSE", "BETWEEN", "IN", "IS", "NOT", "LIKE", "LIMIT", "OFFSET", "RETURNING", "=", ",", "+", "-", "/", "<", ">", "|", "&":
		truncated = true
	}
	truncated = truncated || depth != 0
	template := strings.TrimFunc(pieces.String(), sqlSpace)
	hash := sha256.Sum256([]byte(template))
	fingerprint := hex.EncodeToString(hash[:8])
	template, limited := limitText(template, maxTemplate)
	if limited {
		template += "…"
	}
	return template, fingerprint, truncated || limited
}

func sqlKind(message any) string {
	switch message {
	case "SQL Query":
		return "query"
	case "Slow SQL":
		return "slow"
	case "SQL Error":
		return "error"
	default:
		return ""
	}
}

func compactRecord(line string) (map[string]any, error) {
	match := compactPattern.FindStringSubmatch(line)
	if match == nil {
		if malformedCompactPattern.MatchString(line) {
			return nil, errors.New("malformed compact SQL record")
		}
		return nil, nil
	}
	parts := strings.Split(match[6], " | ")
	source := match[5]
	if strings.HasPrefix(parts[0], "source=") {
		source = parts[0][7:]
		parts = parts[1:]
	}
	if len(parts) == 0 || sqlKind(parts[0]) == "" {
		return nil, nil
	}
	record := map[string]any{"ts": match[1], "level": match[2], "source": source, "msg": parts[0], "truncated": true}
	parts = parts[1:]
	for _, field := range strings.FieldsFunc(match[4], sqlSpace) {
		name, value, found := strings.Cut(field, ":")
		if found && (name == "trace" || name == "span") {
			size := 32
			if name == "span" {
				size = 16
			}
			record[name+".id"] = traceValue(value, size)
		}
	}
	for len(parts) > 0 {
		part := parts[0]
		if strings.HasPrefix(part, "sql=") {
			parts[0] = part[4:]
			break
		}
		switch {
		case strings.HasPrefix(part, "elapsed="):
			record["elapsed"] = part[8:]
		case strings.HasPrefix(part, "rows="):
			record["rows"] = part[5:]
		case durationMS(part) != nil:
			record["elapsed"] = part
		case compactRowsPattern.MatchString(part):
			record["rows"] = part[:len(part)-5]
		case strings.HasPrefix(part, "threshold:"):
		default:
			goto sqlBody
		}
		parts = parts[1:]
	}
sqlBody:
	sql := strings.Join(parts, " | ")
	sqlTokens(sql, func(token sqlToken) bool {
		if token.kind == tokenSpace && strings.HasPrefix(sql[token.start:], " | error:") {
			record["error"] = strings.TrimFunc(sql[token.start+len(" | error:"):], sqlSpace)
			sql = sql[:token.start]
			return false
		}
		return true
	})
	if strings.TrimFunc(sql, sqlSpace) == "" {
		return nil, errors.New("missing SQL")
	}
	if !sqlStart(sql) {
		return nil, errors.New("malformed compact SQL")
	}
	record["sql"] = sql
	return record, nil
}

func sqlStart(sql string) bool {
	first := ""
	sqlTokens(sql, func(token sqlToken) bool {
		if token.kind != tokenSpace && token.kind != tokenComment && token.text != "(" {
			first = strings.ToUpper(token.text)
			return false
		}
		return true
	})
	switch first {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "MERGE", "WITH", "CREATE", "ALTER", "DROP", "TRUNCATE", "SHOW", "EXPLAIN", "DESCRIBE", "DESC", "BEGIN", "START", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "SET", "USE", "PRAGMA", "CALL", "EXEC", "EXECUTE", "PREPARE", "DEALLOCATE", "GRANT", "REVOKE", "ANALYZE", "VACUUM", "LOCK", "UNLOCK", "DO", "VALUES", "TABLE", "COMMENT", "RENAME", "FLUSH", "KILL", "LOAD", "COPY", "REINDEX", "ATTACH", "DETACH":
		return true
	default:
		return false
	}
}

func parseLine(line []byte) (*Event, RawEvent, error) {
	if !utf8.Valid(line) {
		return nil, RawEvent{}, errors.New("invalid UTF-8")
	}
	s := strings.TrimRight(ansiPattern.ReplaceAllString(string(line), ""), "\r\n")
	if strings.TrimFunc(s, sqlSpace) == "" {
		return nil, RawEvent{}, nil
	}
	var record map[string]any
	if strings.HasPrefix(strings.TrimLeftFunc(s, sqlSpace), "{") {
		decoder := json.NewDecoder(strings.NewReader(s))
		decoder.UseNumber()
		if err := decoder.Decode(&record); err != nil {
			return nil, RawEvent{}, errors.New("malformed JSON")
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			return nil, RawEvent{}, errors.New("malformed JSON")
		}
		message, ok := record["msg"].(string)
		if !ok || sqlKind(message) == "" {
			return nil, RawEvent{}, nil
		}
	} else {
		var err error
		record, err = compactRecord(s)
		if err != nil || record == nil {
			return nil, RawEvent{}, err
		}
	}
	return recordEvent(record)
}

func recordEvent(record map[string]any) (*Event, RawEvent, error) {
	sql, ok := record["sql"].(string)
	if !ok || strings.TrimFunc(sql, sqlSpace) == "" {
		return nil, RawEvent{}, errors.New("missing SQL")
	}
	rawSQL, sqlLimited := limitText(sql, maxSQL)
	rawError, _ := record["error"].(string)
	limitedError, errorLimited := limitText(rawError, maxError)
	template, fingerprint, truncated := redactSQL(rawSQL)
	explicitTruncation, _ := record["truncated"].(bool)
	event := &Event{
		Time: cleanText(record["ts"]), ReceivedAt: now(), Level: strings.ToUpper(cleanText(record["level"])),
		Kind: sqlKind(record["msg"]), SQL: template, Fingerprint: fingerprint,
		ElapsedMS: durationMS(record["elapsed"]), Rows: rowsValue(record["rows"]), Source: cleanText(record["source"]),
		TraceID: traceValue(record["trace.id"], 32), SpanID: traceValue(record["span.id"], 16),
		Truncated: truncated || sqlLimited || errorLimited || explicitTruncation,
	}
	if rawError != "" || event.Kind == "error" {
		event.Error = "SQL execution error (details available on explicit request)"
	}
	return event, RawEvent{SQL: strings.Clone(rawSQL), Error: strings.Clone(limitedError)}, nil
}
