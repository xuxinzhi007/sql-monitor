package main

import (
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	gormHeader  = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) (.+?\.go:[0-9]+)(?: (.*))?$`)
	gormMetrics = regexp.MustCompile(`^\[([0-9]+(?:\.[0-9]+)?ms)\] \[rows:(-|[0-9]+)\] (.+)$`)
	gormSlow    = regexp.MustCompile(`^SLOW SQL >= (.+)$`)
)

// A native GORM entry has one header followed immediately by one SQL line.
// Continuations are never guessed: physical SQL lines are marked incomplete.
func (m *Monitor) parsePhysicalLine(line []byte) {
	s := strings.TrimRight(ansiPattern.ReplaceAllString(string(line), ""), "\r\n")
	if !utf8.Valid(line) {
		m.resetGORM()
		m.status.ParseErrors++
		return
	}
	if header := gormHeader.FindStringSubmatch(s); header != nil {
		m.resetGORM()
		stamp, err := time.ParseInLocation("2006/01/02 15:04:05", header[1], time.Local)
		if err != nil || len(s) > maxError+1024 {
			m.status.ParseErrors++
			return
		}
		record := map[string]any{"ts": stamp.Format(time.RFC3339), "source": header[2], "msg": "SQL Query", "level": "INFO", "truncated": true}
		if header[3] != "" {
			slow := gormSlow.FindStringSubmatch(header[3])
			if slow != nil && durationMS(slow[1]) != nil {
				record["msg"], record["level"] = "Slow SQL", "WARN"
			} else {
				record["msg"], record["level"], record["error"] = "SQL Error", "ERROR", header[3]
			}
		}
		m.gormPending = record
		return
	}
	if metrics := gormMetrics.FindStringSubmatch(s); metrics != nil {
		pending := m.gormPending
		m.gormPending = nil
		if pending == nil || !sqlStart(metrics[3]) || durationMS(metrics[1]) == nil || metrics[2] != "-" && rowsValue(metrics[2]) == nil {
			m.status.ParseErrors++
			return
		}
		pending["elapsed"], pending["rows"], pending["sql"] = metrics[1], metrics[2], metrics[3]
		event, raw, err := recordEvent(pending)
		m.accept(event, raw, err, "gorm")
		return
	}
	if m.gormPending != nil && (strings.HasPrefix(s, "[info] ") || strings.HasPrefix(s, "[warn] ") || strings.HasPrefix(s, "[error] ")) {
		m.gormPending = nil
		m.status.IgnoredLines += 2
		return
	}
	m.resetGORM()
	if strings.HasPrefix(s, "[") && strings.Contains(s, "] [rows:") {
		m.status.ParseErrors++
		return
	}
	event, raw, err := parseLine(line)
	format := "compact"
	if strings.HasPrefix(strings.TrimSpace(s), "{") {
		format = "json"
	}
	m.accept(event, raw, err, format)
}

func (m *Monitor) resetGORM() {
	if m.gormPending != nil {
		m.status.IncompleteRecords++
		m.gormPending = nil
	}
}

func (m *Monitor) accept(event *Event, raw RawEvent, err error, format string) {
	switch {
	case err != nil:
		m.status.ParseErrors++
	case event == nil:
		m.status.IgnoredLines++
	default:
		if m.summaries != nil {
			m.summaries.record(event)
		}
		m.status.SQLObserved++
		if event.ElapsedMS != nil {
			m.status.WithDuration++
		}
		if event.Source != "" {
			m.status.WithSource++
		}
		if event.TraceID != "" {
			m.status.WithTrace++
		}
		switch format {
		case "json":
			m.status.Formats.JSON++
		case "compact":
			m.status.Formats.Compact++
		case "gorm":
			m.status.Formats.GORM++
		}
		event.ID = m.nextID
		m.nextID++
		if len(m.events) == m.status.Capacity {
			delete(m.raw, m.events[0].ID)
			copy(m.events, m.events[1:])
			m.events = m.events[:len(m.events)-1]
			m.status.DroppedEvents++
		}
		m.events = append(m.events, *event)
		m.raw[event.ID] = raw
	}
}
