package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

const maxSummaryGroups = 1000

type SQLSummaryGroup struct {
	Fingerprint string
	Source      string
	Count       uint64
	Errors      uint64
	Slow        uint64
	MaxMS       float64
	HasDuration bool
}

type SQLSummary struct {
	Start           time.Time
	End             time.Time
	Total           uint64
	Errors          uint64
	Slow            uint64
	MissingDuration uint64
	Overflow        uint64
	Groups          []SQLSummaryGroup
}

type sqlSummaryCollector struct {
	mu      sync.Mutex
	slowMS  float64
	summary SQLSummary
	groups  map[[32]byte]*SQLSummaryGroup
}

func newSQLSummaryCollector(slowMS float64, start time.Time) *sqlSummaryCollector {
	return &sqlSummaryCollector{slowMS: slowMS, summary: SQLSummary{Start: start}, groups: make(map[[32]byte]*SQLSummaryGroup)}
}

func (c *sqlSummaryCollector) record(e *Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.summary.Total++
	isError := e.Kind == "error" || e.Error != ""
	isSlow := e.ElapsedMS != nil && *e.ElapsedMS >= c.slowMS
	if e.ElapsedMS == nil {
		c.summary.MissingDuration++
	}
	if isError {
		c.summary.Errors++
	}
	if isSlow {
		c.summary.Slow++
	}
	if !isError && !isSlow {
		return
	}
	key := sha256.Sum256([]byte(e.Fingerprint + "\x00" + e.Source))
	group := c.groups[key]
	if group == nil {
		if len(c.groups) >= maxSummaryGroups {
			c.summary.Overflow++
			return
		}
		source, _ := limitText(e.Source, 240)
		group = &SQLSummaryGroup{Fingerprint: e.Fingerprint, Source: strings.Clone(source)}
		c.groups[key] = group
	}
	group.Count++
	if isError {
		group.Errors++
	}
	if isSlow {
		group.Slow++
	}
	if e.ElapsedMS != nil {
		group.HasDuration = true
		if *e.ElapsedMS > group.MaxMS {
			group.MaxMS = *e.ElapsedMS
		}
	}
}

func (c *sqlSummaryCollector) drain(end time.Time) SQLSummary {
	c.mu.Lock()
	result := c.summary
	result.End = end
	groups := c.groups
	c.summary = SQLSummary{Start: end}
	c.groups = make(map[[32]byte]*SQLSummaryGroup)
	c.mu.Unlock()
	for _, group := range groups {
		result.Groups = append(result.Groups, *group)
	}
	sort.Slice(result.Groups, func(i, j int) bool {
		a, b := result.Groups[i], result.Groups[j]
		if a.Errors != b.Errors {
			return a.Errors > b.Errors
		}
		if a.Slow != b.Slow {
			return a.Slow > b.Slow
		}
		if a.Count != b.Count {
			return a.Count > b.Count
		}
		if a.MaxMS != b.MaxMS {
			return a.MaxMS > b.MaxMS
		}
		if a.Fingerprint != b.Fingerprint {
			return a.Fingerprint < b.Fingerprint
		}
		return a.Source < b.Source
	})
	if len(result.Groups) > 5 {
		result.Groups = result.Groups[:5]
	}
	return result
}

func runSQLSummaries(ctx context.Context, collector *sqlSummaryCollector, interval time.Duration, output chan<- SQLSummary, diagnostics io.Writer) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(output)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report := collector.drain(time.Now())
			if report.Errors == 0 && report.Slow == 0 {
				continue
			}
			select {
			case output <- report:
			default:
				fmt.Fprintln(diagnostics, "Feishu summary queue full; report dropped (SQL collection continues)")
			}
		}
	}
}

type notificationLog struct {
	mu     sync.Mutex
	output io.Writer
}

func (l *notificationLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.output.Write(p)
}
