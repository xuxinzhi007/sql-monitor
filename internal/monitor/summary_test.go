package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSummaryIndependentOfRetention(t *testing.T) {
	m, err := New(filepath.Join(t.TempDir(), "unused.log"), 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	collector := NewSummaryCollector(200, time.Now())
	m.SetSummaryCollector(collector)
	for i := 0; i < 3000; i++ {
		m.Consume([]byte("{\"msg\":\"SQL Error\",\"sql\":\"SELECT 'secret'\",\"elapsed\":\"250ms\",\"error\":\"private\",\"source\":\"test.go:1\"}\n"))
	}
	report := collector.Drain(time.Now())
	if report.Total != 3000 || report.Errors != 3000 || report.Slow != 3000 || len(report.Groups) != 1 || report.Groups[0].Count != 3000 {
		t.Fatalf("incorrect report: %+v", report)
	}
	if len(m.Snapshot().Events) != 2 {
		t.Fatal("retention changed")
	}
	data, _ := json.Marshal(report)
	if strings.Contains(string(data), "secret") || strings.Contains(string(data), "private") || strings.Contains(string(data), "SELECT") {
		t.Fatal("raw data in summary")
	}
	next := collector.Drain(time.Now())
	if next.Total != 0 || len(next.Groups) != 0 || !next.Start.Equal(report.End) {
		t.Fatal("window not reset")
	}
}

func TestSummaryLimitsAndThreshold(t *testing.T) {
	c := NewSummaryCollector(200, time.Now())
	below, at := 199.0, 200.0
	c.Record(&Event{Kind: "slow", ElapsedMS: &below})
	c.Record(&Event{ElapsedMS: &at, Fingerprint: "at"})
	c.Record(&Event{Kind: "error", Fingerprint: "missing"})
	r := c.Drain(time.Now())
	if r.Total != 3 || r.Slow != 1 || r.Errors != 1 || r.MissingDuration != 1 {
		t.Fatalf("thresholds: %+v", r)
	}
	for i := 0; i < maxSummaryGroups+10; i++ {
		c.Record(&Event{Kind: "error", Fingerprint: fmt.Sprint(i)})
	}
	if len(c.groups) != maxSummaryGroups {
		t.Fatal("groups not bounded")
	}
	r = c.Drain(time.Now())
	if r.Errors != maxSummaryGroups+10 || r.Overflow != 10 || len(r.Groups) != 5 {
		t.Fatalf("bounded report: %+v", r)
	}
}

func TestSummaryConcurrentDrains(t *testing.T) {
	c := NewSummaryCollector(200, time.Now())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			c.Record(&Event{Kind: "error", Fingerprint: "x"})
		}
	}()
	total := uint64(0)
	for i := 0; i < 100; i++ {
		total += c.Drain(time.Now()).Total
	}
	wg.Wait()
	total += c.Drain(time.Now()).Total
	if total != 10000 {
		t.Fatalf("lost events: %d", total)
	}
}

func TestSummaryWorkerCancellation(t *testing.T) {
	c := NewSummaryCollector(200, time.Now())
	c.Record(&Event{Kind: "error"})
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan SQLSummary, 2)
	done := make(chan struct{})
	go func() { defer close(done); RunSummaries(ctx, c, time.Millisecond, out, io.Discard) }()
	select {
	case r := <-out:
		if r.Errors != 1 {
			t.Fatal("missing error")
		}
	case <-time.After(time.Second):
		t.Fatal("no report")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}
