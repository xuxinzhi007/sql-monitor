package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSummaryIndependentOfRetention(t *testing.T) {
	m, err := newMonitor(filepath.Join(t.TempDir(), "unused.log"), 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.summaries = newSQLSummaryCollector(200, time.Now())
	for i := 0; i < 3000; i++ {
		m.Consume([]byte("{\"msg\":\"SQL Error\",\"sql\":\"SELECT 'secret'\",\"elapsed\":\"250ms\",\"error\":\"private\",\"source\":\"test.go:1\"}\n"))
	}
	report := m.summaries.drain(time.Now())
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
	next := m.summaries.drain(time.Now())
	if next.Total != 0 || len(next.Groups) != 0 || !next.Start.Equal(report.End) {
		t.Fatal("window not reset")
	}
}

func TestSummaryLimitsAndThreshold(t *testing.T) {
	c := newSQLSummaryCollector(200, time.Now())
	below, at := 199.0, 200.0
	c.record(&Event{Kind: "slow", ElapsedMS: &below})
	c.record(&Event{ElapsedMS: &at, Fingerprint: "at"})
	c.record(&Event{Kind: "error", Fingerprint: "missing"})
	r := c.drain(time.Now())
	if r.Total != 3 || r.Slow != 1 || r.Errors != 1 || r.MissingDuration != 1 {
		t.Fatalf("thresholds: %+v", r)
	}
	for i := 0; i < maxSummaryGroups+10; i++ {
		c.record(&Event{Kind: "error", Fingerprint: fmt.Sprint(i)})
	}
	if len(c.groups) != maxSummaryGroups {
		t.Fatal("groups not bounded")
	}
	r = c.drain(time.Now())
	if r.Errors != maxSummaryGroups+10 || r.Overflow != 10 || len(r.Groups) != 5 {
		t.Fatalf("bounded report: %+v", r)
	}
}

func TestSummaryConcurrentDrains(t *testing.T) {
	c := newSQLSummaryCollector(200, time.Now())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			c.record(&Event{Kind: "error", Fingerprint: "x"})
		}
	}()
	total := uint64(0)
	for i := 0; i < 100; i++ {
		total += c.drain(time.Now()).Total
	}
	wg.Wait()
	total += c.drain(time.Now()).Total
	if total != 10000 {
		t.Fatalf("lost events: %d", total)
	}
}

func TestSummaryWorkerCancellation(t *testing.T) {
	c := newSQLSummaryCollector(200, time.Now())
	c.record(&Event{Kind: "error"})
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan SQLSummary, 2)
	done := make(chan struct{})
	go func() { defer close(done); runSQLSummaries(ctx, c, time.Millisecond, out, io.Discard) }()
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

func TestFeishuConfigValidation(t *testing.T) {
	good := func() FeishuConfig {
		return FeishuConfig{Enabled: true, AppID: "test", AppSecret: "secret", Project: "test", IntervalSeconds: 300, SlowMS: 200, Recipients: []FeishuRecipient{{Email: "test@example.com"}}}
	}
	for _, change := range []func(*FeishuConfig){
		func(c *FeishuConfig) { c.IntervalSeconds = 0 },
		func(c *FeishuConfig) { c.SlowMS = -1 },
		func(c *FeishuConfig) { c.AppSecret = "" },
		func(c *FeishuConfig) { c.Recipients = nil },
		func(c *FeishuConfig) { c.Recipients[0].Email = "Name <test@example.com>" },
		func(c *FeishuConfig) { c.Recipients = append(c.Recipients, c.Recipients[0]) },
	} {
		c := good()
		change(&c)
		if c.validate() == nil {
			t.Fatal("accepted invalid config")
		}
	}
	cfg := good()
	b, _ := json.Marshal(map[string]any{"feishu": cfg})
	p := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	opts, err := parseOptions([]string{"--config", p}, io.Discard)
	if err != nil || opts.feishu == nil || !opts.feishu.Enabled {
		t.Fatalf("config failed: %v", err)
	}
	if err := os.Chmod(p, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := parseOptions([]string{"--config", p}, io.Discard); err == nil {
		t.Fatal("accepted world readable credentials")
	}
	if _, err := parseOptions(nil, io.Discard); err != nil {
		t.Fatal(err)
	}
}
