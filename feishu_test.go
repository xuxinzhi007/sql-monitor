package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type feishuTestTransport func(*http.Request) (*http.Response, error)

func (f feishuTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Only tests rewrite the official URL to a local httptest server. A request to
// any unexpected host fails here rather than ever reaching the public network.
func feishuTestNotifier(t *testing.T, cfg FeishuConfig, handler http.HandlerFunc) *feishuNotifier {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	n := newFeishuNotifier(cfg)
	transport := server.Client().Transport
	n.client = &http.Client{Transport: feishuTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "open.feishu.cn" {
			return nil, errors.New("unexpected destination")
		}
		if r.URL.Path != "/open-apis/auth/v3/tenant_access_token/internal" && r.URL.Path != "/open-apis/im/v1/messages" {
			return nil, errors.New("unexpected path")
		}
		copy := r.Clone(r.Context())
		copy.URL.Scheme, copy.URL.Host = base.Scheme, base.Host
		copy.Host = base.Host
		return transport.RoundTrip(copy)
	})}
	return n
}

func feishuTestConfig() FeishuConfig {
	return FeishuConfig{Enabled: true, AppID: "test-app", AppSecret: "test-secret-not-real", Project: "test-project", SlowMS: 200, Recipients: []FeishuRecipient{{Name: "One", Email: "one@example.test"}, {Name: "Two", Email: "two@example.test"}}}
}

func feishuTestSummary() SQLSummary {
	return SQLSummary{Start: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC), End: time.Date(2026, 1, 1, 1, 1, 0, 0, time.UTC), Total: 10, Errors: 2, Slow: 3, MissingDuration: 1, Overflow: 4, Groups: []SQLSummaryGroup{{Fingerprint: "hash", Source: "app.go:12", Count: 10, Errors: 2, Slow: 3, MaxMS: 450}}}
}

func feishuTestRun(n *feishuNotifier, summaries ...SQLSummary) string {
	ch := make(chan SQLSummary, len(summaries))
	for _, summary := range summaries {
		ch <- summary
	}
	close(ch)
	var output bytes.Buffer
	n.run(context.Background(), ch, &output)
	return output.String()
}

func TestFeishuTokenReuseAndExpiry(t *testing.T) {
	cfg := feishuTestConfig()
	var mu sync.Mutex
	tokens, messages := 0, 0
	uuidSeen := make(map[string]bool)
	uuidPattern := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	n := feishuTestNotifier(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			t.Error("missing JSON content type")
		}
		if strings.Contains(r.URL.Path, "/auth/") {
			tokens++
			if r.Header.Get("Authorization") != "" {
				t.Error("auth request carried bearer token")
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body["app_id"] != cfg.AppID || body["app_secret"] != cfg.AppSecret || len(body) != 2 {
				t.Error("incorrect auth payload")
			}
			fmt.Fprintf(w, `{"code":0,"tenant_access_token":"token-%d","expire":7200}`, tokens)
			return
		}
		messages++
		if r.URL.Query().Get("receive_id_type") != "email" {
			t.Error("not addressed by email")
		}
		if r.Header.Get("Authorization") != fmt.Sprintf("Bearer token-%d", tokens) {
			t.Error("incorrect bearer token")
		}
		var body struct {
			ReceiveID string `json:"receive_id"`
			MsgType   string `json:"msg_type"`
			Content   string `json:"content"`
			UUID      string `json:"uuid"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.ReceiveID != cfg.Recipients[(messages-1)%2].Email {
			t.Errorf("recipient = %q", body.ReceiveID)
		}
		if body.MsgType != "interactive" {
			t.Errorf("message type = %q", body.MsgType)
		}
		var card feishuCard
		if err := json.Unmarshal([]byte(body.Content), &card); err != nil {
			t.Error(err)
		}
		if card.Header.Title.Tag != "plain_text" || len(card.Elements) == 0 {
			t.Error("invalid nested card")
		}
		if !uuidPattern.MatchString(body.UUID) || uuidSeen[body.UUID] {
			t.Errorf("invalid/repeated UUID %q", body.UUID)
		}
		uuidSeen[body.UUID] = true
		io.WriteString(w, `{"code":0}`)
	})
	if logs := feishuTestRun(n, feishuTestSummary(), feishuTestSummary()); logs != "" {
		t.Fatalf("unexpected logs: %s", logs)
	}
	mu.Lock()
	if tokens != 1 || messages != 4 {
		t.Errorf("tokens=%d messages=%d", tokens, messages)
	}
	mu.Unlock()
	n.tokenExpires = time.Now().Add(-time.Second)
	if logs := feishuTestRun(n, feishuTestSummary()); logs != "" {
		t.Fatalf("unexpected logs: %s", logs)
	}
	mu.Lock()
	defer mu.Unlock()
	if tokens != 2 || messages != 6 {
		t.Errorf("tokens=%d messages=%d", tokens, messages)
	}
	if !n.tokenExpires.After(time.Now()) || n.tokenExpires.After(time.Now().Add(7200*time.Second)) {
		t.Error("incorrect expiration")
	}
	data, err := json.Marshal(n)
	if err != nil || string(data) != "{}" {
		t.Fatalf("notifier exported private state: %s, %v", data, err)
	}
}

func TestFeishuCardPlainTextAndOmissions(t *testing.T) {
	cfg, summary := feishuTestConfig(), feishuTestSummary()
	injection := "[click](https://evil.example/) <at id=all> **bold**"
	cfg.Project = injection
	summary.Groups = nil
	for i := 0; i < 7; i++ {
		summary.Groups = append(summary.Groups, SQLSummaryGroup{Fingerprint: fmt.Sprintf("fp-%d %s", i, injection), Source: injection, Count: uint64(i + 1), Errors: uint64(i + 1), MaxMS: 900})
	}
	card := feishuSummaryCard(cfg, summary)
	if card.Header.Title.Tag != "plain_text" || !strings.Contains(card.Header.Title.Content, injection) {
		t.Error("header must preserve plain text")
	}
	if len(card.Elements) != 10 {
		t.Fatalf("elements = %d, want 10", len(card.Elements))
	}
	var all strings.Builder
	for _, div := range card.Elements {
		if div.Tag != "div" || div.Text.Tag != "plain_text" {
			t.Errorf("unsafe element: %+v", div)
		}
		all.WriteString(div.Text.Content)
		all.WriteByte('\n')
	}
	text := all.String()
	for _, want := range []string{"采集 SQL：10", "执行错误：2", "慢查询：3", "慢查询阈值：≥ 200 ms", "采集时间窗口（不是 SQL 实际发生时间）", summary.Start.Format(time.RFC3339), summary.End.Format(time.RFC3339), "缺少耗时：1", "不参与慢查询判断", "未纳入分组明细：4", "不等同于纯数据库执行时间", "另有 2 个分组未在 Top 5 中展示", "fp-6", "fp-2", injection} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q", want)
		}
	}
	for _, unwanted := range []string{"fp-0", "fp-1", "localhost", cfg.AppSecret, cfg.AppID} {
		if strings.Contains(text, unwanted) {
			t.Errorf("unexpected %q", unwanted)
		}
	}
	if !strings.HasPrefix(summary.Groups[0].Fingerprint, "fp-0") {
		t.Error("card mutated input group order")
	}
	data, err := json.Marshal(card)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if len(raw) != 2 || raw["header"] == nil || raw["elements"] == nil {
		t.Errorf("unexpected card shape: %s", data)
	}
	if bytes.Contains(data, []byte(`"tag":"lark_md"`)) || bytes.Contains(data, []byte(`"tag":"markdown"`)) {
		t.Error("markdown injection surface")
	}
}

func TestFeishuRecipientFailureIsolation(t *testing.T) {
	for _, failure := range []string{"api", "http", "network"} {
		t.Run(failure, func(t *testing.T) {
			cfg := feishuTestConfig()
			var mu sync.Mutex
			var delivered []string
			n := feishuTestNotifier(t, cfg, func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "/auth/") {
					io.WriteString(w, `{"code":0,"tenant_access_token":"private-token","expire":7200}`)
					return
				}
				var body map[string]string
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				mu.Lock()
				delivered = append(delivered, body["receive_id"])
				mu.Unlock()
				if body["receive_id"] == cfg.Recipients[0].Email {
					if failure == "http" {
						w.WriteHeader(http.StatusForbidden)
					}
					io.WriteString(w, `{"code":999,"msg":"private-token test-secret-not-real"}`)
					return
				}
				io.WriteString(w, `{"code":0}`)
			})
			transport := n.client.Transport
			attempts := 0
			n.client.Transport = feishuTestTransport(func(r *http.Request) (*http.Response, error) {
				if strings.Contains(r.URL.Path, "/messages") {
					attempts++
					if failure == "network" && attempts == 1 {
						return nil, errors.New("private-token test-secret-not-real one@example.test")
					}
				}
				return transport.RoundTrip(r)
			})
			logs := feishuTestRun(n, feishuTestSummary())
			if attempts != 2 {
				t.Errorf("attempts=%d, want exactly two, no retries", attempts)
			}
			if !strings.Contains(logs, "recipient 1 delivery failed") || !strings.Contains(logs, "report lost") || strings.Contains(logs, "recipient 2") {
				t.Errorf("incorrect logs: %s", logs)
			}
			for _, secret := range []string{cfg.AppSecret, "private-token", cfg.Recipients[0].Email} {
				if strings.Contains(logs, secret) {
					t.Errorf("log leaked %q", secret)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if len(delivered) == 0 || delivered[len(delivered)-1] != cfg.Recipients[1].Email {
				t.Errorf("second recipient not delivered: %v", delivered)
			}
		})
	}
}

func TestFeishuRejectsUnsafeResponses(t *testing.T) {
	cases := []struct {
		name, body string
		status     int
	}{
		{"redirect", "private-body", http.StatusFound},
		{"oversize", strings.Repeat("x", feishuMaxResponseBytes+1), http.StatusOK},
		{"malformed", "private-body", http.StatusOK},
		{"trailing", `{"code":0} {"private-body":true}`, http.StatusOK},
		{"missing-code", `{"tenant_access_token":"private-token","expire":7200}`, http.StatusOK},
		{"api-error", `{"code":5,"msg":"private-body"}`, http.StatusOK},
		{"empty-token", `{"code":0,"expire":7200}`, http.StatusOK},
		{"expired", `{"code":0,"tenant_access_token":"private-token","expire":0}`, http.StatusOK},
		{"overflow", `{"code":0,"tenant_access_token":"private-token","expire":9223372036854775807}`, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			calls := 0
			n := feishuTestNotifier(t, feishuTestConfig(), func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				calls++
				mu.Unlock()
				w.Header().Set("Location", "https://open.feishu.cn/open-apis/im/v1/messages")
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			})
			logs := feishuTestRun(n, feishuTestSummary())
			mu.Lock()
			defer mu.Unlock()
			if calls != 1 {
				t.Errorf("calls=%d; redirect followed or auth retried", calls)
			}
			if !strings.Contains(logs, "authentication failed") || !strings.Contains(logs, "report lost") {
				t.Errorf("missing failure: %s", logs)
			}
			if strings.Contains(logs, "private-body") || strings.Contains(logs, "private-token") {
				t.Errorf("response leaked: %s", logs)
			}
			if n.token != "" {
				t.Error("invalid token cached")
			}
		})
	}
}

func TestFeishuSkipAndCancellation(t *testing.T) {
	cfg := feishuTestConfig()
	n := newFeishuNotifier(cfg)
	calls := 0
	n.client.Transport = feishuTestTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		return nil, errors.New("unexpected request")
	})
	if logs := feishuTestRun(n, SQLSummary{Total: 100, MissingDuration: 10, Overflow: 2}); logs != "" {
		t.Errorf("healthy window: %s", logs)
	}
	n.cfg.Enabled = false
	feishuTestRun(n, feishuTestSummary())
	n.cfg.Enabled = true
	n.cfg.Recipients = nil
	feishuTestRun(n, feishuTestSummary())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n.run(ctx, nil, nil)
	if calls != 0 {
		t.Errorf("unexpected requests: %d", calls)
	}
	// Public lifecycle wrapper must also exit on cancellation or disabled config.
	runFeishu(ctx, cfg, nil, nil)
	cfg.Enabled = false
	runFeishu(context.Background(), cfg, nil, nil)
}

func TestFeishuCollectorToCard(t *testing.T) {
	m, err := newMonitor("unused.log", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.summaries = newSQLSummaryCollector(200, time.Now())
	for i := 0; i < 3000; i++ {
		m.Consume([]byte(`{"msg":"SQL Error","sql":"SELECT 'sensitive-value'","elapsed":"250ms","source":"repo.go:42"}` + "\n"))
	}
	var received atomic.Int64
	n := feishuTestNotifier(t, feishuTestConfig(), func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/auth/") {
			io.WriteString(w, `{"code":0,"tenant_access_token":"test-token","expire":7200}`)
			return
		}
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		var card feishuCard
		if err := json.Unmarshal([]byte(payload["content"]), &card); err != nil {
			t.Error(err)
		}
		text := ""
		for _, e := range card.Elements {
			text += e.Text.Content
		}
		for _, want := range []string{"采集 SQL：3000", "执行错误：3000", "慢查询：3000", "repo.go:42", "250 ms"} {
			if !strings.Contains(text, want) {
				t.Errorf("missing %s", want)
			}
		}
		if strings.Contains(text, "sensitive-value") || strings.Contains(text, "SELECT") {
			t.Error("SQL leaked")
		}
		received.Add(1)
		io.WriteString(w, `{"code":0}`)
	})
	feishuTestRun(n, m.summaries.drain(time.Now()))
	if received.Load() != 2 {
		t.Fatalf("got %d deliveries", received.Load())
	}
}

func TestFeishuUnknownDuration(t *testing.T) {
	c := newSQLSummaryCollector(200, time.Now())
	c.record(&Event{Kind: "error", Fingerprint: "missing"})
	card := feishuSummaryCard(feishuTestConfig(), c.drain(time.Now()))
	data, _ := json.Marshal(card)
	if !strings.Contains(string(data), "最大已知耗时：未知") {
		t.Fatal("missing duration rendered as zero")
	}
}

func TestFeishuRequestDeadlineAndCancellation(t *testing.T) {
	n := newFeishuNotifier(feishuTestConfig())
	started := make(chan struct{})
	n.client.Transport = feishuTestTransport(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) > feishuHTTPTimeout {
			t.Error("missing bounded deadline")
		}
		close(started)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := n.accessToken(ctx); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request not started")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || err.Error() != "HTTP request failed" {
			t.Errorf("unsanitized error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not stop request")
	}
}
