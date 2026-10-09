// Package feishu delivers aggregated SQL-anomaly summaries to Feishu (Lark)
// recipients. It is a pluggable notifier channel: it only depends on the
// config and monitor packages, never on HTTP handlers or the app runtime.
package feishu

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"local/sql-monitor/internal/config"
	"local/sql-monitor/internal/monitor"
)

const (
	feishuTokenURL         = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
	feishuMessageURL       = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=email"
	feishuHTTPTimeout      = 10 * time.Second
	feishuMaxResponseBytes = 1 << 20
)

type Notifier struct {
	cfg          config.FeishuConfig
	client       *http.Client
	token        string
	tokenExpires time.Time
}

func NewNotifier(cfg config.FeishuConfig) *Notifier {
	return &Notifier{cfg: cfg, client: &http.Client{Timeout: feishuHTTPTimeout}}
}

// RunFeishu starts a notifier that consumes summaries until the channel closes
// or ctx is cancelled. Cancellation abandons queued reports; delivery is not
// persisted.
func RunFeishu(ctx context.Context, cfg config.FeishuConfig, summaries <-chan monitor.SQLSummary, output io.Writer) {
	NewNotifier(cfg).Run(ctx, summaries, output)
}

func (n *Notifier) Run(ctx context.Context, summaries <-chan monitor.SQLSummary, output io.Writer) {
	if !n.cfg.Enabled {
		return
	}
	if output == nil {
		output = io.Discard
	}
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case summary, ok := <-summaries:
			if !ok {
				return
			}
			if summary.Errors == 0 && summary.Slow == 0 || len(n.cfg.Recipients) == 0 {
				continue
			}
			n.sendReport(ctx, summary, output)
		}
	}
}

func (n *Notifier) sendReport(ctx context.Context, summary monitor.SQLSummary, output io.Writer) {
	card, err := json.Marshal(feishuSummaryCard(n.cfg, summary))
	if err != nil {
		fmt.Fprintln(output, "feishu: card encoding failed; report lost (no retry)")
		return
	}
	token, err := n.accessToken(ctx)
	if err != nil {
		fmt.Fprintf(output, "feishu: authentication failed (%v); report lost (no retry)\n", err)
		return
	}
	for i, recipient := range n.cfg.Recipients {
		if ctx.Err() != nil {
			return
		}
		if err := n.sendCard(ctx, token, recipient.Email, string(card)); err != nil {
			// Transport and API response text may contain credentials.
			fmt.Fprintf(output, "feishu: recipient %d delivery failed (%v); report lost for recipient (no retry)\n", i+1, err)
		}
	}
}

func (n *Notifier) accessToken(ctx context.Context) (string, error) {
	if n.token != "" && time.Now().Before(n.tokenExpires) {
		return n.token, nil
	}
	n.token, n.tokenExpires = "", time.Time{}
	var response struct {
		Code   *int   `json:"code"`
		Token  string `json:"tenant_access_token"`
		Expire int64  `json:"expire"`
	}
	// The credential-bearing request is only marshaled for the auth endpoint.
	request := struct {
		AppID     string `json:"app_id"`
		AppSecret string `json:"app_secret"`
	}{n.cfg.AppID, n.cfg.AppSecret}
	started := time.Now()
	if err := n.post(ctx, feishuTokenURL, "", request, &response); err != nil {
		return "", err
	}
	if err := feishuAPICode(response.Code); err != nil {
		return "", err
	}
	// Reject unreasonable lifetimes before converting seconds to Duration.
	if response.Token == "" || response.Expire <= 0 || response.Expire > 365*24*60*60 {
		return "", errors.New("invalid token metadata")
	}
	lifetime := time.Duration(response.Expire) * time.Second
	margin := lifetime / 10
	if margin > 30*time.Second {
		margin = 30 * time.Second
	}
	n.token = response.Token
	n.tokenExpires = started.Add(lifetime - margin)
	return n.token, nil
}

func (n *Notifier) sendCard(ctx context.Context, token, email, card string) error {
	uuid, err := feishuUUID()
	if err != nil {
		return errors.New("message identifier unavailable")
	}
	request := struct {
		ReceiveID string `json:"receive_id"`
		MsgType   string `json:"msg_type"`
		Content   string `json:"content"`
		UUID      string `json:"uuid"`
	}{email, "interactive", card, uuid}
	var response struct {
		Code *int `json:"code"`
	}
	if err := n.post(ctx, feishuMessageURL, token, request, &response); err != nil {
		return err
	}
	return feishuAPICode(response.Code)
}

func feishuUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func feishuAPICode(code *int) error {
	if code == nil {
		return errors.New("missing API status")
	}
	if *code == 0 {
		return nil
	}
	hint := map[int]string{
		10014:    "invalid App Secret",
		230013:   "bot unavailable to this user",
		99991672: "missing message-send permission",
	}[*code]
	if hint == "" {
		return fmt.Errorf("API status %d", *code)
	}
	return fmt.Errorf("API status %d (%s)", *code, hint)
}

func (n *Notifier) sendText(ctx context.Context, token, email, text string) (string, error) {
	uuid, err := feishuUUID()
	if err != nil {
		return "", errors.New("message identifier unavailable")
	}
	content, err := json.Marshal(struct {
		Text string `json:"text"`
	}{text})
	if err != nil {
		return "", errors.New("request encoding failed")
	}
	request := struct {
		ReceiveID string `json:"receive_id"`
		MsgType   string `json:"msg_type"`
		Content   string `json:"content"`
		UUID      string `json:"uuid"`
	}{email, "text", string(content), uuid}
	var response struct {
		Code *int `json:"code"`
		Data struct {
			MessageID string `json:"message_id"`
		} `json:"data"`
	}
	if err := n.post(ctx, feishuMessageURL, token, request, &response); err != nil {
		return "", err
	}
	if err := feishuAPICode(response.Code); err != nil {
		return "", err
	}
	return response.Data.MessageID, nil
}

func (n *Notifier) post(ctx context.Context, endpoint, token string, payload, result any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return errors.New("request encoding failed")
	}
	ctx, cancel := context.WithTimeout(ctx, feishuHTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return errors.New("request creation failed")
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := *n.client
	client.Timeout = feishuHTTPTimeout
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(req)
	if err != nil {
		return errors.New("HTTP request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("HTTP status %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, feishuMaxResponseBytes+1))
	if err != nil {
		return errors.New("response read failed")
	}
	if len(data) > feishuMaxResponseBytes {
		return errors.New("response too large")
	}
	if err := json.Unmarshal(data, result); err != nil {
		return errors.New("invalid JSON response")
	}
	return nil
}

type feishuPlainText struct {
	Tag     string `json:"tag"`
	Content string `json:"content"`
}

type feishuDiv struct {
	Tag  string          `json:"tag"`
	Text feishuPlainText `json:"text"`
}

type feishuCard struct {
	Header struct {
		Title    feishuPlainText `json:"title"`
		Template string          `json:"template"`
	} `json:"header"`
	Elements []feishuDiv `json:"elements"`
}

func feishuSummaryCard(cfg config.FeishuConfig, summary monitor.SQLSummary) feishuCard {
	var card feishuCard
	card.Header.Title = feishuPlainText{Tag: "plain_text", Content: "SQL 异常汇总 — " + cfg.Project}
	card.Header.Template = "orange"
	add := func(text string) {
		card.Elements = append(card.Elements, feishuDiv{Tag: "div", Text: feishuPlainText{Tag: "plain_text", Content: text}})
	}
	if summary.Errors > 0 {
		card.Header.Template = "red"
	}
	add(fmt.Sprintf("采集时间窗口（不是 SQL 实际发生时间）：\n%s — %s", summary.Start.Format(time.RFC3339), summary.End.Format(time.RFC3339)))
	add(fmt.Sprintf("采集 SQL：%d | 执行错误：%d | 慢查询：%d\n慢查询阈值：≥ %g ms；错误与慢查询可重叠。耗时为日志记录耗时，不等同于纯数据库执行时间。", summary.Total, summary.Errors, summary.Slow, cfg.SlowMS))
	add(fmt.Sprintf("缺少耗时：%d 条（不参与慢查询判断）。\n未纳入分组明细：%d 条异常（分组容量上限；仍计入总数，排名可能不完整）。", summary.MissingDuration, summary.Overflow))
	groups := append([]monitor.SQLSummaryGroup(nil), summary.Groups...)
	sort.SliceStable(groups, func(i, j int) bool {
		a, b := groups[i], groups[j]
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
	add("异常 Top 5：按错误数、慢查询数、异常次数、最大耗时排序。仅发送模板指纹和代码位置，不含 SQL 正文、参数及原始错误。")
	for i, group := range groups {
		if i == 5 {
			break
		}
		elapsed := "未知"
		if group.HasDuration {
			elapsed = fmt.Sprintf("%g ms", group.MaxMS)
		}
		add(fmt.Sprintf("%d. 模板指纹：%s\n代码位置：%s\n异常次数：%d | 错误：%d | 慢查询：%d | 最大已知耗时：%s", i+1, group.Fingerprint, group.Source, group.Count, group.Errors, group.Slow, elapsed))
	}
	if len(groups) > 5 {
		add(fmt.Sprintf("另有 %d 个分组未在 Top 5 中展示", len(groups)-5))
	}
	return card
}
