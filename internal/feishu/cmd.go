package feishu

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"local/sql-monitor/internal/config"
)

// TestCommand replaces the standalone feishu-message/send.py helper. It loads
// the feishu section of a config file and either validates it offline (--check)
// or sends a real test message to every configured recipient.
func TestCommand(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("sql-monitor feishu test", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "JSON config path containing a feishu section")
	checkOnly := flags.Bool("check", false, "Validate config offline; never contact Feishu or send")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: sql-monitor feishu test --config PATH [--check]")
		fmt.Fprintln(stderr, "Loads the feishu section from PATH and sends a test message to each recipient.")
		fmt.Fprintln(stderr, "--check validates config only, with no network access and no sending.")
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "feishu test takes no positional arguments")
		return 2
	}
	if *configPath == "" {
		fmt.Fprintln(stderr, "feishu test requires --config PATH")
		return 2
	}
	cfg, err := config.LoadFeishu(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, "config:", err)
		return 2
	}
	names := make([]string, len(cfg.Recipients))
	for i, r := range cfg.Recipients {
		names[i] = r.Name + " <" + r.Email + ">"
	}
	fmt.Fprintf(stdout, "项目：%s\n收件人：%s\n采集窗口：%ds | 慢查询阈值：≥ %g ms\n", cfg.Project, strings.Join(names, ", "), cfg.IntervalSeconds, cfg.SlowMS)
	if *checkOnly {
		fmt.Fprintln(stdout, "配置检查通过；未联网，未验证凭证、权限或收件账号。")
		return 0
	}
	notifier := NewNotifier(*cfg)
	ctx := context.Background()
	token, err := notifier.accessToken(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "认证失败：", err)
		return 1
	}
	failed := 0
	for i, r := range cfg.Recipients {
		text := fmt.Sprintf("SQL 监控飞书通知测试 — %s（发送于 %s）", cfg.Project, time.Now().Format(time.RFC3339))
		messageID, err := notifier.sendText(ctx, token, r.Email, text)
		if err != nil {
			fmt.Fprintf(stderr, "收件人 %d (%s) 发送失败：%v\n", i+1, r.Name, err)
			failed++
			continue
		}
		fmt.Fprintf(stdout, "收件人 %d (%s <%s>) 发送成功，消息 ID：%s\n", i+1, r.Name, r.Email, messageID)
	}
	if failed > 0 {
		return 1
	}
	return 0
}
