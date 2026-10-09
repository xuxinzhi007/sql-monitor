package app

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"local/sql-monitor/internal/config"
)

func TestFeishuConfigValidation(t *testing.T) {
	good := func() config.FeishuConfig {
		return config.FeishuConfig{Enabled: true, AppID: "test", AppSecret: "secret", Project: "test", IntervalSeconds: 300, SlowMS: 200, Recipients: []config.FeishuRecipient{{Email: "test@example.com"}}}
	}
	for _, change := range []func(*config.FeishuConfig){
		func(c *config.FeishuConfig) { c.IntervalSeconds = 0 },
		func(c *config.FeishuConfig) { c.SlowMS = -1 },
		func(c *config.FeishuConfig) { c.AppSecret = "" },
		func(c *config.FeishuConfig) { c.Recipients = nil },
		func(c *config.FeishuConfig) { c.Recipients[0].Email = "Name <test@example.com>" },
		func(c *config.FeishuConfig) { c.Recipients = append(c.Recipients, c.Recipients[0]) },
	} {
		c := good()
		change(&c)
		if c.Validate() == nil {
			t.Fatal("accepted invalid config")
		}
	}
	cfg := good()
	b, _ := json.Marshal(map[string]any{"feishu": cfg})
	p := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	opts, err := ParseOptions([]string{"--config", p}, io.Discard)
	if err != nil || opts.feishu == nil || !opts.feishu.Enabled {
		t.Fatalf("config failed: %v", err)
	}
	if err := os.Chmod(p, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseOptions([]string{"--config", p}, io.Discard); err == nil {
		t.Fatal("accepted world readable credentials")
	}
	if _, err := ParseOptions(nil, io.Discard); err != nil {
		t.Fatal(err)
	}
}
