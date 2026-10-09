package app

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestFeishuTestCommand(t *testing.T) {
	directory := t.TempDir()
	valid := filepath.Join(directory, "valid.json")
	writeLog(t, valid, []byte(`{"feishu":{"enabled":true,"project":"p","app_id":"app","app_secret":"secret","recipients":[{"name":"One","email":"one@example.test"}],"interval_seconds":300,"slow_ms":200}}`))

	var out, errOut bytes.Buffer
	if code := Run(context.Background(), []string{"feishu", "test", "--check", "--config", valid}, &out, &errOut); code != 0 {
		t.Fatalf("check: %d %s", code, errOut.String())
	}
	for _, want := range []string{"项目：p", "one@example.test", "配置检查通过"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in %q", want, out.String())
		}
	}
	if errOut.Len() != 0 {
		t.Errorf("check stderr: %s", errOut.String())
	}

	out.Reset()
	errOut.Reset()
	if code := Run(context.Background(), []string{"feishu", "test"}, &out, &errOut); code != 2 {
		t.Fatal("missing --config accepted")
	}

	noFeishu := filepath.Join(directory, "nofeishu.json")
	writeLog(t, noFeishu, []byte(`{"source":{"mode":"file","path":"x.log"}}`))
	errOut.Reset()
	if code := Run(context.Background(), []string{"feishu", "test", "--config", noFeishu}, &out, &errOut); code != 2 || !strings.Contains(errOut.String(), "no feishu section") {
		t.Fatalf("no feishu section: %d %s", code, errOut.String())
	}

	disabled := filepath.Join(directory, "disabled.json")
	writeLog(t, disabled, []byte(`{"feishu":{"enabled":false}}`))
	errOut.Reset()
	if code := Run(context.Background(), []string{"feishu", "test", "--config", disabled}, &out, &errOut); code != 2 || !strings.Contains(errOut.String(), "disabled") {
		t.Fatalf("disabled config: %d %s", code, errOut.String())
	}

	errOut.Reset()
	if code := Run(context.Background(), []string{"feishu"}, &out, &errOut); code != 2 {
		t.Fatal("bare feishu subcommand accepted")
	}
	if strings.Contains(errOut.String(), "secret") || strings.Contains(errOut.String(), "app_secret") {
		t.Fatal("subcommand leaked config secret")
	}
}
