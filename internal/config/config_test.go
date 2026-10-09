package config

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func writeConfig(t *testing.T, path string, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestReadRejectsMalformed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	for _, text := range []string{"null", "[]", "{", "{} {}", `{"unknown":1}`, `{"source":{"mode":"stdin","secret":"x"}}`} {
		writeConfig(t, path, text)
		if _, _, err := Read(path); err == nil {
			t.Errorf("accepted malformed config %q", text)
		}
	}
	writeConfig(t, path, `{}`+strings.Repeat(" ", 65536))
	if _, _, err := Read(path); err == nil {
		t.Error("accepted oversized config")
	}
	fifo := filepath.Join(t.TempDir(), "fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Read(fifo); err == nil {
		t.Error("accepted non-regular config")
	}
}

func TestLoadResolvesRelativePaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "monitor.json")
	writeConfig(t, path, `{"source":{"mode":"file","path":"logs/sql.log"}}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Source.Path != filepath.Join(dir, "logs", "sql.log") {
		t.Fatalf("file path not resolved: %s", cfg.Source.Path)
	}

	writeConfig(t, path, `{"source":{"mode":"k8s","k8s":{"environments":[{"name":"dev","kubeconfig":"./kube.yml"}]}}}`)
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Source.K8s.Environments[0].Kubeconfig != filepath.Join(dir, "kube.yml") {
		t.Fatalf("kubeconfig not resolved: %s", cfg.Source.K8s.Environments[0].Kubeconfig)
	}
}

func TestLoadSourceValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "monitor.json")
	for _, text := range []string{
		`{"source":{"mode":"network"}}`,
		`{"source":{"mode":"file"}}`,
		`{"source":{"mode":"file","path":"x","k8s":{}}}`,
		`{"source":{"mode":"stdin","path":"x"}}`,
		`{"source":{"mode":"stdin","k8s":{}}}`,
		`{"source":{"mode":"k8s"}}`,
		`{"source":{"mode":"k8s","path":"x","k8s":{}}}`,
	} {
		writeConfig(t, path, text)
		if _, err := Load(path); err == nil {
			t.Errorf("accepted invalid source %q", text)
		}
	}
}

func TestLoadFeishu(t *testing.T) {
	path := filepath.Join(t.TempDir(), "monitor.json")
	writeConfig(t, path, `{"feishu":{"enabled":false}}`)
	if _, err := LoadFeishu(path); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("disabled feishu not rejected: %v", err)
	}
	writeConfig(t, path, `{}`)
	if _, err := LoadFeishu(path); err == nil {
		t.Fatal("missing feishu section accepted")
	}
	writeConfig(t, path, `{"feishu":{"enabled":true,"app_id":"a","app_secret":"s","project":"p","interval_seconds":300,"slow_ms":200,"recipients":[{"email":"a@example.com"}]}}`)
	cfg, err := LoadFeishu(path)
	if err != nil || !cfg.Enabled {
		t.Fatalf("valid feishu rejected: %v", err)
	}
}

func TestLoadFeishuRequiresPrivateMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "monitor.json")
	writeConfig(t, path, `{"feishu":{"enabled":true,"app_id":"a","app_secret":"s"}}`)
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFeishu(path); err == nil {
		t.Fatal("accepted world-readable app_secret")
	}
}

func TestFindEnvironment(t *testing.T) {
	cfg := &K8sConfig{Environments: []K8sEnvironment{{Name: "dev"}, {Name: "test"}}}
	if env, err := FindEnvironment(cfg, "test"); err != nil || env.Name != "test" {
		t.Fatalf("lookup: %+v %v", env, err)
	}
	if _, err := FindEnvironment(cfg, "nope"); err == nil || !strings.Contains(err.Error(), "available: dev, test") {
		t.Fatalf("missing env error: %v", err)
	}
	if _, err := FindEnvironment(nil, "x"); err == nil {
		t.Fatal("nil config lookup succeeded")
	}
}

func TestValidateLabel(t *testing.T) {
	if err := ValidateLabel("lane", "dev2"); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"a,b", "a=b"} {
		if err := ValidateLabel("lane", v); err == nil {
			t.Fatalf("accepted label value %q", v)
		}
	}
}
