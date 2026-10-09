package app

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"local/sql-monitor/internal/config"
)

func TestConfigPrecedenceAndPaths(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "monitor.json")
	writeLog(t, path, []byte(`{"source":{"mode":"file","path":"logs/sql.log"},"port":9000,"max_events":30,"initial_bytes":0}`))
	opts, err := ParseOptions([]string{"--config", path}, io.Discard)
	if err != nil || opts.logPath != filepath.Join(directory, "logs/sql.log") || opts.stdin || opts.port != 9000 || opts.maxEvents != 30 || opts.initialBytes != 0 {
		t.Fatalf("config: %+v %v", opts, err)
	}
	opts, err = ParseOptions([]string{"--port", "8768", "--config", path, "--max-events", "4", "--initial-bytes", "12", "--log", "relative.log"}, io.Discard)
	if err != nil || opts.logPath != "relative.log" || opts.port != 8768 || opts.maxEvents != 4 || opts.initialBytes != 12 || opts.stdin {
		t.Fatalf("flag precedence: %+v %v", opts, err)
	}
	opts, err = ParseOptions([]string{"--config", path, "--stdin"}, io.Discard)
	if err != nil || !opts.stdin {
		t.Fatal("stdin must override configured file")
	}
	writeLog(t, path, []byte(`{"source":{"mode":"stdin"}}`))
	opts, err = ParseOptions([]string{"--config", path}, io.Discard)
	if err != nil || !opts.stdin || opts.port != 8767 {
		t.Fatal("stdin config/default merge")
	}
	opts, err = ParseOptions([]string{"--config", path, "--log", "file.log"}, io.Discard)
	if err != nil || opts.stdin || opts.logPath != "file.log" {
		t.Fatal("explicit log must override stdin config")
	}
	opts, err = ParseOptions([]string{"--config", path, "--stdin=false"}, io.Discard)
	if err != nil || opts.stdin {
		t.Fatal("explicit false ignored")
	}
	writeLog(t, path, []byte(`{"source":{"mode":"file","path":"/tmp/generic.log"}}`))
	opts, err = ParseOptions([]string{"--config", path}, io.Discard)
	if err != nil || opts.logPath != "/tmp/generic.log" {
		t.Fatal("absolute path changed")
	}
	for _, args := range [][]string{{"--stdin", "--log", "x"}, {"--log=x", "--stdin=false"}, {"--config", ""}, {"--config", path + "missing"}} {
		if _, err := ParseOptions(args, io.Discard); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

func TestInvalidConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "monitor.json")
	for _, text := range []string{"null", "[]", "{", "{} {}", `{"unknown":"do not echo me"}`, `{"port":0}`, `{"max_events":2001}`, `{"initial_bytes":-1}`, `{"source":{"mode":"network"}}`, `{"source":{"mode":"file"}}`, `{"source":{"mode":"stdin","path":"x"}}`, `{"source":{"mode":"stdin","secret":"do not echo me"}}`, "{}" + strings.Repeat(" ", 65536)} {
		writeLog(t, path, []byte(text))
		_, err := ParseOptions([]string{"--config", path}, io.Discard)
		if err == nil {
			t.Errorf("accepted invalid config %q", text[:min(len(text), 100)])
		} else if strings.Contains(err.Error(), "do not echo me") {
			t.Fatal("config error leaked payload")
		}
	}
}

func TestInitNoOverwriteNoImplicitConfig(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	writeLog(t, filepath.Join(directory, "go.mod"), []byte("module example.invalid/demo\n\ngo 1.24\nrequire (\n gorm.io/gorm v1.31.1 // indirect\n)\n"))
	original := []byte("keep\n")
	writeLog(t, filepath.Join(directory, ".gitignore"), original)
	var output bytes.Buffer
	if code := Run(context.Background(), []string{"init"}, io.Discard, &output); code != 0 {
		t.Fatalf("init: %d %s", code, output.String())
	}
	if !strings.Contains(output.String(), "Detected a Go module") || !strings.Contains(output.String(), "Detected a GORM dependency") || !strings.Contains(output.String(), "actual logger configuration is unknown") {
		t.Fatal("bounded go.mod detection missing")
	}
	path := filepath.Join(directory, "sql-monitor.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("unsafe config permissions")
	}
	if err := config.Init(nil, io.Discard); err == nil {
		t.Fatal("overwrote existing config")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("existing config changed")
	}
	ignore, _ := os.ReadFile(filepath.Join(directory, ".gitignore"))
	if !bytes.Equal(ignore, original) {
		t.Fatal("gitignore modified")
	}
	opts, err := ParseOptions(nil, io.Discard)
	if err != nil || opts.logPath != "/tmp/data-service-local.log" {
		t.Fatal("implicitly loaded generated config")
	}
	opts, err = ParseOptions([]string{"--config", path}, io.Discard)
	if err != nil || opts.logPath != filepath.Join(directory, "application.log") {
		t.Fatal("generated config unusable")
	}
	if _, err := os.Stat(opts.logPath); !os.IsNotExist(err) {
		t.Fatal("init created source log")
	}
}

func TestConfigNonregularAndModuleProbe(t *testing.T) {
	directory := t.TempDir()
	fifo := filepath.Join(directory, "config.fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{directory, fifo} {
		if _, err := ParseOptions([]string{"--config", path}, io.Discard); err == nil {
			t.Fatal("accepted nonregular config")
		}
	}
	t.Chdir(directory)
	if err := os.Symlink(fifo, filepath.Join(directory, "go.mod")); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := config.Init(nil, &output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "Detected") {
		t.Fatal("followed module symlink")
	}
}

func TestInitRefusesSymlinkAndArguments(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	target := filepath.Join(directory, "target")
	if err := os.Symlink(target, filepath.Join(directory, "sql-monitor.json")); err != nil {
		t.Fatal(err)
	}
	if err := config.Init(nil, io.Discard); err == nil {
		t.Fatal("followed symlink")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatal("symlink target touched")
	}
	if err := config.Init([]string{"extra"}, io.Discard); err == nil {
		t.Fatal("accepted extra argument")
	}
}

func TestK8sConfigParsing(t *testing.T) {
	kubectl := writeFakeKubectl(t, "exit 0\n")
	path := filepath.Join(t.TempDir(), "monitor.json")
	writeLog(t, path, []byte(fmt.Sprintf(`{"source":{"mode":"k8s","k8s":{"default":"dev2","environments":[{"name":"dev2","kubeconfig":"./kube.yml","context":"ctx","namespace":"ns-cloud-dev","app":"data-service","lane":"dev2","tail":0,"kubectl":%q},{"name":"test","app":"data-service","lane":"test"}]}}}`, kubectl)))
	opts, err := ParseOptions([]string{"--config", path}, io.Discard)
	if err != nil || opts.k8s == nil || opts.stdin {
		t.Fatalf("k8s config: %+v %v", opts, err)
	}
	target := opts.k8s
	if target.Environment != "dev2" || target.App != "data-service" || target.Lane != "dev2" || target.Namespace != "ns-cloud-dev" || target.Context != "ctx" || target.Tail != 0 {
		t.Fatalf("k8s target: %+v", target)
	}
	if target.Kubeconfig != filepath.Join(filepath.Dir(path), "kube.yml") {
		t.Fatalf("relative kubeconfig not resolved: %s", target.Kubeconfig)
	}
	opts, err = ParseOptions([]string{"--config", path, "--k8s-env", "test"}, io.Discard)
	if err != nil || opts.k8s == nil || opts.k8s.Environment != "test" || opts.k8s.Lane != "test" {
		t.Fatalf("k8s-env selection: %+v %v", opts, err)
	}
}

func TestK8sConfigErrors(t *testing.T) {
	kubectl := writeFakeKubectl(t, "exit 0\n")
	path := filepath.Join(t.TempDir(), "monitor.json")
	cases := []string{
		`{"source":{"mode":"k8s"}}`,
		`{"source":{"mode":"file","path":"x.log","k8s":{"environments":[]}}}`,
		`{"source":{"mode":"stdin","k8s":{"environments":[]}}}`,
		fmt.Sprintf(`{"source":{"mode":"k8s","k8s":{"default":"dev2","environments":[{"name":"dev2","app":"a","kubectl":%q,"tail":-1}]}}}`, kubectl),
		fmt.Sprintf(`{"source":{"mode":"k8s","k8s":{"default":"dev2","environments":[{"name":"dev2","lane":"a,b","kubectl":%q}]}}}`, kubectl),
		fmt.Sprintf(`{"source":{"mode":"k8s","k8s":{"default":"dev2","environments":[{"name":"dev2","kubectl":%q}]}}}`, kubectl),
		`{"source":{"mode":"k8s","k8s":{"environments":[{"name":"a","app":"x","bogus":1}]}}}`,
	}
	for _, text := range cases {
		writeLog(t, path, []byte(text))
		if _, err := ParseOptions([]string{"--config", path}, io.Discard); err == nil {
			t.Errorf("accepted invalid k8s config %q", text[:min(len(text), 80)])
		}
	}
	writeLog(t, path, []byte(fmt.Sprintf(`{"source":{"mode":"k8s","k8s":{"default":"dev2","environments":[{"name":"dev2","app":"a","kubectl":%q}]}}}`, kubectl)))
	if _, err := ParseOptions([]string{"--config", path, "--k8s-env", "nope"}, io.Discard); err == nil || !strings.Contains(err.Error(), "available") {
		t.Fatal("--k8s-env with unknown name not rejected with available list")
	}
}
