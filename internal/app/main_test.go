package app

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestCLIOptionsAndOccupiedPort(t *testing.T) {
	var output bytes.Buffer
	opts, err := ParseOptions(nil, &output)
	if err != nil || !reflect.DeepEqual(opts, Options{logPath: "/tmp/data-service-local.log", port: 8767, maxEvents: 2000, initialBytes: 2 * 1024 * 1024}) {
		t.Fatalf("bad defaults: %+v %v", opts, err)
	}
	_, err = ParseOptions([]string{"--help"}, &output)
	if !errors.Is(err, flag.ErrHelp) || !strings.Contains(output.String(), "--initial-bytes") {
		t.Fatal("CLI help missing")
	}
	for _, args := range [][]string{{"--max-events", "2001"}, {"--max-events", "0"}, {"--port", "0"}, {"--port", "65536"}, {"--initial-bytes", "-1"}, {"--log", ""}, {"--port", "bad"}, {"--unknown"}, {"extra"}} {
		if code := Run(context.Background(), args, io.Discard, io.Discard); code != 2 {
			t.Errorf("invalid args %v returned %d", args, code)
		}
	}
	var helpOut, helpErr bytes.Buffer
	if code := Run(context.Background(), []string{"--help"}, &helpOut, &helpErr); code != 0 || !strings.Contains(helpOut.String(), "--initial-bytes") || helpErr.Len() != 0 {
		t.Fatal("help exit status or output stream")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	path := filepath.Join(t.TempDir(), "not-created.log")
	port := listener.Addr().(*net.TCPAddr).Port
	output.Reset()
	if code := Run(context.Background(), []string{"--log", path, "--port", strconv.Itoa(port)}, io.Discard, &output); code != 1 {
		t.Fatalf("occupied port returned %d", code)
	}
	for _, text := range []string{"Cannot bind", "existing UI", "--port", strconv.Itoa(port)} {
		if !strings.Contains(output.String(), text) {
			t.Errorf("occupied-port message missing %q", text)
		}
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("occupied port touched logfile")
	}
}

func TestK8sFlagsPrecedenceAndMutex(t *testing.T) {
	kubectl := writeFakeKubectl(t, "exit 0\n")
	path := filepath.Join(t.TempDir(), "monitor.json")
	writeLog(t, path, []byte(fmt.Sprintf(`{"source":{"mode":"k8s","k8s":{"default":"dev2","environments":[{"name":"dev2","namespace":"ns","app":"data-service","lane":"dev2","kubectl":%q},{"name":"dev3","namespace":"ns","app":"data-service","lane":"dev3","kubectl":%q}]}}}`, kubectl, kubectl)))

	opts, err := ParseOptions([]string{"--config", path}, io.Discard)
	if err != nil || opts.k8s == nil || opts.k8s.Lane != "dev2" || opts.k8s.Environment != "dev2" {
		t.Fatalf("default env: %+v %v", opts, err)
	}
	opts, err = ParseOptions([]string{"--config", path, "--k8s-env", "dev3"}, io.Discard)
	if err != nil || opts.k8s.Lane != "dev3" || opts.k8s.Environment != "dev3" {
		t.Fatalf("env override: %+v %v", opts, err)
	}
	opts, err = ParseOptions([]string{"--config", path, "--k8s-env", "dev2", "--k8s-lane", "dev1"}, io.Discard)
	if err != nil || opts.k8s.Lane != "dev1" || opts.k8s.Environment != "dev2" {
		t.Fatalf("lane override: %+v %v", opts, err)
	}
	opts, err = ParseOptions([]string{"--config", path, "--k8s-namespace", "other"}, io.Discard)
	if err != nil || opts.k8s.Namespace != "other" {
		t.Fatalf("namespace override: %+v %v", opts, err)
	}
	opts, err = ParseOptions([]string{"--k8s-kubectl", kubectl, "--k8s-app", "data-service", "--k8s-lane", "dev2"}, io.Discard)
	if err != nil || opts.k8s == nil || opts.k8s.App != "data-service" || opts.k8s.Lane != "dev2" {
		t.Fatalf("standalone k8s: %+v %v", opts, err)
	}
	for _, args := range [][]string{
		{"--k8s-app", "a", "--stdin"},
		{"--k8s-lane", "l", "--log", "x.log"},
		{"--k8s-env", "dev2", "--config", path, "--log", "x.log"},
	} {
		if _, err := ParseOptions(args, io.Discard); err == nil {
			t.Fatalf("accepted k8s flags with stdin/log: %v", args)
		}
	}
	if _, err := ParseOptions([]string{"--k8s-env", "dev2"}, io.Discard); err == nil {
		t.Fatal("accepted --k8s-env without a k8s config")
	}
}
