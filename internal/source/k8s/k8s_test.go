package k8s

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"local/sql-monitor/internal/monitor"
)

// writeFakeKubectl writes an executable shell script and returns its path.
func writeFakeKubectl(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kubectl")
	script := "#!/bin/sh\n" + body + "\n"
	if err := os.WriteFile(path, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func testSink(t *testing.T) *monitor.Monitor {
	t.Helper()
	m, err := monitor.New(filepath.Join(t.TempDir(), "unused"), 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return m
}

func TestTargetSelectorArgsDescribe(t *testing.T) {
	target := Target{
		Kubectl:    "/usr/bin/kubectl",
		Kubeconfig: "/kube/config",
		Context:    "ctx",
		Namespace:  "ns-cloud-dev",
		App:        "data-service",
		Lane:       "dev2",
		Container:  "main",
		Tail:       200,
	}
	if got := target.Selector(); got != "app=data-service,x2robot.io/lane=dev2" {
		t.Fatalf("selector: %s", got)
	}
	want := []string{"--kubeconfig", "/kube/config", "--context", "ctx", "-n", "ns-cloud-dev", "logs", "-f", "--tail=50", "-l", "app=data-service,x2robot.io/lane=dev2", "--container=main"}
	if got := target.Args(50, true); !reflect.DeepEqual(got, want) {
		t.Fatalf("args: %v", got)
	}
	if got := target.Describe(); got != "ns-cloud-dev lane=dev2 app=data-service" {
		t.Fatalf("describe: %s", got)
	}
	minimal := Target{App: "app", Lane: "lane"}
	if got := minimal.Args(0, false); !reflect.DeepEqual(got, []string{"logs", "--tail=0", "-l", "app=app,x2robot.io/lane=lane"}) {
		t.Fatalf("minimal args: %v", got)
	}
}

func TestLaneLabelOverride(t *testing.T) {
	if got := (Target{App: "app", Lane: "dev2"}).Selector(); got != "app=app,x2robot.io/lane=dev2" {
		t.Fatalf("default lane label: %s", got)
	}
	if got := (Target{App: "app", Lane: "dev2", LaneLabel: "team.io/rail"}).Selector(); got != "app=app,team.io/rail=dev2" {
		t.Fatalf("custom lane label: %s", got)
	}
}

func TestStreamFeedsEvents(t *testing.T) {
	kubectl := writeFakeKubectl(t, `
printf '%s\n' '{"msg":"SQL Query","sql":"SELECT 1","level":"DEBUG"}'
printf '%s\n' '{"msg":"SQL Query","sql":"SELECT 2","level":"DEBUG"}'
exit 0
`)
	m := testSink(t)
	target := Target{Kubectl: kubectl, Namespace: "ns", App: "data-service", Lane: "dev2", Tail: 200}
	if err := stream(context.Background(), m, target, target.Tail); err != nil {
		t.Fatalf("stream: %v", err)
	}
	snapshot := m.Snapshot()
	if snapshot.Status.SQLObserved != 2 || snapshot.Status.ParseErrors != 0 || snapshot.Status.BytesRead == 0 {
		t.Fatalf("events not fed: %+v", snapshot.Status)
	}
	if raw, ok := m.RawEvent(1); !ok || raw.SQL != "SELECT 1" {
		t.Fatal("raw event 1 missing")
	}
}

func TestRunReconnectUsesTailZero(t *testing.T) {
	dir := t.TempDir()
	argFile := filepath.Join(dir, "args")
	t.Setenv("K8S_ARGFILE", argFile)
	t.Setenv("K8S_FLAGFILE", filepath.Join(dir, "first"))
	kubectl := writeFakeKubectl(t, `
echo "$@" >> "$K8S_ARGFILE"
if [ -f "$K8S_FLAGFILE" ]; then
  exit 0
fi
touch "$K8S_FLAGFILE"
printf '%s\n' '{"msg":"SQL Query","sql":"SELECT 1","level":"DEBUG"}'
exit 0
`)
	m := testSink(t)
	target := Target{Kubectl: kubectl, Namespace: "ns", App: "data-service", Lane: "dev2", Tail: 200}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); target.Run(ctx, m) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		data, err := os.ReadFile(argFile)
		if err == nil && bytes.Count(data, []byte("\n")) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reconnect not observed; args=%q", data)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
	data, _ := os.ReadFile(argFile)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected two invocations: %q", lines)
	}
	if !strings.Contains(lines[0], "--tail=200") || !strings.Contains(lines[1], "--tail=0") {
		t.Fatalf("reconnect tail wrong: first=%q second=%q", lines[0], lines[1])
	}
}

func TestRunStderrBecomesError(t *testing.T) {
	kubectl := writeFakeKubectl(t, `
echo "connection refused to pod" >&2
exit 1
`)
	m := testSink(t)
	target := Target{Kubectl: kubectl, App: "data-service", Lane: "dev2", Tail: 0}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); target.Run(ctx, m) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		status := m.Snapshot().Status
		if status.InputState == "error" && strings.Contains(status.Error, "connection refused") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("error state not set: %+v", m.Snapshot().Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if m.Snapshot().Status.SQLObserved != 0 {
		t.Fatal("stderr content became log events")
	}
	cancel()
	<-done
}

func TestRunCancel(t *testing.T) {
	kubectl := writeFakeKubectl(t, "exec sleep 100\n")
	m := testSink(t)
	target := Target{Kubectl: kubectl, App: "data-service", Lane: "dev2", Tail: 0}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); target.Run(ctx, m) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
