// Package k8s implements a monitor.Source that follows Kubernetes pod logs via
// kubectl, reconnecting on exit (rolling releases replace the pod, which
// terminates kubectl). Reconnects use tail=0 so the backfill is not replayed.
package k8s

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"local/sql-monitor/internal/monitor"
)

const (
	appLabel    = "app"
	laneLabel   = "x2robot.io/lane"
	DefaultTail = 200
	streamChunk = 64 * 1024
	stderrLimit = 64 * 1024
)

var errMonitorClosed = errors.New("monitor closed")

type Target struct {
	Environment string // display only
	Kubectl     string
	Kubeconfig  string
	Context     string
	Namespace   string
	App         string
	Lane        string
	LaneLabel   string
	Container   string
	Tail        int
}

// Selector returns the label selector, e.g. app=data-service,x2robot.io/lane=dev2.
func (t Target) Selector() string {
	label := t.LaneLabel
	if label == "" {
		label = laneLabel
	}
	return appLabel + "=" + t.App + "," + label + "=" + t.Lane
}

// Args builds kubectl arguments (excluding the kubectl binary itself).
func (t Target) Args(tail int, follow bool) []string {
	args := make([]string, 0, 12)
	if t.Kubeconfig != "" {
		args = append(args, "--kubeconfig", t.Kubeconfig)
	}
	if t.Context != "" {
		args = append(args, "--context", t.Context)
	}
	if t.Namespace != "" {
		args = append(args, "-n", t.Namespace)
	}
	args = append(args, "logs")
	if follow {
		args = append(args, "-f")
	}
	args = append(args, fmt.Sprintf("--tail=%d", tail), "-l", t.Selector())
	if t.Container != "" {
		args = append(args, "--container="+t.Container)
	}
	return args
}

// Describe returns a human-readable source description, e.g.
// "ns-cloud-dev lane=dev2 app=data-service".
func (t Target) Describe() string {
	parts := make([]string, 0, 3)
	if t.Namespace != "" {
		parts = append(parts, t.Namespace)
	}
	if t.Lane != "" {
		parts = append(parts, "lane="+t.Lane)
	}
	if t.App != "" {
		parts = append(parts, "app="+t.App)
	}
	return strings.Join(parts, " ")
}

// Run implements monitor.Source.
func (t Target) Run(ctx context.Context, sink monitor.Sink) {
	run(ctx, sink, t)
}

func run(ctx context.Context, sink monitor.Sink, target Target) {
	defer sink.Close()
	sink.SetSource("k8s", target.Describe(), "reading")
	tail := target.Tail
	for {
		if ctx.Err() != nil {
			return
		}
		err := stream(ctx, sink, target, tail)
		tail = 0
		if ctx.Err() != nil || errors.Is(err, errMonitorClosed) {
			return
		}
		if err != nil {
			sink.SetInputState("error", err.Error())
		} else {
			sink.SetInputState("reading", "")
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func stream(ctx context.Context, sink monitor.Sink, target Target, tail int) error {
	cmd := exec.CommandContext(ctx, target.Kubectl, target.Args(tail, true)...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr := &boundedBuffer{limit: stderrLimit}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	closed := false
	chunk := make([]byte, streamChunk)
	for {
		n, readErr := stdout.Read(chunk)
		if n > 0 {
			if !sink.Feed(chunk[:n]) {
				closed = true
				_ = cmd.Process.Kill()
				break
			}
		}
		if readErr != nil {
			break
		}
	}
	waitErr := cmd.Wait()
	if closed {
		return errMonitorClosed
	}
	if ctx.Err() != nil {
		return nil
	}
	if waitErr != nil {
		if message := strings.TrimSpace(stderr.String()); message != "" {
			return errors.New(message)
		}
		return waitErr
	}
	return nil
}

// boundedBuffer keeps at most limit bytes of stderr; overflow is dropped so
// kubectl never blocks on a full pipe. kubectl diagnostics are not log lines.
type boundedBuffer struct {
	data  []byte
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if len(b.data) < b.limit {
		room := b.limit - len(b.data)
		if n > room {
			p = p[:room]
		}
		b.data = append(b.data, p...)
	}
	return n, nil
}

func (b *boundedBuffer) String() string {
	return string(b.data)
}
