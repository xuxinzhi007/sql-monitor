// Command sql-monitor is a local, read-only SQL log monitor. It tails a log
// file, stdin, or a Kubernetes pod and serves a browser UI; it never connects
// to the database itself. See the repository README for configuration.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"local/sql-monitor/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := app.Run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}
