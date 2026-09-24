package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

//go:embed index.html
var indexHTML []byte

//go:embed logs.html
var logsHTML []byte

//go:embed live.html
var liveHTML []byte

var liveReadSlots = make(chan struct{}, 2)

var rawRoute = regexp.MustCompile(`^/api/event/([0-9]{1,20})\?raw=1$`)

const contentSecurityPolicy = "default-src 'none'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"

type monitorHandler struct {
	monitor *Monitor
	port    int
}

func (h monitorHandler) trusted(r *http.Request) bool {
	host := strings.ToLower(r.Host)
	port := strconv.Itoa(h.port)
	if host != "127.0.0.1:"+port && host != "localhost:"+port && !(h.port == 80 && (host == "127.0.0.1" || host == "localhost")) {
		return false
	}
	if len(r.Header.Values("Host")) != 0 {
		return false
	}
	origins := r.Header.Values("Origin")
	if len(origins) > 0 && (len(origins) != 1 || strings.ToLower(origins[0]) != "http://"+host) {
		return false
	}
	for _, site := range r.Header.Values("Sec-Fetch-Site") {
		if strings.EqualFold(site, "cross-site") {
			return false
		}
	}
	return true
}

func sendData(w http.ResponseWriter, r *http.Request, code int, data []byte, contentType string) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", contentSecurityPolicy)
	w.Header().Set("Connection", "close")
	r.Close = true
	w.WriteHeader(code)
	if r.Method != http.MethodHead {
		_, _ = w.Write(data)
	}
}

func sendJSON(w http.ResponseWriter, r *http.Request, code int, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		code, data = http.StatusInternalServerError, []byte(`{"error":"Cannot encode response"}`)
	}
	sendData(w, r, code, data, "application/json; charset=utf-8")
}

func (h monitorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.trusted(r) {
		sendJSON(w, r, http.StatusForbidden, map[string]string{"error": "Untrusted host or origin"})
		return
	}
	if importEndpoint(r.RequestURI) {
		h.serveImport(w, r)
		return
	}
	if r.Method != http.MethodGet {
		sendJSON(w, r, http.StatusMethodNotAllowed, map[string]string{"error": "Read-only server; use GET"})
		return
	}
	switch r.RequestURI {
	case "/", "/overview", "/requests", "/live":
		sendData(w, r, http.StatusOK, liveHTML, "text/html; charset=utf-8")
	case "/sql":
		sendData(w, r, http.StatusOK, indexHTML, "text/html; charset=utf-8")
	case "/api/live?raw=1":
		select {
		case liveReadSlots <- struct{}{}:
			defer func() { <-liveReadSlots }()
			sendJSON(w, r, http.StatusOK, h.monitor.live.snapshot())
		default:
			sendJSON(w, r, http.StatusTooManyRequests, map[string]string{"error": "Too many live snapshot readers"})
		}
	case "/api/status":
		sendJSON(w, r, http.StatusOK, h.monitor.StatusSnapshot())
	case "/api/state":
		sendJSON(w, r, http.StatusOK, h.monitor.Snapshot())
	case "/logs":
		sendData(w, r, http.StatusOK, logsHTML, "text/html; charset=utf-8")
	default:
		if strings.HasPrefix(r.RequestURI, "/api/logs/") && r.URL.EscapedPath() == r.URL.Path {
			h.serveLogs(w, r)
			return
		}
		match := rawRoute.FindStringSubmatch(r.RequestURI)
		if match == nil {
			sendJSON(w, r, http.StatusNotFound, map[string]string{"error": "Not found"})
			return
		}
		id, err := strconv.ParseUint(match[1], 10, 64)
		if err == nil {
			if raw, found := h.monitor.RawEvent(id); found {
				sendJSON(w, r, http.StatusOK, raw)
				return
			}
		}
		sendJSON(w, r, http.StatusNotFound, map[string]string{"error": "Event not found"})
	}
}

type options struct {
	logPath      string
	port         int
	maxEvents    int
	initialBytes int64
	stdin        bool
	feishu       *FeishuConfig
}

func parseOptions(args []string, output io.Writer) (options, error) {
	var opts options
	var configPath string
	flags := flag.NewFlagSet("sql-monitor", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&opts.logPath, "log", "/tmp/data-service-local.log", "Log file to follow (read-only)")
	flags.IntVar(&opts.port, "port", 8767, "Local HTTP port")
	flags.IntVar(&opts.maxEvents, "max-events", 2000, "In-memory event capacity (1..2000)")
	flags.Int64Var(&opts.initialBytes, "initial-bytes", 2*1024*1024, "Bytes to read from the initial log tail")
	flags.BoolVar(&opts.stdin, "stdin", false, "Read stdin until EOF; keep the UI alive")
	flags.StringVar(&configPath, "config", "", "Explicit JSON config path (never loaded automatically)")
	flags.Usage = func() {
		fmt.Fprintln(output, "Local, read-only SQL log monitor. No database access.")
		fmt.Fprintln(output, "Usage: sql-monitor init | sql-monitor [--config PATH] [--stdin | --log PATH] [--port PORT] [--max-events N] [--initial-bytes N]")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return opts, err
	}
	if flags.NArg() != 0 {
		return opts, errors.New("unexpected positional arguments")
	}
	explicit := make(map[string]bool)
	flags.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	if explicit["stdin"] && explicit["log"] {
		return opts, errors.New("--stdin and --log cannot be supplied together")
	}
	cli := opts
	if explicit["config"] {
		if err := loadConfig(configPath, &opts); err != nil {
			return opts, err
		}
	}
	if explicit["log"] {
		opts.logPath, opts.stdin = cli.logPath, false
	}
	if explicit["stdin"] {
		opts.stdin = cli.stdin
	}
	if explicit["port"] {
		opts.port = cli.port
	}
	if explicit["max-events"] {
		opts.maxEvents = cli.maxEvents
	}
	if explicit["initial-bytes"] {
		opts.initialBytes = cli.initialBytes
	}
	if opts.port < 1 || opts.port > 65535 || opts.maxEvents < 1 || opts.maxEvents > 2000 || opts.initialBytes < 0 {
		return opts, errors.New("port must be 1..65535, max-events 1..2000, initial-bytes nonnegative")
	}
	if opts.logPath == "" {
		return opts, errors.New("log path must not be empty")
	}
	return opts, nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	stderr = &notificationLog{output: stderr}
	if len(args) > 0 && args[0] == "init" {
		err := initConfig(args[1:], stderr)
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		return 0
	}
	var flagOutput strings.Builder
	opts, err := parseOptions(args, &flagOutput)
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprint(stdout, flagOutput.String())
		return 0
	}
	if err != nil {
		if flagOutput.Len() > 0 {
			fmt.Fprint(stderr, flagOutput.String())
		} else {
			fmt.Fprintln(stderr, err)
		}
		return 2
	}
	monitor, err := newMonitor(opts.logPath, opts.maxEvents, opts.initialBytes)
	if err != nil {
		fmt.Fprintln(stderr, "Cannot initialize monitor:", err)
		return 1
	}
	defer monitor.Close()
	var input *os.File
	if opts.stdin {
		input, err = os.OpenFile("/dev/stdin", os.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			fmt.Fprintln(stderr, "Cannot open stdin")
			return 1
		}
		defer input.Close()
		monitor.status.SourceMode, monitor.status.Path = "stdin", ""
	}
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(opts.port))
	listener, err := net.Listen("tcp4", address)
	if err != nil {
		fmt.Fprintf(stderr, "Cannot bind local HTTP server at http://%s/: %v\nIf a monitor is already running, use its existing UI or choose another --port.\n", address, err)
		return 1
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	server := &http.Server{
		Handler:           monitorHandler{monitor: monitor, port: port},
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 5 * time.Second,
		MaxHeaderBytes: 64 * 1024, ErrorLog: log.New(io.Discard, "", 0),
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	notifyDone := make(chan struct{})
	if opts.feishu != nil && opts.feishu.Enabled {
		monitor.summaries = newSQLSummaryCollector(opts.feishu.SlowMS, time.Now())
		reports := make(chan SQLSummary, 2)
		summaryDone := make(chan struct{})
		go func() {
			defer close(summaryDone)
			runSQLSummaries(ctx, monitor.summaries, time.Duration(opts.feishu.IntervalSeconds)*time.Second, reports, stderr)
		}()
		go func() {
			defer close(notifyDone)
			runFeishu(ctx, *opts.feishu, reports, stderr)
			<-summaryDone
		}()
		fmt.Fprintf(stderr, "Feishu enabled: %d recipients, %ds ingestion windows, slow >= %.2fms; SQL parameters are not sent. Pending reports are not persisted.\n", len(opts.feishu.Recipients), opts.feishu.IntervalSeconds, opts.feishu.SlowMS)
	} else {
		close(notifyDone)
	}
	tailDone := make(chan struct{})
	go func() {
		defer close(tailDone)
		if input != nil {
			monitor.RunInput(ctx, input)
		} else {
			monitor.Run(ctx)
		}
	}()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	if opts.stdin {
		fmt.Fprintf(stderr, "SQL Monitor: http://127.0.0.1:%d/\nSource: stdin (UI stays alive after EOF)\nCapacity: %d events (in memory)\n", port, opts.maxEvents)
	} else {
		fmt.Fprintf(stdout, "SQL Monitor: http://127.0.0.1:%d/\nLog file: %s\nCapacity: %d events (in memory)\n", port, monitor.Snapshot().Status.Path, opts.maxEvents)
	}
	code := 0
	select {
	case <-ctx.Done():
	case err := <-serveDone:
		if !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(stderr, "HTTP server stopped:", err)
			code = 1
		}
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
	}
	<-tailDone
	<-notifyDone
	return code
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}
