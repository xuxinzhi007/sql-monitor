package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Pointer fields distinguish omitted values from explicit zero/false values.
type config struct {
	Source       *sourceConfig `json:"source,omitempty"`
	Port         *int          `json:"port,omitempty"`
	MaxEvents    *int          `json:"max_events,omitempty"`
	InitialBytes *int64        `json:"initial_bytes,omitempty"`
	Feishu       *FeishuConfig `json:"feishu,omitempty"`
}

type sourceConfig struct {
	Mode string `json:"mode"`
	Path string `json:"path,omitempty"`
}

func loadConfig(path string, opts *options) error {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return fmt.Errorf("cannot open config: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("config must be a readable regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, 64*1024+1))
	if err != nil || len(data) > 64*1024 {
		return errors.New("cannot read config or config exceeds 64 KiB")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var cfg *config
	if err := decoder.Decode(&cfg); err != nil || cfg == nil {
		return errors.New("invalid config JSON or unknown field")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("config must contain one JSON object")
	}
	if cfg.Source != nil {
		switch cfg.Source.Mode {
		case "file":
			if cfg.Source.Path == "" {
				return errors.New("file source requires path")
			}
			opts.stdin = false
			opts.logPath = cfg.Source.Path
			if !filepath.IsAbs(opts.logPath) {
				opts.logPath = filepath.Join(filepath.Dir(path), opts.logPath)
			}
		case "stdin":
			if cfg.Source.Path != "" {
				return errors.New("stdin source must not have path")
			}
			opts.stdin = true
		default:
			return errors.New("source mode must be file or stdin")
		}
	}
	if cfg.Port != nil {
		opts.port = *cfg.Port
	}
	if cfg.MaxEvents != nil {
		opts.maxEvents = *cfg.MaxEvents
	}
	if cfg.InitialBytes != nil {
		opts.initialBytes = *cfg.InitialBytes
	}
	if cfg.Feishu != nil {
		if cfg.Feishu.AppSecret != "" && info.Mode().Perm()&0077 != 0 {
			return errors.New("config containing feishu app_secret must be private; run chmod 600 on the config file")
		}
		if err := cfg.Feishu.validate(); err != nil {
			return err
		}
		opts.feishu = cfg.Feishu
	}
	return nil
}

func initConfig(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("sql-monitor init", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.Usage = func() {
		fmt.Fprintln(output, "Usage: sql-monitor init\nCreates sql-monitor.json in the current directory; never changes application configuration.")
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("init takes no positional arguments")
	}
	file, err := os.OpenFile("sql-monitor.json", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("cannot create sql-monitor.json (existing files are never overwritten): %w", err)
	}
	data := "{\n  \"source\": {\"mode\": \"file\", \"path\": \"./application.log\"},\n  \"port\": 8767,\n  \"max_events\": 2000,\n  \"initial_bytes\": 2097152\n}\n"
	_, writeErr := io.WriteString(file, data)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	fmt.Fprintln(output, "Created sql-monitor.json. Set source.path to an existing SQL log, then run sql-monitor --config sql-monitor.json.\nRelative config source paths resolve beside the config; --log paths resolve from the working directory.\nThis config only tells the monitor where to read. It does not enable SQL logging or modify application code, application config, or .gitignore.")
	// Only inspect the current directory's bounded go.mod, never application files.
	mod, err := os.OpenFile("go.mod", os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil
	}
	defer mod.Close()
	info, err := mod.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil
	}
	content, err := io.ReadAll(io.LimitReader(mod, 64*1024+1))
	if err != nil || len(content) > 64*1024 {
		return nil
	}
	module, gorm, requireBlock := false, false, false
	for _, line := range strings.Split(string(content), "\n") {
		line, _, _ = strings.Cut(line, "//")
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if fields[0] == "module" && len(fields) == 2 {
			module = true
		}
		if fields[0] == "require" {
			requireBlock = len(fields) == 2 && fields[1] == "("
			if len(fields) == 3 && fields[1] == "gorm.io/gorm" && strings.HasPrefix(fields[2], "v") {
				gorm = true
			}
		} else if requireBlock && len(fields) == 2 && fields[0] == "gorm.io/gorm" && strings.HasPrefix(fields[1], "v") {
			gorm = true
		}
		if fields[0] == ")" {
			requireBlock = false
		}
	}
	if module {
		fmt.Fprintln(output, "Detected a Go module in ./go.mod.")
	}
	if module && gorm {
		fmt.Fprintln(output, "Detected a GORM dependency in ./go.mod; actual logger configuration is unknown. Standard GORM two-line SQL logs are supported.")
	}
	return nil
}
