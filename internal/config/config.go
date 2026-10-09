// Package config loads and validates sql-monitor's JSON configuration. It has
// no dependencies on the rest of the application, so callers can consume it
// directly without importing the monitor, sources, or web layers.
package config

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

// Config is the root of the JSON configuration. Pointer fields distinguish an
// omitted value from an explicit zero/false value.
type Config struct {
	Source       *Source       `json:"source,omitempty"`
	Port         *int          `json:"port,omitempty"`
	MaxEvents    *int          `json:"max_events,omitempty"`
	InitialBytes *int64        `json:"initial_bytes,omitempty"`
	Feishu       *FeishuConfig `json:"feishu,omitempty"`
}

// Source selects where log lines come from.
type Source struct {
	Mode string     `json:"mode"`
	Path string     `json:"path,omitempty"`
	K8s  *K8sConfig `json:"k8s,omitempty"`
}

// K8sConfig configures the kubectl-based source. LaneLabel overrides the pod
// label key used to distinguish lanes; an empty value keeps the default.
type K8sConfig struct {
	LaneLabel    string           `json:"lane_label,omitempty"`
	Environments []K8sEnvironment `json:"environments,omitempty"`
	Default      string           `json:"default,omitempty"`
}

type K8sEnvironment struct {
	Name       string `json:"name"`
	Kubeconfig string `json:"kubeconfig,omitempty"`
	Context    string `json:"context,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	App        string `json:"app,omitempty"`
	Lane       string `json:"lane,omitempty"`
	Container  string `json:"container,omitempty"`
	Kubectl    string `json:"kubectl,omitempty"`
	Tail       *int   `json:"tail,omitempty"` // pointer distinguishes omitted from explicit 0
}

// Read decodes one JSON object from path without validating it. The returned
// mode lets the caller enforce permission policy (e.g. credential files).
func Read(path string) (*Config, os.FileMode, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("cannot open config: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, 0, errors.New("config must be a readable regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, 64*1024+1))
	if err != nil || len(data) > 64*1024 {
		return nil, 0, errors.New("cannot read config or config exceeds 64 KiB")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var cfg *Config
	if err := decoder.Decode(&cfg); err != nil || cfg == nil {
		return nil, 0, errors.New("invalid config JSON or unknown field")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, 0, errors.New("config must contain one JSON object")
	}
	return cfg, info.Mode(), nil
}

// Load validates path's configuration and resolves relative source paths
// against the config file's directory. It does not apply CLI flag precedence;
// the caller merges the returned values with its own defaults and flags.
func Load(path string) (*Config, error) {
	cfg, mode, err := Read(path)
	if err != nil {
		return nil, err
	}
	if cfg.Source != nil {
		switch cfg.Source.Mode {
		case "file":
			if cfg.Source.K8s != nil {
				return nil, errors.New("k8s configuration requires source mode \"k8s\"")
			}
			if cfg.Source.Path == "" {
				return nil, errors.New("file source requires path")
			}
			if !filepath.IsAbs(cfg.Source.Path) {
				cfg.Source.Path = filepath.Join(filepath.Dir(path), cfg.Source.Path)
			}
		case "stdin":
			if cfg.Source.K8s != nil {
				return nil, errors.New("k8s configuration requires source mode \"k8s\"")
			}
			if cfg.Source.Path != "" {
				return nil, errors.New("stdin source must not have path")
			}
		case "k8s":
			if cfg.Source.K8s == nil {
				return nil, errors.New("k8s source requires source.k8s configuration")
			}
			if cfg.Source.Path != "" {
				return nil, errors.New("k8s source must not have path")
			}
			for i := range cfg.Source.K8s.Environments {
				env := &cfg.Source.K8s.Environments[i]
				if env.Kubeconfig != "" && !filepath.IsAbs(env.Kubeconfig) {
					env.Kubeconfig = filepath.Join(filepath.Dir(path), env.Kubeconfig)
				}
			}
		default:
			return nil, errors.New("source mode must be file, stdin or k8s")
		}
	}
	if cfg.Feishu != nil {
		if err := requirePrivate(mode, cfg.Feishu); err != nil {
			return nil, err
		}
		if err := cfg.Feishu.Validate(); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

// LoadFeishu reads only the feishu section of a config file, for the
// "feishu test" subcommand. The scheduled task is driven by Load instead.
func LoadFeishu(path string) (*FeishuConfig, error) {
	cfg, mode, err := Read(path)
	if err != nil {
		return nil, err
	}
	if cfg.Feishu == nil {
		return nil, errors.New("config has no feishu section")
	}
	if err := requirePrivate(mode, cfg.Feishu); err != nil {
		return nil, err
	}
	if err := cfg.Feishu.Validate(); err != nil {
		return nil, err
	}
	if !cfg.Feishu.Enabled {
		return nil, errors.New("feishu is disabled in config (set feishu.enabled=true)")
	}
	return cfg.Feishu, nil
}

func requirePrivate(mode os.FileMode, feishu *FeishuConfig) error {
	if feishu.AppSecret != "" && mode.Perm()&0077 != 0 {
		return errors.New("config containing feishu app_secret must be private; run chmod 600 on the config file")
	}
	return nil
}

// Init writes a starter sql-monitor.json (never overwriting an existing file)
// and reports whether the current directory looks like a GORM Go module.
func Init(args []string, output io.Writer) error {
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

// FindEnvironment looks up a named K8s environment. The error lists available
// names so a mis-typed --k8s-env flag is self-explanatory.
func FindEnvironment(cfg *K8sConfig, name string) (K8sEnvironment, error) {
	if cfg == nil {
		return K8sEnvironment{}, fmt.Errorf("environment %q not found (no environments configured)", name)
	}
	for _, env := range cfg.Environments {
		if env.Name == name {
			return env, nil
		}
	}
	names := make([]string, 0, len(cfg.Environments))
	for _, env := range cfg.Environments {
		names = append(names, env.Name)
	}
	if len(names) == 0 {
		return K8sEnvironment{}, fmt.Errorf("environment %q not found (no environments configured)", name)
	}
	return K8sEnvironment{}, fmt.Errorf("environment %q not found; available: %s", name, strings.Join(names, ", "))
}

// ValidateLabel rejects label values that would break a kubectl label selector.
func ValidateLabel(name, value string) error {
	if strings.ContainsAny(value, ",=") {
		return fmt.Errorf("k8s %s label must not contain ',' or '='", name)
	}
	return nil
}
