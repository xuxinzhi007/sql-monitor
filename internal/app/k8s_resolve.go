package app

import (
	"errors"
	"fmt"
	"os/exec"

	"local/sql-monitor/internal/config"
	k8s "local/sql-monitor/internal/source/k8s"
)

func applyEnvironment(target *k8s.Target, env config.K8sEnvironment, name string) {
	target.Environment = name
	target.Kubeconfig = env.Kubeconfig
	target.Context = env.Context
	target.Namespace = env.Namespace
	target.App = env.App
	target.Lane = env.Lane
	target.Container = env.Container
	target.Kubectl = env.Kubectl
	if env.Tail != nil {
		target.Tail = *env.Tail
	}
}

// resolveK8s merges the configured k8s environment (if any) with explicit CLI
// flags. Priority: explicit flag > selected environment > default.
func resolveK8s(cfg *config.K8sConfig, kf k8sFlags, explicit map[string]bool) (*k8s.Target, error) {
	var target k8s.Target
	target.Tail = k8s.DefaultTail
	if cfg != nil {
		target.LaneLabel = cfg.LaneLabel
	}
	if explicit["k8s-env"] {
		if cfg == nil {
			return nil, errors.New("--k8s-env requires a config with source mode \"k8s\"")
		}
		env, err := config.FindEnvironment(cfg, kf.env)
		if err != nil {
			return nil, err
		}
		applyEnvironment(&target, env, env.Name)
	} else if cfg != nil && cfg.Default != "" {
		env, err := config.FindEnvironment(cfg, cfg.Default)
		if err != nil {
			return nil, fmt.Errorf("invalid k8s.default: %w", err)
		}
		applyEnvironment(&target, env, env.Name)
	}
	if explicit["k8s-kubeconfig"] {
		target.Kubeconfig = kf.kubeconfig
	}
	if explicit["k8s-context"] {
		target.Context = kf.context
	}
	if explicit["k8s-namespace"] {
		target.Namespace = kf.namespace
	}
	if explicit["k8s-app"] {
		target.App = kf.app
	}
	if explicit["k8s-lane"] {
		target.Lane = kf.lane
	}
	if explicit["k8s-container"] {
		target.Container = kf.container
	}
	if explicit["k8s-tail"] {
		target.Tail = kf.tail
	}
	if explicit["k8s-kubectl"] {
		target.Kubectl = kf.kubectl
	}
	if target.Kubectl == "" {
		target.Kubectl = "kubectl"
	}
	resolved, err := exec.LookPath(target.Kubectl)
	if err != nil {
		return nil, fmt.Errorf("kubectl not found: %s", target.Kubectl)
	}
	target.Kubectl = resolved
	if target.Tail < 0 {
		return nil, errors.New("k8s tail must be nonnegative")
	}
	if target.App == "" {
		return nil, errors.New("k8s source requires an app label (set --k8s-app or k8s.environments[].app)")
	}
	if err := config.ValidateLabel("app", target.App); err != nil {
		return nil, err
	}
	if target.Lane != "" {
		if err := config.ValidateLabel("lane", target.Lane); err != nil {
			return nil, err
		}
	}
	if target.LaneLabel != "" {
		if err := config.ValidateLabel("lane_label", target.LaneLabel); err != nil {
			return nil, err
		}
	}
	return &target, nil
}
