package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"local/sql-monitor/internal/config"
	k8s "local/sql-monitor/internal/source/k8s"
)

func TestResolveK8s(t *testing.T) {
	kubectl := filepath.Join(t.TempDir(), "kubectl")
	if err := os.WriteFile(kubectl, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	cfg := &config.K8sConfig{
		Default: "dev2",
		Environments: []config.K8sEnvironment{
			{Name: "dev2", Namespace: "ns", App: "data-service", Lane: "dev2", Kubeconfig: "/c/dev2"},
			{Name: "test", Namespace: "ns", App: "data-service", Lane: "test"},
		},
	}
	got, err := resolveK8s(cfg, k8sFlags{kubectl: kubectl}, map[string]bool{"k8s-kubectl": true})
	if err != nil || got.Lane != "dev2" || got.Environment != "dev2" || got.Tail != k8s.DefaultTail {
		t.Fatalf("default: %+v %v", got, err)
	}
	got, err = resolveK8s(cfg, k8sFlags{env: "test", kubectl: kubectl}, map[string]bool{"k8s-env": true, "k8s-kubectl": true})
	if err != nil || got.Lane != "test" || got.Environment != "test" {
		t.Fatalf("env: %+v %v", got, err)
	}
	got, err = resolveK8s(cfg, k8sFlags{lane: "dev3", kubectl: kubectl}, map[string]bool{"k8s-lane": true, "k8s-kubectl": true})
	if err != nil || got.Lane != "dev3" || got.Environment != "dev2" {
		t.Fatalf("lane override: %+v %v", got, err)
	}
	if _, err := resolveK8s(cfg, k8sFlags{env: "nope", kubectl: kubectl}, map[string]bool{"k8s-env": true, "k8s-kubectl": true}); err == nil || !strings.Contains(err.Error(), "available") {
		t.Fatal("missing environment not rejected with available names")
	}
	if _, err := resolveK8s(cfg, k8sFlags{kubectl: "/nonexistent/kubectl"}, map[string]bool{"k8s-kubectl": true}); err == nil || !strings.Contains(err.Error(), "kubectl not found") {
		t.Fatal("missing kubectl not rejected")
	}
}

func TestResolveK8sLaneLabelOverride(t *testing.T) {
	kubectl := filepath.Join(t.TempDir(), "kubectl")
	if err := os.WriteFile(kubectl, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	cfg := &config.K8sConfig{
		LaneLabel:    "team.io/rail",
		Default:      "dev",
		Environments: []config.K8sEnvironment{{Name: "dev", App: "data-service", Lane: "dev2"}},
	}
	target, err := resolveK8s(cfg, k8sFlags{kubectl: kubectl}, map[string]bool{"k8s-kubectl": true})
	if err != nil || target.LaneLabel != "team.io/rail" || target.Selector() != "app=data-service,team.io/rail=dev2" {
		t.Fatalf("lane label not applied: %+v %v", target, err)
	}
}
