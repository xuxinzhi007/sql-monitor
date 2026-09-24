package main

import (
	"errors"
	"math"
	"net/mail"
	"strings"
)

type FeishuRecipient struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type FeishuConfig struct {
	Enabled         bool              `json:"enabled"`
	AppID           string            `json:"app_id"`
	AppSecret       string            `json:"app_secret"`
	Recipients      []FeishuRecipient `json:"recipients"`
	IntervalSeconds int               `json:"interval_seconds"`
	SlowMS          float64           `json:"slow_ms"`
	Project         string            `json:"project"`
}

func (c *FeishuConfig) validate() error {
	if !c.Enabled {
		return nil
	}
	c.AppID = strings.TrimSpace(c.AppID)
	c.AppSecret = strings.TrimSpace(c.AppSecret)
	c.Project = strings.TrimSpace(c.Project)
	if c.AppID == "" || c.AppSecret == "" {
		return errors.New("feishu requires app_id and app_secret")
	}
	if c.IntervalSeconds < 10 || c.IntervalSeconds > 86400 {
		return errors.New("feishu interval_seconds must be 10..86400")
	}
	if math.IsNaN(c.SlowMS) || math.IsInf(c.SlowMS, 0) || c.SlowMS <= 0 {
		return errors.New("feishu slow_ms must be positive and finite")
	}
	if c.Project == "" || len(c.Project) > 120 {
		return errors.New("feishu project must be 1..120 bytes")
	}
	if len(c.Recipients) == 0 || len(c.Recipients) > 20 {
		return errors.New("feishu requires 1..20 recipients")
	}
	seen := make(map[string]bool)
	for i := range c.Recipients {
		r := &c.Recipients[i]
		r.Email = strings.TrimSpace(r.Email)
		address, err := mail.ParseAddress(r.Email)
		if err != nil || address.Address != r.Email || len(r.Email) > 254 || len(r.Name) > 120 {
			return errors.New("feishu recipient requires a plain email address and name of at most 120 bytes")
		}
		key := strings.ToLower(r.Email)
		if seen[key] {
			return errors.New("feishu recipients must have unique emails")
		}
		seen[key] = true
	}
	return nil
}
