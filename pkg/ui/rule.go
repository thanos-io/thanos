// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package ui

import (
	"fmt"
	"html/template"
	"math"
	"regexp"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/rules"

	"github.com/thanos-io/thanos/pkg/component"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	thanosrules "github.com/thanos-io/thanos/pkg/rules"
)

type Rule struct {
	*BaseUI

	externalPrefix, prefixHeader string

	ruleManager *thanosrules.Manager
	queryURL    string
	reg         prometheus.Registerer
}

func NewRuleUI(logger log.Logger, reg prometheus.Registerer, ruleManager *thanosrules.Manager, queryURL, externalPrefix, prefixHeader string) *Rule {
	tmplVariables := map[string]string{
		"Component": component.Rule.String(),
		"queryURL":  queryURL,
	}

	tmplFuncs := ruleTmplFuncs(queryURL)

	return &Rule{
		BaseUI:         NewBaseUI(logger, tmplFuncs, tmplVariables, externalPrefix, prefixHeader, component.Rule),
		externalPrefix: externalPrefix,
		prefixHeader:   prefixHeader,
		ruleManager:    ruleManager,
		queryURL:       queryURL,
		reg:            reg,
	}
}

func ruleTmplFuncs(queryURL string) template.FuncMap {
	return template.FuncMap{
		"since": func(t time.Time) time.Duration {
			return time.Since(t) / time.Millisecond * time.Millisecond
		},
		"alertStateToClass": func(as rules.AlertState) string {
			switch as {
			case rules.StateInactive:
				return "success"
			case rules.StatePending:
				return "warning"
			case rules.StateFiring:
				return "danger"
			default:
				panic("unknown alert state")
			}
		},
		"ruleHealthToClass": func(rh rules.RuleHealth) string {
			switch rh {
			case rules.HealthUnknown:
				return "warning"
			case rules.HealthGood:
				return "success"
			default:
				return "danger"
			}
		},
		"queryURL": func() string { return queryURL },
		"reReplaceAll": func(pattern, repl, text string) string {
			re := regexp.MustCompile(pattern)
			return re.ReplaceAllString(text, repl)
		},
		"humanizeDuration": func(v float64) string {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return fmt.Sprintf("%.4g", v)
			}
			if v == 0 {
				return fmt.Sprintf("%.4gs", v)
			}
			if math.Abs(v) >= 1 {
				sign := ""
				if v < 0 {
					sign = "-"
					v = -v
				}
				seconds := int64(v) % 60
				minutes := (int64(v) / 60) % 60
				hours := (int64(v) / 60 / 60) % 24
				days := (int64(v) / 60 / 60 / 24)
				// For days to minutes, we display seconds as an integer.
				if days != 0 {
					return fmt.Sprintf("%s%dd %dh %dm %ds", sign, days, hours, minutes, seconds)
				}
				if hours != 0 {
					return fmt.Sprintf("%s%dh %dm %ds", sign, hours, minutes, seconds)
				}
				if minutes != 0 {
					return fmt.Sprintf("%s%dm %ds", sign, minutes, seconds)
				}
				// For seconds, we display 4 significant digits.
				return fmt.Sprintf("%s%.4gs", sign, v)
			}
			prefix := ""
			for _, p := range []string{"m", "u", "n", "p", "f", "a", "z", "y"} {
				if math.Abs(v) >= 1 {
					break
				}
				prefix = p
				v *= 1000
			}
			return fmt.Sprintf("%.4g%ss", v, prefix)
		},
	}
}

func (ru *Rule) Register(r *route.Router, ins extpromhttp.InstrumentationMiddleware) {
	registerReactApp(r, ins, ru.BaseUI)
}
