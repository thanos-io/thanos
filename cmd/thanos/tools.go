// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"os"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/rules"
)

type checkRulesConfig struct {
	rulesFiles []string
}

func registerTools(app *extkingpin.App) {
	cmd := app.Command("tools", "Tools utility commands")

	registerBucket(cmd)
	registerCheckRules(cmd)
	registerStreamMetric(cmd)
}

func (tc *checkRulesConfig) registerFlag(cmd extkingpin.FlagClause) *checkRulesConfig {
	cmd.Flag("rules", "The rule files glob to check (repeated).").Required().StringsVar(&tc.rulesFiles)
	return tc
}

func registerCheckRules(app extkingpin.AppClause) {
	cmd := app.Command("rules-check", "Check if the rule files are valid or not.")
	crc := &checkRulesConfig{}
	crc.registerFlag(cmd)
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})
		return checkRulesFiles(logger, &crc.rulesFiles)
	})
}

func checkRulesFiles(logger log.Logger, patterns *[]string) error {
	var failed errutil.MultiError

	for _, p := range *patterns {
		level.Info(logger).Log("msg", "checking", "pattern", p)
		matches, err := filepath.Glob(p)
		if err != nil || matches == nil {
			err = errors.New("matching file not found")
			level.Error(logger).Log("result", "FAILED", "error", err)
			failed.Add(err)
			continue
		}
		for _, fn := range matches {
			level.Info(logger).Log("msg", "checking", "filename", filepath.Clean(fn))
			f, er := os.Open(fn)
			if er != nil {
				level.Error(logger).Log("result", "FAILED", "error", er)
				failed.Add(er)
				continue
			}
			defer func() { _ = f.Close() }()

			n, errs := rules.ValidateAndCount(f)
			if errs.Err() != nil {
				level.Error(logger).Log("result", "FAILED")
				for _, e := range errs {
					level.Error(logger).Log("error", e.Error())
					failed.Add(e)
				}
				continue
			}
			level.Info(logger).Log("result", "SUCCESS", "rules found", n)
		}
	}
	return failed.Err()
}
