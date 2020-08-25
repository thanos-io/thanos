// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/tombstone"
)

func registerDelete(m map[string]setupFunc, app *kingpin.CmdClause, pre string) {
	cmd := app.Command("delete", "Delete series command for the object storage. NOTE: Currently it only performs Store API masking in the object storage at chunk level with respect to the tombstones created by the user (Doesn't actually delete the data in objstore).")

	matchers := cmd.Flag("matchers", "The string representing label matchers").Default("").String()

	objStoreConfig := regCommonObjStoreFlags(cmd, "", true)

	minTime := model.TimeOrDuration(cmd.Flag("min-time", "Start of time range limit to delete. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d (Calculates the actual timestamp at the tombstone creation time) or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z"))

	maxTime := model.TimeOrDuration(cmd.Flag("max-time", "End of time range limit to delete. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d (Calculates the actual timestamp at the tombstone creation time) or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("9999-12-31T23:59:59Z"))

	author := cmd.Flag("author", "Author of the deletion request").Default("not specified").String()

	reason := cmd.Flag("reason", "Reason to perform the deletion request").Default("not specified").String()

	m[pre+" delete"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, reg, pre+" delete")
		if err != nil {
			return err
		}
		defer runutil.CloseWithLogOnErr(logger, bkt, "tools delete")

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		_, err = parser.ParseMetricSelector(*matchers)
		if err != nil {
			return err
		}

		ts := tombstone.NewTombstone(*matchers, minTime.PrometheusTimestamp(), maxTime.PrometheusTimestamp(), *author, *reason)

		return tombstone.UploadTombstone(ts, bkt, logger)
	}
}
