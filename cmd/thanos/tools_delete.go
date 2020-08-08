// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/cespare/xxhash"
	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

type Tombstone struct {
	Matcher string `json:"matcher"`
	MinTime int64  `json:"minTime"`
	MaxTime int64  `json:"maxTime"`
}

func registerDelete(m map[string]setupFunc, app *kingpin.CmdClause, pre string) {
	cmd := app.Command("delete", "Delete series command")

	matcher := cmd.Flag("matcher", "The string representing label matchers").Default("").String()

	objStoreConfig := regCommonObjStoreFlags(cmd, "", true)

	minTime := model.TimeOrDuration(cmd.Flag("min-time", "Start of time range limit to delete. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z"))

	maxTime := model.TimeOrDuration(cmd.Flag("max-time", "End of time range limit to delete. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("9999-12-31T23:59:59Z"))

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

		_, err = parser.ParseMetricSelector(*matcher)
		if err != nil {
			return err
		}

		ts := Tombstone{Matcher: *matcher, MinTime: minTime.PrometheusTimestamp(), MaxTime: maxTime.PrometheusTimestamp()}
		b, err := json.Marshal(ts)
		if err != nil {
			return err
		}

		tmpDir := os.TempDir()

		tsPath := tmpDir + "/tombstone.json"
		err = ioutil.WriteFile(tsPath, b, 0644)
		if err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		hash := xxhash.Sum64(b)

		err = objstore.UploadFile(ctx, logger, bkt, tsPath, "tombstones/"+strconv.FormatUint(hash, 10)+".json")
		if err != nil {
			return err
		}

		return nil
	}
}
