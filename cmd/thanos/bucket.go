// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/olekukonko/tablewriter"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/replicate"
	"github.com/thanos-io/thanos/pkg/runutil"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/ui"
	"github.com/thanos-io/thanos/pkg/verifier"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const extpromPrefix = "thanos_bucket_"

var (
	issuesMap = map[string]verifier.Issue{
		verifier.IndexIssueID:                verifier.IndexIssue,
		verifier.OverlappedBlocksIssueID:     verifier.OverlappedBlocksIssue,
		verifier.DuplicatedCompactionIssueID: verifier.DuplicatedCompactionIssue,
	}
	allIssues = func() (s []string) {
		for id := range issuesMap {
			s = append(s, id)
		}

		sort.Strings(s)
		return s
	}
	inspectColumns = []string{"ULID", "FROM", "UNTIL", "RANGE", "UNTIL-DOWN", "#SERIES", "#SAMPLES", "#CHUNKS", "COMP-LEVEL", "COMP-FAILED", "LABELS", "RESOLUTION", "SOURCE"}
)

func registerBucket(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "Bucket utility commands")

	objStoreConfig := regCommonObjStoreFlags(cmd, "", true)
	registerBucketVerify(m, cmd, name, objStoreConfig)
	registerBucketLs(m, cmd, name, objStoreConfig)
	registerBucketInspect(m, cmd, name, objStoreConfig)
	registerBucketWeb(m, cmd, name, objStoreConfig)
	registerBucketReplicate(m, cmd, name, objStoreConfig)
	registerBucketDownsample(m, cmd, name, objStoreConfig)
}

func registerBucketVerify(m map[string]setupFunc, root *kingpin.CmdClause, name string, objStoreConfig *extflag.PathOrContent) {
	cmd := root.Command("verify", "Verify all blocks in the bucket against specified issues")
	objStoreBackupConfig := regCommonObjStoreFlags(cmd, "-backup", false, "Used for repair logic to backup blocks before removal.")
	repair := cmd.Flag("repair", "Attempt to repair blocks for which issues were detected").
		Short('r').Default("false").Bool()
	issuesToVerify := cmd.Flag("issues", fmt.Sprintf("Issues to verify (and optionally repair). Possible values: %v", allIssues())).
		Short('i').Default(verifier.IndexIssueID, verifier.OverlappedBlocksIssueID).Strings()
	idWhitelist := cmd.Flag("id-whitelist", "Block IDs to verify (and optionally repair) only. "+
		"If none is specified, all blocks will be verified. Repeated field").Strings()
	deleteDelay := modelDuration(cmd.Flag("delete-delay", "Duration after which blocks marked for deletion would be deleted permanently from source bucket by compactor component. "+
		"If delete-delay is non zero, blocks will be marked for deletion and compactor component is required to delete blocks from source bucket. "+
		"If delete-delay is 0, blocks will be deleted straight away. Use this if you want to get rid of or move the block immediately. "+
		"Note that deleting blocks immediately can cause query failures, if store gateway still has the block loaded, "+
		"or compactor is ignoring the deletion because it's compacting the block at the same time.").
		Default("0s"))
	m[name+" verify"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, reg, name)
		if err != nil {
			return err
		}
		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		backupconfContentYaml, err := objStoreBackupConfig.Content()
		if err != nil {
			return err
		}

		var backupBkt objstore.Bucket
		if len(backupconfContentYaml) == 0 {
			if *repair {
				return errors.New("repair is specified, so backup client is required")
			}
		} else {
			// nil Prometheus registerer: don't create conflicting metrics.
			backupBkt, err = client.NewBucket(logger, backupconfContentYaml, nil, name)
			if err != nil {
				return err
			}

			defer runutil.CloseWithLogOnErr(logger, backupBkt, "backup bucket client")
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		var (
			ctx    = context.Background()
			v      *verifier.Verifier
			issues []verifier.Issue
		)

		for _, i := range *issuesToVerify {
			issueFn, ok := issuesMap[i]
			if !ok {
				return errors.Errorf("no such issue name %s", i)
			}
			issues = append(issues, issueFn)
		}

		fetcher, err := block.NewMetaFetcher(logger, fetcherConcurrency, bkt, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg), nil, nil)
		if err != nil {
			return err
		}

		if *repair {
			v = verifier.NewWithRepair(logger, reg, bkt, backupBkt, fetcher, time.Duration(*deleteDelay), issues)
		} else {
			v = verifier.New(logger, reg, bkt, fetcher, time.Duration(*deleteDelay), issues)
		}

		var idMatcher func(ulid.ULID) bool = nil
		if len(*idWhitelist) > 0 {
			whilelistIDs := map[string]struct{}{}
			for _, bid := range *idWhitelist {
				id, err := ulid.Parse(bid)
				if err != nil {
					return errors.Wrap(err, "invalid ULID found in --id-whitelist flag")
				}
				whilelistIDs[id.String()] = struct{}{}
			}

			idMatcher = func(id ulid.ULID) bool {
				if _, ok := whilelistIDs[id.String()]; !ok {
					return false
				}
				return true
			}
		}

		return v.Verify(ctx, idMatcher)
	}
}

func registerBucketLs(m map[string]setupFunc, root *kingpin.CmdClause, name string, objStoreConfig *extflag.PathOrContent) {
	cmd := root.Command("ls", "List all blocks in the bucket")
	output := cmd.Flag("output", "Optional format in which to print each block's information. Options are 'json', 'wide' or a custom template.").
		Short('o').Default("").String()
	m[name+" ls"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, reg, name)
		if err != nil {
			return err
		}

		fetcher, err := block.NewMetaFetcher(logger, fetcherConcurrency, bkt, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg), nil, nil)
		if err != nil {
			return err
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		var (
			format     = *output
			objects    = 0
			printBlock func(m *metadata.Meta) error
		)

		switch format {
		case "":
			printBlock = func(m *metadata.Meta) error {
				fmt.Fprintln(os.Stdout, m.ULID.String())
				return nil
			}
		case "wide":
			printBlock = func(m *metadata.Meta) error {
				minTime := time.Unix(m.MinTime/1000, 0)
				maxTime := time.Unix(m.MaxTime/1000, 0)

				if _, err = fmt.Fprintf(os.Stdout, "%s -- %s - %s Diff: %s, Compaction: %d, Downsample: %d, Source: %s\n",
					m.ULID, minTime.Format("2006-01-02 15:04"), maxTime.Format("2006-01-02 15:04"), maxTime.Sub(minTime),
					m.Compaction.Level, m.Thanos.Downsample.Resolution, m.Thanos.Source); err != nil {
					return err
				}
				return nil
			}
		case "json":
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "\t")

			printBlock = func(m *metadata.Meta) error {
				return enc.Encode(&m)
			}
		default:
			tmpl, err := template.New("").Parse(format)
			if err != nil {
				return errors.Wrap(err, "invalid template")
			}
			printBlock = func(m *metadata.Meta) error {
				if err := tmpl.Execute(os.Stdout, &m); err != nil {
					return errors.Wrap(err, "execute template")
				}
				fmt.Fprintln(os.Stdout, "")
				return nil
			}
		}

		metas, _, err := fetcher.Fetch(ctx)
		if err != nil {
			return err
		}

		for _, meta := range metas {
			objects++
			if err := printBlock(meta); err != nil {
				return errors.Wrap(err, "iter")
			}
		}
		level.Info(logger).Log("msg", "ls done", "objects", objects)
		return nil
	}
}

func registerBucketInspect(m map[string]setupFunc, root *kingpin.CmdClause, name string, objStoreConfig *extflag.PathOrContent) {
	cmd := root.Command("inspect", "Inspect all blocks in the bucket in detailed, table-like way")
	selector := cmd.Flag("selector", "Selects blocks based on label, e.g. '-l key1=\\\"value1\\\" -l key2=\\\"value2\\\"'. All key value pairs must match.").Short('l').
		PlaceHolder("<name>=\\\"<value>\\\"").Strings()
	sortBy := cmd.Flag("sort-by", "Sort by columns. It's also possible to sort by multiple columns, e.g. '--sort-by FROM --sort-by UNTIL'. I.e., if the 'FROM' value is equal the rows are then further sorted by the 'UNTIL' value.").
		Default("FROM", "UNTIL").Enums(inspectColumns...)
	timeout := cmd.Flag("timeout", "Timeout to download metadata from remote storage").Default("5m").Duration()

	m[name+" inspect"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {

		// Parse selector.
		selectorLabels, err := parseFlagLabels(*selector)
		if err != nil {
			return errors.Wrap(err, "error parsing selector flag")
		}

		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, reg, name)
		if err != nil {
			return err
		}

		fetcher, err := block.NewMetaFetcher(logger, fetcherConcurrency, bkt, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg), nil, nil)
		if err != nil {
			return err
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()

		// Getting Metas.
		metas, _, err := fetcher.Fetch(ctx)
		if err != nil {
			return err
		}

		blockMetas := make([]*metadata.Meta, 0, len(metas))
		for _, meta := range metas {
			blockMetas = append(blockMetas, meta)
		}

		return printTable(blockMetas, selectorLabels, *sortBy)
	}
}

// registerBucketWeb exposes a web interface for the state of remote store like `pprof web`.
func registerBucketWeb(m map[string]setupFunc, root *kingpin.CmdClause, name string, objStoreConfig *extflag.PathOrContent) {
	cmd := root.Command("web", "Web interface for remote storage bucket")
	httpBindAddr, httpGracePeriod := regHTTPFlags(cmd)
	webExternalPrefix := cmd.Flag("web.external-prefix", "Static prefix for all HTML links and redirect URLs in the bucket web UI interface. Actual endpoints are still served on / or the web.route-prefix. This allows thanos bucket web UI to be served behind a reverse proxy that strips a URL sub-path.").Default("").String()
	webPrefixHeaderName := cmd.Flag("web.prefix-header", "Name of HTTP request header used for dynamic prefixing of UI links and redirects. This option is ignored if web.external-prefix argument is set. Security risk: enable this option only if a reverse proxy in front of thanos is resetting the header. The --web.prefix-header=X-Forwarded-Prefix option can be useful, for example, if Thanos UI is served via Traefik reverse proxy with PathPrefixStrip option enabled, which sends the stripped prefix value in X-Forwarded-Prefix header. This allows thanos UI to be served on a sub-path.").Default("").String()
	interval := cmd.Flag("refresh", "Refresh interval to download metadata from remote storage").Default("30m").Duration()
	timeout := cmd.Flag("timeout", "Timeout to download metadata from remote storage").Default("5m").Duration()
	label := cmd.Flag("label", "Prometheus label to use as timeline title").String()

	m[name+" web"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		comp := component.Bucket
		httpProbe := prober.NewHTTP()
		statusProber := prober.Combine(
			httpProbe,
			prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
		)

		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(*httpBindAddr),
			httpserver.WithGracePeriod(time.Duration(*httpGracePeriod)),
		)

		router := route.New()

		bucketUI := ui.NewBucketUI(logger, *label, *webExternalPrefix, *webPrefixHeaderName)
		bucketUI.Register(router, extpromhttp.NewInstrumentationMiddleware(reg))
		srv.Handle("/", router)

		if *interval < 5*time.Minute {
			level.Warn(logger).Log("msg", "Refreshing more often than 5m could lead to large data transfers")
		}

		if *timeout < time.Minute {
			level.Warn(logger).Log("msg", "Timeout less than 1m could lead to frequent failures")
		}

		if *interval < (*timeout * 2) {
			level.Warn(logger).Log("msg", "Refresh interval should be at least 2 times the timeout")
		}

		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Bucket.String())
		if err != nil {
			return errors.Wrap(err, "bucket client")
		}

		// TODO(bwplotka): Allow Bucket UI to visualize the state of block as well.
		fetcher, err := block.NewMetaFetcher(logger, fetcherConcurrency, bkt, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg), nil, nil)
		if err != nil {
			return err
		}
		fetcher.UpdateOnChange(bucketUI.Set)

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			statusProber.Ready()
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
			return runutil.Repeat(*interval, ctx.Done(), func() error {
				return runutil.RetryWithLog(logger, time.Minute, ctx.Done(), func() error {
					iterCtx, iterCancel := context.WithTimeout(ctx, *timeout)
					defer iterCancel()

					_, _, err := fetcher.Fetch(iterCtx)
					if err != nil {
						return err
					}
					return nil
				})
			})
		}, func(error) {
			cancel()
		})

		g.Add(func() error {
			statusProber.Healthy()

			return srv.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			defer statusProber.NotHealthy(err)

			srv.Shutdown(err)
		})

		return nil
	}
}

// Provide a list of resolution, can not use Enum directly, since string does not implement int64 function.
func listResLevel() []string {
	return []string{
		strconv.FormatInt(downsample.ResLevel0, 10),
		strconv.FormatInt(downsample.ResLevel1, 10),
		strconv.FormatInt(downsample.ResLevel2, 10)}
}

func registerBucketReplicate(m map[string]setupFunc, root *kingpin.CmdClause, name string, objStoreConfig *extflag.PathOrContent) {
	cmd := root.Command("replicate", fmt.Sprintf("Replicate data from one object storage to another. NOTE: Currently it works only with Thanos blocks (%v has to have Thanos metadata).", block.MetaFilename))
	httpBindAddr, httpGracePeriod := regHTTPFlags(cmd)
	toObjStoreConfig := regCommonObjStoreFlags(cmd, "-to", false, "The object storage which replicate data to.")
	// TODO(bwplotka): Allow to replicate many resolution levels.
	resolution := cmd.Flag("resolution", "Only blocks with this resolution will be replicated.").Default(strconv.FormatInt(downsample.ResLevel0, 10)).HintAction(listResLevel).Int64()
	// TODO(bwplotka): Allow to replicate many compaction levels.
	compaction := cmd.Flag("compaction", "Only blocks with this compaction level will be replicated.").Default("1").Int()
	matcherStrs := cmd.Flag("matcher", "Only blocks whose external labels exactly match this matcher will be replicated.").PlaceHolder("key=\"value\"").Strings()
	singleRun := cmd.Flag("single-run", "Run replication only one time, then exit.").Default("false").Bool()

	m[name+" replicate"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		matchers, err := replicate.ParseFlagMatchers(*matcherStrs)
		if err != nil {
			return errors.Wrap(err, "parse block label matchers")
		}

		return replicate.RunReplicate(
			g,
			logger,
			reg,
			tracer,
			*httpBindAddr,
			time.Duration(*httpGracePeriod),
			matchers,
			compact.ResolutionLevel(*resolution),
			*compaction,
			objStoreConfig,
			toObjStoreConfig,
			*singleRun,
		)
	}

}

func registerBucketDownsample(m map[string]setupFunc, root *kingpin.CmdClause, name string, objStoreConfig *extflag.PathOrContent) {
	comp := component.Downsample
	cmd := root.Command(comp.String(), "continuously downsamples blocks in an object store bucket")

	httpAddr, httpGracePeriod := regHTTPFlags(cmd)

	dataDir := cmd.Flag("data-dir", "Data directory in which to cache blocks and process downsamplings.").
		Default("./data").String()

	m[name+" "+comp.String()] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		return RunDownsample(g, logger, reg, *httpAddr, time.Duration(*httpGracePeriod), *dataDir, objStoreConfig, comp)
	}
}

func printTable(blockMetas []*metadata.Meta, selectorLabels labels.Labels, sortBy []string) error {
	header := inspectColumns

	var lines [][]string
	p := message.NewPrinter(language.English)

	for _, blockMeta := range blockMetas {
		if !matchesSelector(blockMeta, selectorLabels) {
			continue
		}

		timeRange := time.Duration((blockMeta.MaxTime - blockMeta.MinTime) * int64(time.Millisecond))

		untilDown := "-"
		if until, err := compact.UntilNextDownsampling(blockMeta); err == nil {
			untilDown = until.String()
		}
		var labels []string
		for _, key := range getKeysAlphabetically(blockMeta.Thanos.Labels) {
			labels = append(labels, fmt.Sprintf("%s=%s", key, blockMeta.Thanos.Labels[key]))
		}

		var line []string
		line = append(line, blockMeta.ULID.String())
		line = append(line, time.Unix(blockMeta.MinTime/1000, 0).Format("02-01-2006 15:04:05"))
		line = append(line, time.Unix(blockMeta.MaxTime/1000, 0).Format("02-01-2006 15:04:05"))
		line = append(line, timeRange.String())
		line = append(line, untilDown)
		line = append(line, p.Sprintf("%d", blockMeta.Stats.NumSeries))
		line = append(line, p.Sprintf("%d", blockMeta.Stats.NumSamples))
		line = append(line, p.Sprintf("%d", blockMeta.Stats.NumChunks))
		line = append(line, p.Sprintf("%d", blockMeta.Compaction.Level))
		line = append(line, p.Sprintf("%t", blockMeta.Compaction.Failed))
		line = append(line, strings.Join(labels, ","))
		line = append(line, time.Duration(blockMeta.Thanos.Downsample.Resolution*int64(time.Millisecond)).String())
		line = append(line, string(blockMeta.Thanos.Source))
		lines = append(lines, line)
	}

	var sortByColNum []int
	for _, col := range sortBy {
		index := getIndex(header, col)
		if index == -1 {
			return errors.Errorf("column %s not found", col)
		}
		sortByColNum = append(sortByColNum, index)
	}

	t := Table{Header: header, Lines: lines, SortIndices: sortByColNum}
	sort.Sort(t)

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(t.Header)
	table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	table.SetCenterSeparator("|")
	table.SetAutoWrapText(false)
	table.SetReflowDuringAutoWrap(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.AppendBulk(t.Lines)
	table.Render()

	return nil
}

func getKeysAlphabetically(labels map[string]string) []string {
	var keys []string
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// matchesSelector checks if blockMeta contains every label from
// the selector with the correct value.
func matchesSelector(blockMeta *metadata.Meta, selectorLabels labels.Labels) bool {
	for _, l := range selectorLabels {
		if v, ok := blockMeta.Thanos.Labels[l.Name]; !ok || v != l.Value {
			return false
		}
	}
	return true
}

// getIndex calculates the index of s in strs.
func getIndex(strs []string, s string) int {
	for i, col := range strs {
		if col == s {
			return i
		}
	}
	return -1
}

type Table struct {
	Header      []string
	Lines       [][]string
	SortIndices []int
}

func (t Table) Len() int { return len(t.Lines) }

func (t Table) Swap(i, j int) { t.Lines[i], t.Lines[j] = t.Lines[j], t.Lines[i] }

func (t Table) Less(i, j int) bool {
	for _, index := range t.SortIndices {
		if t.Lines[i][index] == t.Lines[j][index] {
			continue
		}
		return compare(t.Lines[i][index], t.Lines[j][index])
	}
	return compare(t.Lines[i][0], t.Lines[j][0])
}

func compare(s1, s2 string) bool {
	s1Time, s1Err := time.Parse("02-01-2006 15:04:05", s1)
	s2Time, s2Err := time.Parse("02-01-2006 15:04:05", s2)
	if s1Err != nil || s2Err != nil {
		return s1 < s2
	}
	return s1Time.Before(s2Time)
}
