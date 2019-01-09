package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/client"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/shipper"
	"github.com/improbable-eng/thanos/pkg/verifier"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/olekukonko/tablewriter"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/labels"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"gopkg.in/alecthomas/kingpin.v2"
)

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
	inspectColumns = []string{"ULID", "FROM", "UNTIL", "RANGE", "UNTIL-COMP", "#SERIES", "#SAMPLES", "#CHUNKS", "COMP-LEVEL", "COMP-FAILED", "LABELS", "RESOLUTION", "SOURCE"}
)

func registerBucket(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "Bucket utility commands")

	objStoreConfig := regCommonObjStoreFlags(cmd, "", true)
	registerBucketSync(m, cmd, name, objStoreConfig)
	registerBucketVerify(m, cmd, name, objStoreConfig)
	registerBucketLs(m, cmd, name, objStoreConfig)
	registerBucketInspect(m, cmd, name, objStoreConfig)
	return
}

func registerBucketSync(m map[string]setupFunc, root *kingpin.CmdClause, name string, objStoreConfig *pathOrContent) {
	cmd := root.Command("sync",
		`Synchronise all blocks with remote bucket. NOTE: Currently NO compactor must be running in the same time.


`)
	promURL := cmd.Flag("prometheus.url", "URL at which to reach Prometheus's API to get external labels from and snapshot blocks from if Prometheus has `--web.enable-admin-api` flag. For better performance use local network.").
		Default("http://localhost:9090").URL()
	labelStrs := cmd.Flag("label", "Optional external labels that will be used for synced blocks. If no label is specified, prometheus.url flag will be used to fetch labels from running Prometheus. (repeated). Similar to external labels for Prometheus, used to identify ruler and its blocks as unique source.").
		PlaceHolder("<name>=\"<value>\"").Strings()
	dataDir := cmd.Flag("data-dir", "Data to sync local blocks from. Usually directory of Prometheus TSDB. If empty, snapshotted blocks from Prometheus will be used. (recommended if Prometheus is up)").
		String()
	workDir := cmd.Flag("work-dir", "Work dir for temporary files (hard links) created by sync command. Useful when data-dir has no more space.").
		Default("./").String()

	m[name+" sync"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ bool) error {
		ctx := context.Background()
		lset, err := parseFlagLabels(*labelStrs)
		if err != nil {
			return errors.Wrap(err, "parse labels")
		}

		bucketConfig, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, bucketConfig, reg, name)
		if err != nil {
			return err
		}

		if len(lset) == 0 {
			level.Info(logger).Log("msg", "--labels are not specified; querying Prometheus config API to get external labels.", "addr", *promURL)
			lset, err = queryExternalLabels(ctx, logger, *promURL)
			if err != nil {
				return errors.Wrapf(err, "get external labels from Prometheus on %s", *promURL)
			}
		}

		if len(lset) == 0 {
			level.Error(logger).Log("msg", "syncing blocks without external labels is not allowed. Set unique external labels in your Prometheus config or pass explicit external labels via --labels flag.")
		}

		level.Info(logger).Log("msg", "starting syncing blocks", "prometheus.url", *promURL, "dataDir", *dataDir, "workDir", *workDir, "extLset", lset)
		if *dataDir != "" {
			level.Warn(logger).Log("msg", "is Prometheus operating on specified data dir? If yes, turn it's local compaction or leave dataDir empty to safely snapshot data to avoid races.")
		}
		level.Warn(logger).Log("msg", "have you turned off compactor? Currently it is unsafe to run compactor in the same time as this command! (y/N)")
		reader := bufio.NewReader(os.Stdin)
		text, _ := reader.ReadString('\n')
		if text != "y\n" {
			return errors.New("aborting")
		}

		lsetFn := func() labels.Labels { return lset }
		uploaded, err := shipper.SyncAll(ctx, logger, *dataDir, bkt, lsetFn, block.BucketSyncSource)
		if err != nil {
			if uploaded > 0 {
				level.Warn(logger).Log("msg", "synced only some blocks", "uploaded", uploaded)
			}
			return err
		}
		level.Info(logger).Log("msg", "synced all blocks", "uploaded", uploaded)
		return nil
	}
}

func registerBucketVerify(m map[string]setupFunc, root *kingpin.CmdClause, name string, objStoreConfig *pathOrContent) {
	cmd := root.Command("verify", "Verify all blocks in the bucket against specified issues")
	objStoreBackupConfig := regCommonObjStoreFlags(cmd, "-backup", false)
	repair := cmd.Flag("repair", "Attempt to repair blocks for which issues were detected").
		Short('r').Default("false").Bool()
	issuesToVerify := cmd.Flag("issues", fmt.Sprintf("Issues to verify (and optionally repair). Possible values: %v", allIssues())).
		Short('i').Default(verifier.IndexIssueID, verifier.OverlappedBlocksIssueID).Strings()
	idWhitelist := cmd.Flag("id-whitelist", "Block IDs to verify (and optionally repair) only. "+
		"If none is specified, all blocks will be verified. Repeated field").Strings()
	m[name+" verify"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ bool) error {
		bucketConfig, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, bucketConfig, reg, name)
		if err != nil {
			return err
		}
		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		backupBucketConfig, err := objStoreBackupConfig.Content()
		if err != nil {
			return err
		}

		var backupBkt objstore.Bucket
		if len(backupBucketConfig) == 0 {
			if *repair {
				return errors.Wrap(err, "repair is specified, so backup client is required")
			}
		} else {
			backupBkt, err = client.NewBucket(logger, backupBucketConfig, reg, name)
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

		if *repair {
			v = verifier.NewWithRepair(logger, bkt, backupBkt, issues)
		} else {
			v = verifier.New(logger, bkt, issues)
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

func registerBucketLs(m map[string]setupFunc, root *kingpin.CmdClause, name string, objStoreConfig *pathOrContent) {
	cmd := root.Command("ls", "List all blocks in the bucket")
	output := cmd.Flag("output", "Format in which to print each block's information. May be 'json' or custom template.").
		Short('o').Default("").String()
	m[name+" ls"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ bool) error {
		bucketConfig, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, bucketConfig, reg, name)
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
			printBlock func(id ulid.ULID) error
		)

		switch format {
		case "":
			printBlock = func(id ulid.ULID) error {
				fmt.Fprintln(os.Stdout, id.String())
				return nil
			}
		case "wide":
			printBlock = func(id ulid.ULID) error {
				m, err := block.DownloadMeta(ctx, logger, bkt, id)
				if err != nil {
					return err
				}

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

			printBlock = func(id ulid.ULID) error {
				m, err := block.DownloadMeta(ctx, logger, bkt, id)
				if err != nil {
					return err
				}
				return enc.Encode(&m)
			}
		default:
			tmpl, err := template.New("").Parse(format)
			if err != nil {
				return errors.Wrap(err, "invalid template")
			}
			printBlock = func(id ulid.ULID) error {
				m, err := block.DownloadMeta(ctx, logger, bkt, id)
				if err != nil {
					return err
				}

				if err := tmpl.Execute(os.Stdout, &m); err != nil {
					return errors.Wrap(err, "execute template")
				}
				fmt.Fprintln(os.Stdout, "")
				return nil
			}
		}

		return bkt.Iter(ctx, "", func(name string) error {
			id, ok := block.IsBlockDir(name)
			if !ok {
				return nil
			}
			return printBlock(id)
		})
	}
}

func registerBucketInspect(m map[string]setupFunc, root *kingpin.CmdClause, name string, objStoreConfig *pathOrContent) {
	cmd := root.Command("inspect", "Inspect all blocks in the bucket")
	selector := cmd.Flag("selector", "Selects blocks based on label, e.g. '-l key1=\"value1\" -l key2=\"value2\"'. All key value pairs must match.").Short('l').
		PlaceHolder("<name>=\"<value>\"").Strings()
	sortBy := cmd.Flag("sort-by", "Sort by columns. It's also possible to sort by multiple columns, e.g. '--sort-by FROM --sort-by UNTIL'. I.e., if the 'FROM' value is equal the rows are then further sorted by the 'UNTIL' value.").
		Default("FROM", "UNTIL").Enums(inspectColumns...)

	m[name+" inspect"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ bool) error {

		// Parse selector.
		selectorLabels, err := parseFlagLabels(*selector)
		if err != nil {
			return fmt.Errorf("error parsing selector flag: %v", err)
		}

		bucketConfig, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, bucketConfig, reg, name)
		if err != nil {
			return err
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		// Getting Metas.
		var blockMetas []*block.Meta
		if err = bkt.Iter(ctx, "", func(name string) error {
			id, ok := block.IsBlockDir(name)
			if !ok {
				return nil
			}

			m, err := block.DownloadMeta(ctx, logger, bkt, id)
			if err != nil {
				return err
			}

			blockMetas = append(blockMetas, &m)

			return nil
		}); err != nil {
			return err
		}

		return printTable(blockMetas, selectorLabels, *sortBy)
	}
}

func printTable(blockMetas []*block.Meta, selectorLabels labels.Labels, sortBy []string) error {
	header := inspectColumns

	var lines [][]string
	p := message.NewPrinter(language.English)

	for _, blockMeta := range blockMetas {
		if !matchesSelector(blockMeta, selectorLabels) {
			continue
		}

		timeRange := time.Duration((blockMeta.MaxTime - blockMeta.MinTime) * int64(time.Millisecond))
		// Calculate how long it takes until the next compaction.
		untilComp := "-"
		if blockMeta.Thanos.Downsample.Resolution == 0 { // data currently raw, downsample if range >= 40 hours
			untilComp = (time.Duration(40*60*60*1000*time.Millisecond) - timeRange).String()
		}
		if blockMeta.Thanos.Downsample.Resolution == 5*60*1000 { // data currently 5m resolution, downsample if range >= 10 days
			untilComp = (time.Duration(10*24*60*60*1000*time.Millisecond) - timeRange).String()
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
		line = append(line, untilComp)
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
			return fmt.Errorf("column %s not found", col)
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
// the selector with the correct value
func matchesSelector(blockMeta *block.Meta, selectorLabels labels.Labels) bool {
	for _, l := range selectorLabels {
		if v, ok := blockMeta.Thanos.Labels[l.Name]; !ok || v != l.Value {
			return false
		}
	}
	return true
}

// getIndex calculates the index of s in strs
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
