package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore/client"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/verifier"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sbueringer/thanos-inspect/pkg/table"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
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
)

func registerBucket(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "inspect metric data in an object storage bucket")

	objStoreConfig := regCommonObjStoreFlags(cmd, "")
	objStoreBackupConfig := regCommonObjStoreFlags(cmd, "-backup")

	// Verify command.
	verify := cmd.Command("verify", "verify all blocks in the bucket against specified issues")
	verifyRepair := verify.Flag("repair", "attempt to repair blocks for which issues were detected").
		Short('r').Default("false").Bool()
	verifyIssues := verify.Flag("issues", fmt.Sprintf("Issues to verify (and optionally repair). Possible values: %v", allIssues())).
		Short('i').Default(verifier.IndexIssueID, verifier.OverlappedBlocksIssueID).Strings()
	verifyIDWhitelist := verify.Flag("id-whitelist", "Block IDs to verify (and optionally repair) only. "+
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

		backupBkt, err := client.NewBucket(logger, backupBucketConfig, reg, name)
		if err == client.ErrNotFound {
			if *verifyRepair {
				return errors.Wrap(err, "repair is specified, so backup client is required")
			}
		} else if err != nil {
			return err
		} else {
			defer runutil.CloseWithLogOnErr(logger, backupBkt, "backup bucket client")
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		var (
			ctx    = context.Background()
			v      *verifier.Verifier
			issues []verifier.Issue
		)

		for _, i := range *verifyIssues {
			issueFn, ok := issuesMap[i]
			if !ok {
				return errors.Errorf("no such issue name %s", i)
			}
			issues = append(issues, issueFn)
		}

		if *verifyRepair {
			v = verifier.NewWithRepair(logger, bkt, backupBkt, issues)
		} else {
			v = verifier.New(logger, bkt, issues)
		}

		var idMatcher func(ulid.ULID) bool = nil
		if len(*verifyIDWhitelist) > 0 {
			whilelistIDs := map[string]struct{}{}
			for _, bid := range *verifyIDWhitelist {
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

	ls := cmd.Command("ls", "list all blocks in the bucket")
	lsOutput := ls.Flag("output", "Format in which to print each block's information. May be 'json' or custom template.").
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
			format     = *lsOutput
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

	inspect := cmd.Command("inspect", "inspect all blocks in the bucket")
	replica := inspect.Flag("replica ", "Filter by replica.").
		Default("").String()
	sortBy := inspect.Flag("sort-by", "Sort by columns.").
		Default("FROM,UNTIL").String()

	m[name+" inspect"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ bool) error {
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

		// Getting Metas
		var blockMetas []*block.Meta
		err = bkt.Iter(ctx, "", func(name string) error {
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
		})

		if err != nil {
			return err
		}

		return printTable(blockMetas, replica, sortBy)
	}
}

func printTable(blockMetas []*block.Meta, replica *string, sortBy *string) error {
	header := []string{"ULID", "FROM", "UNTIL", "RANGE", "UNTIL-COMP", "~SIZE", "#SERIES", "#SAMPLES", "#CHUNKS", "COMP-LEVEL", "COMP-FAILED", "REPLICA", "RESOLUTION", "SOURCE"}

	var lines [][]string
	p := message.NewPrinter(language.English)

	for _, blockMeta := range blockMetas {
		if *replica != "" && *replica != blockMeta.Thanos.Labels["replica"] {
			continue
		}

		var line []string
		line = append(line, blockMeta.ULID.String())
		line = append(line, time.Unix(blockMeta.MinTime/1000, 0).Format("02-01-2006 15:04:05"))
		line = append(line, time.Unix(blockMeta.MaxTime/1000, 0).Format("02-01-2006 15:04:05"))
		timeRange := time.Duration((blockMeta.MaxTime - blockMeta.MinTime) * 1000 * 1000)
		line = append(line, timeRange.String())
		untilComp := "-"
		if blockMeta.Thanos.Downsample.Resolution == 0 { // data currently raw, downsample if range >= 40 hours
			untilComp = (time.Duration(40*60*60*1000*time.Millisecond) - timeRange).String()
		}
		if blockMeta.Thanos.Downsample.Resolution == 5*60*1000 { // data currently 5m resolution, downsample if range >= 10 days
			untilComp = (time.Duration(10*24*60*60*1000*time.Millisecond) - timeRange).String()
		}
		line = append(line, untilComp)
		line = append(line, p.Sprintf("%0.2fMiB", (float64(blockMeta.Stats.NumSamples)*1.07)/(1024*1024)))
		line = append(line, p.Sprintf("%d", blockMeta.Stats.NumSeries))
		line = append(line, p.Sprintf("%d", blockMeta.Stats.NumSamples))
		line = append(line, p.Sprintf("%d", blockMeta.Stats.NumChunks))
		line = append(line, p.Sprintf("%d", blockMeta.Compaction.Level))
		line = append(line, p.Sprintf("%t", blockMeta.Compaction.Failed))
		line = append(line, blockMeta.Thanos.Labels["replica"])
		line = append(line, time.Duration(blockMeta.Thanos.Downsample.Resolution*1000000).String())
		line = append(line, string(blockMeta.Thanos.Source))

		lines = append(lines, line)
	}

	var sortByColNum []int
	for _, col := range strings.Split(*sortBy, ",") {
		index := getIndex(header, col)
		if index == -1 {
			return fmt.Errorf("column %s not found", col)
		}
		sortByColNum = append(sortByColNum, index)
	}

	output, err := table.ConvertToTable(table.Table{Header: header, Lines: lines, SortIndices: sortByColNum, Output: "markdown"})
	if err != nil {
		return err
	}
	fmt.Printf(output)
	return nil
}

func getIndex(cols []string, s string) int {
	for i, col := range cols {
		if col == s {
			return i
		}
	}
	return -1
}
