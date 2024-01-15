// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"

	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/olekukonko/tablewriter"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	prommodel "github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"gopkg.in/yaml.v3"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	objstoretracing "github.com/thanos-io/objstore/tracing/opentracing"

	v1 "github.com/thanos-io/thanos/pkg/api/blocks"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/compactv2"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/replicate"
	"github.com/thanos-io/thanos/pkg/runutil"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/ui"
	"github.com/thanos-io/thanos/pkg/verifier"
)

const extpromPrefix = "thanos_bucket_"

var (
	issuesVerifiersRegistry = verifier.Registry{
		Verifiers: []verifier.Verifier{verifier.OverlappedBlocksIssue{}},
		VerifierRepairers: []verifier.VerifierRepairer{
			verifier.IndexKnownIssues{},
			verifier.DuplicatedCompactionBlocks{},
		},
	}
	inspectColumns = []string{"ULID", "FROM", "UNTIL", "RANGE", "UNTIL-DOWN", "#SERIES", "#SAMPLES", "#CHUNKS", "COMP-LEVEL", "COMP-FAILED", "LABELS", "RESOLUTION", "SOURCE"}
	outputTypes    = []string{"table", "tsv", "csv"}
)

type outputType string

const (
	TABLE outputType = "table"
	CSV   outputType = "csv"
	TSV   outputType = "tsv"
)

type bucketRewriteConfig struct {
	blockIDs     []string
	tmpDir       string
	dryRun       bool
	promBlocks   bool
	deleteBlocks bool
}

type bucketInspectConfig struct {
	selector []string
	sortBy   []string
	timeout  time.Duration
}

type bucketVerifyConfig struct {
	repair         bool
	ids            []string
	issuesToVerify []string
}

type bucketLsConfig struct {
	output        string
	excludeDelete bool
}

type bucketWebConfig struct {
	webRoutePrefix         string
	webExternalPrefix      string
	webPrefixHeaderName    string
	webDisableCORS         bool
	interval               time.Duration
	label                  string
	timeout                time.Duration
	disableAdminOperations bool
}

type bucketReplicateConfig struct {
	resolutions []time.Duration
	compactMin  int
	compactMax  int
	compactions []int
	matcherStrs string
	singleRun   bool
}

type bucketDownsampleConfig struct {
	waitInterval          time.Duration
	downsampleConcurrency int
	blockFilesConcurrency int
	dataDir               string
	hashFunc              string
}

type bucketCleanupConfig struct {
	consistencyDelay     time.Duration
	blockSyncConcurrency int
	deleteDelay          time.Duration
}

type bucketRetentionConfig struct {
	consistencyDelay     time.Duration
	blockSyncConcurrency int
	deleteDelay          time.Duration
}

type bucketMarkBlockConfig struct {
	details      string
	marker       string
	blockIDs     []string
	removeMarker bool
}

type bucketUploadBlocksConfig struct {
	path   string
	labels []string
}

func (tbc *bucketVerifyConfig) registerBucketVerifyFlag(cmd extkingpin.FlagClause) *bucketVerifyConfig {
	cmd.Flag("repair", "Attempt to repair blocks for which issues were detected").
		Short('r').Default("false").BoolVar(&tbc.repair)

	cmd.Flag("issues", fmt.Sprintf("Issues to verify (and optionally repair). "+
		"Possible issue to verify, without repair: %v; Possible issue to verify and repair: %v",
		issuesVerifiersRegistry.VerifiersIDs(), issuesVerifiersRegistry.VerifierRepairersIDs())).
		Short('i').Default(verifier.IndexKnownIssues{}.IssueID(), verifier.OverlappedBlocksIssue{}.IssueID()).StringsVar(&tbc.issuesToVerify)

	cmd.Flag("id", "Block IDs to verify (and optionally repair) only. "+
		"If none is specified, all blocks will be verified. Repeated field").StringsVar(&tbc.ids)
	return tbc
}

func (tbc *bucketLsConfig) registerBucketLsFlag(cmd extkingpin.FlagClause) *bucketLsConfig {
	cmd.Flag("output", "Optional format in which to print each block's information. Options are 'json', 'wide' or a custom template.").
		Short('o').Default("").StringVar(&tbc.output)
	cmd.Flag("exclude-delete", "Exclude blocks marked for deletion.").
		Default("false").BoolVar(&tbc.excludeDelete)
	return tbc
}

func (tbc *bucketInspectConfig) registerBucketInspectFlag(cmd extkingpin.FlagClause) *bucketInspectConfig {
	cmd.Flag("selector", "Selects blocks based on label, e.g. '-l key1=\\\"value1\\\" -l key2=\\\"value2\\\"'. All key value pairs must match.").Short('l').
		PlaceHolder("<name>=\\\"<value>\\\"").StringsVar(&tbc.selector)
	cmd.Flag("sort-by", "Sort by columns. It's also possible to sort by multiple columns, e.g. '--sort-by FROM --sort-by UNTIL'. I.e., if the 'FROM' value is equal the rows are then further sorted by the 'UNTIL' value.").
		Default("FROM", "UNTIL").EnumsVar(&tbc.sortBy, inspectColumns...)
	cmd.Flag("timeout", "Timeout to download metadata from remote storage").Default("5m").DurationVar(&tbc.timeout)

	return tbc
}

func (tbc *bucketWebConfig) registerBucketWebFlag(cmd extkingpin.FlagClause) *bucketWebConfig {
	cmd.Flag("web.route-prefix", "Prefix for API and UI endpoints. This allows thanos UI to be served on a sub-path. Defaults to the value of --web.external-prefix. This option is analogous to --web.route-prefix of Prometheus.").Default("").StringVar(&tbc.webRoutePrefix)

	cmd.Flag("web.external-prefix", "Static prefix for all HTML links and redirect URLs in the bucket web UI interface. Actual endpoints are still served on / or the web.route-prefix. This allows thanos bucket web UI to be served behind a reverse proxy that strips a URL sub-path.").Default("").StringVar(&tbc.webExternalPrefix)

	cmd.Flag("web.prefix-header", "Name of HTTP request header used for dynamic prefixing of UI links and redirects. This option is ignored if web.external-prefix argument is set. Security risk: enable this option only if a reverse proxy in front of thanos is resetting the header. The --web.prefix-header=X-Forwarded-Prefix option can be useful, for example, if Thanos UI is served via Traefik reverse proxy with PathPrefixStrip option enabled, which sends the stripped prefix value in X-Forwarded-Prefix header. This allows thanos UI to be served on a sub-path.").Default("").StringVar(&tbc.webPrefixHeaderName)

	cmd.Flag("web.disable-cors", "Whether to disable CORS headers to be set by Thanos. By default Thanos sets CORS headers to be allowed by all.").Default("false").BoolVar(&tbc.webDisableCORS)

	cmd.Flag("refresh", "Refresh interval to download metadata from remote storage").Default("30m").DurationVar(&tbc.interval)

	cmd.Flag("timeout", "Timeout to download metadata from remote storage").Default("5m").DurationVar(&tbc.timeout)

	cmd.Flag("label", "External block label to use as group title").StringVar(&tbc.label)

	cmd.Flag("disable-admin-operations", "Disable UI/API admin operations like marking blocks for deletion and no compaction.").Default("false").BoolVar(&tbc.disableAdminOperations)
	return tbc
}

func (tbc *bucketReplicateConfig) registerBucketReplicateFlag(cmd extkingpin.FlagClause) *bucketReplicateConfig {
	cmd.Flag("resolution", "Only blocks with these resolutions will be replicated. Repeated flag.").Default("0s", "5m", "1h").HintAction(listResLevel).DurationListVar(&tbc.resolutions)

	cmd.Flag("compaction-min", "Only blocks with at least this compaction level will be replicated.").Default("1").IntVar(&tbc.compactMin)

	cmd.Flag("compaction-max", "Only blocks up to a maximum of this compaction level will be replicated.").Default("4").IntVar(&tbc.compactMax)

	cmd.Flag("compaction", "Only blocks with these compaction levels will be replicated. Repeated flag. Overrides compaction-min and compaction-max if set.").Default().IntsVar(&tbc.compactions)

	cmd.Flag("matcher", "blocks whose external labels match this matcher will be replicated. All Prometheus matchers are supported, including =, !=, =~ and !~.").StringVar(&tbc.matcherStrs)

	cmd.Flag("single-run", "Run replication only one time, then exit.").Default("false").BoolVar(&tbc.singleRun)

	return tbc
}

func (tbc *bucketRewriteConfig) registerBucketRewriteFlag(cmd extkingpin.FlagClause) *bucketRewriteConfig {
	cmd.Flag("id", "ID (ULID) of the blocks for rewrite (repeated flag).").Required().StringsVar(&tbc.blockIDs)
	cmd.Flag("tmp.dir", "Working directory for temporary files").Default(filepath.Join(os.TempDir(), "thanos-rewrite")).StringVar(&tbc.tmpDir)
	cmd.Flag("dry-run", "Prints the series changes instead of doing them. Defaults to true, for user to double check. (: Pass --no-dry-run to skip this.").Default("true").BoolVar(&tbc.dryRun)
	cmd.Flag("prom-blocks", "If specified, we assume the blocks to be uploaded are only used with Prometheus so we don't check external labels in this case.").Default("false").BoolVar(&tbc.promBlocks)
	cmd.Flag("delete-blocks", "Whether to delete the original blocks after rewriting blocks successfully. Available in non dry-run mode only.").Default("false").BoolVar(&tbc.deleteBlocks)

	return tbc
}

func (tbc *bucketDownsampleConfig) registerBucketDownsampleFlag(cmd extkingpin.FlagClause) *bucketDownsampleConfig {
	cmd.Flag("wait-interval", "Wait interval between downsample runs.").
		Default("5m").DurationVar(&tbc.waitInterval)
	cmd.Flag("downsample.concurrency", "Number of goroutines to use when downsampling blocks.").
		Default("1").IntVar(&tbc.downsampleConcurrency)
	cmd.Flag("block-files-concurrency", "Number of goroutines to use when fetching/uploading block files from object storage.").
		Default("1").IntVar(&tbc.blockFilesConcurrency)
	cmd.Flag("data-dir", "Data directory in which to cache blocks and process downsamplings.").
		Default("./data").StringVar(&tbc.dataDir)
	cmd.Flag("hash-func", "Specify which hash function to use when calculating the hashes of produced files. If no function has been specified, it does not happen. This permits avoiding downloading some files twice albeit at some performance cost. Possible values are: \"\", \"SHA256\".").
		Default("").EnumVar(&tbc.hashFunc, "SHA256", "")

	return tbc
}

func (tbc *bucketMarkBlockConfig) registerBucketMarkBlockFlag(cmd extkingpin.FlagClause) *bucketMarkBlockConfig {
	cmd.Flag("id", "ID (ULID) of the blocks to be marked for deletion (repeated flag)").Required().StringsVar(&tbc.blockIDs)
	cmd.Flag("marker", "Marker to be put.").Required().EnumVar(&tbc.marker, metadata.DeletionMarkFilename, metadata.NoCompactMarkFilename, metadata.NoDownsampleMarkFilename)
	cmd.Flag("details", "Human readable details to be put into marker.").StringVar(&tbc.details)
	cmd.Flag("remove", "Remove the marker.").Default("false").BoolVar(&tbc.removeMarker)
	return tbc
}

func (tbc *bucketCleanupConfig) registerBucketCleanupFlag(cmd extkingpin.FlagClause) *bucketCleanupConfig {
	cmd.Flag("delete-delay", "Time before a block marked for deletion is deleted from bucket.").Default("48h").DurationVar(&tbc.deleteDelay)
	cmd.Flag("consistency-delay", fmt.Sprintf("Minimum age of fresh (non-compacted) blocks before they are being processed. Malformed blocks older than the maximum of consistency-delay and %v will be removed.", compact.PartialUploadThresholdAge)).
		Default("30m").DurationVar(&tbc.consistencyDelay)
	cmd.Flag("block-sync-concurrency", "Number of goroutines to use when syncing block metadata from object storage.").
		Default("20").IntVar(&tbc.blockSyncConcurrency)
	return tbc
}

func (tbc *bucketRetentionConfig) registerBucketRetentionFlag(cmd extkingpin.FlagClause) *bucketRetentionConfig {
	cmd.Flag("delete-delay", "Time before a block marked for deletion is deleted from bucket.").Default("48h").DurationVar(&tbc.deleteDelay)
	cmd.Flag("consistency-delay", fmt.Sprintf("Minimum age of fresh (non-compacted) blocks before they are being processed. Malformed blocks older than the maximum of consistency-delay and %v will be removed.", compact.PartialUploadThresholdAge)).
		Default("30m").DurationVar(&tbc.consistencyDelay)
	cmd.Flag("block-sync-concurrency", "Number of goroutines to use when syncing block metadata from object storage.").
		Default("20").IntVar(&tbc.blockSyncConcurrency)

	return tbc
}

func (tbc *bucketUploadBlocksConfig) registerBucketUploadBlocksFlag(cmd extkingpin.FlagClause) *bucketUploadBlocksConfig {
	cmd.Flag("path", "Path to the directory containing blocks to upload.").Default("./data").StringVar(&tbc.path)
	cmd.Flag("label", "External labels to add to the uploaded blocks (repeated).").PlaceHolder("key=\"value\"").StringsVar(&tbc.labels)

	return tbc
}

func registerBucket(app extkingpin.AppClause) {
	cmd := app.Command("bucket", "Bucket utility commands")

	objStoreConfig := extkingpin.RegisterCommonObjStoreFlags(cmd, "", true)
	registerBucketVerify(cmd, objStoreConfig)
	registerBucketLs(cmd, objStoreConfig)
	registerBucketInspect(cmd, objStoreConfig)
	registerBucketWeb(cmd, objStoreConfig)
	registerBucketReplicate(cmd, objStoreConfig)
	registerBucketDownsample(cmd, objStoreConfig)
	registerBucketCleanup(cmd, objStoreConfig)
	registerBucketMarkBlock(cmd, objStoreConfig)
	registerBucketRewrite(cmd, objStoreConfig)
	registerBucketRetention(cmd, objStoreConfig)
	registerBucketUploadBlocks(cmd, objStoreConfig)
}

func registerBucketVerify(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command("verify", "Verify all blocks in the bucket against specified issues. NOTE: Depending on issue this might take time and will need downloading all specified blocks to disk.")
	objStoreBackupConfig := extkingpin.RegisterCommonObjStoreFlags(cmd, "-backup", false, "Used for repair logic to backup blocks before removal.")

	tbc := &bucketVerifyConfig{}
	tbc.registerBucketVerifyFlag(cmd)

	deleteDelay := extkingpin.ModelDuration(cmd.Flag("delete-delay", "Duration after which blocks marked for deletion would be deleted permanently from source bucket by compactor component. "+
		"If delete-delay is non zero, blocks will be marked for deletion and compactor component is required to delete blocks from source bucket. "+
		"If delete-delay is 0, blocks will be deleted straight away. Use this if you want to get rid of or move the block immediately. "+
		"Note that deleting blocks immediately can cause query failures, if store gateway still has the block loaded, "+
		"or compactor is ignoring the deletion because it's compacting the block at the same time.").
		Default("0s"))
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, component.Bucket.String())
		if err != nil {
			return err
		}
		insBkt := objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))
		defer runutil.CloseWithLogOnErr(logger, insBkt, "bucket client")

		backupconfContentYaml, err := objStoreBackupConfig.Content()
		if err != nil {
			return err
		}

		var backupBkt objstore.Bucket
		if len(backupconfContentYaml) == 0 {
			if tbc.repair {
				return errors.New("repair is specified, so backup client is required")
			}
		} else {
			// nil Prometheus registerer: don't create conflicting metrics.
			backupBkt, err = client.NewBucket(logger, backupconfContentYaml, component.Bucket.String())
			if err != nil {
				return err
			}
			insBkt = objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, nil, bkt.Name()))

			defer runutil.CloseWithLogOnErr(logger, backupBkt, "backup bucket client")
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		r, err := issuesVerifiersRegistry.SubstractByIDs(tbc.issuesToVerify, tbc.repair)
		if err != nil {
			return err
		}

		// We ignore any block that has the deletion marker file.
		filters := []block.MetadataFilter{block.NewIgnoreDeletionMarkFilter(logger, insBkt, 0, block.FetcherConcurrency)}
		baseBlockIDsFetcher := block.NewBaseBlockIDsFetcher(logger, insBkt)
		fetcher, err := block.NewMetaFetcher(logger, block.FetcherConcurrency, insBkt, baseBlockIDsFetcher, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg), filters)
		if err != nil {
			return err
		}

		var idMatcher func(ulid.ULID) bool = nil
		if len(tbc.ids) > 0 {
			idsMap := map[string]struct{}{}
			for _, bid := range tbc.ids {
				id, err := ulid.Parse(bid)
				if err != nil {
					return errors.Wrap(err, "invalid ULID found in --id flag")
				}
				idsMap[id.String()] = struct{}{}
			}

			idMatcher = func(id ulid.ULID) bool {
				if _, ok := idsMap[id.String()]; !ok {
					return false
				}
				return true
			}
		}

		v := verifier.NewManager(reg, logger, insBkt, backupBkt, fetcher, time.Duration(*deleteDelay), r)
		if tbc.repair {
			return v.VerifyAndRepair(context.Background(), idMatcher)
		}

		return v.Verify(context.Background(), idMatcher)
	})
}

func registerBucketLs(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command("ls", "List all blocks in the bucket.")

	tbc := &bucketLsConfig{}
	tbc.registerBucketLsFlag(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, component.Bucket.String())
		if err != nil {
			return err
		}
		insBkt := objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))

		var filters []block.MetadataFilter

		if tbc.excludeDelete {
			ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(logger, insBkt, 0, block.FetcherConcurrency)
			filters = append(filters, ignoreDeletionMarkFilter)
		}
		baseBlockIDsFetcher := block.NewBaseBlockIDsFetcher(logger, insBkt)
		fetcher, err := block.NewMetaFetcher(logger, block.FetcherConcurrency, insBkt, baseBlockIDsFetcher, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg), filters)
		if err != nil {
			return err
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		defer runutil.CloseWithLogOnErr(logger, insBkt, "bucket client")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		var (
			format     = tbc.output
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
					m.ULID, minTime.Format(time.RFC3339), maxTime.Format(time.RFC3339), maxTime.Sub(minTime),
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
	})
}

func registerBucketInspect(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command("inspect", "Inspect all blocks in the bucket in detailed, table-like way.")

	tbc := &bucketInspectConfig{}
	tbc.registerBucketInspectFlag(cmd)

	output := cmd.Flag("output", "Output format for result. Currently supports table, cvs, tsv.").Default("table").Enum(outputTypes...)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {

		// Parse selector.
		selectorLabels, err := parseFlagLabels(tbc.selector)
		if err != nil {
			return errors.Wrap(err, "error parsing selector flag")
		}

		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, component.Bucket.String())
		if err != nil {
			return err
		}
		insBkt := objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))

		baseBlockIDsFetcher := block.NewBaseBlockIDsFetcher(logger, insBkt)
		fetcher, err := block.NewMetaFetcher(logger, block.FetcherConcurrency, insBkt, baseBlockIDsFetcher, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg), nil)
		if err != nil {
			return err
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		defer runutil.CloseWithLogOnErr(logger, insBkt, "bucket client")

		ctx, cancel := context.WithTimeout(context.Background(), tbc.timeout)
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

		var opPrinter tablePrinter
		op := outputType(*output)
		switch op {
		case TABLE:
			opPrinter = printTable
		case TSV:
			opPrinter = printTSV
		case CSV:
			opPrinter = printCSV
		}
		return printBlockData(blockMetas, selectorLabels, tbc.sortBy, opPrinter)
	})
}

// registerBucketWeb exposes a web interface for the state of remote store like `pprof web`.
func registerBucketWeb(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command("web", "Web interface for remote storage bucket.")
	httpBindAddr, httpGracePeriod, httpTLSConfig := extkingpin.RegisterHTTPFlags(cmd)

	tbc := &bucketWebConfig{}
	tbc.registerBucketWebFlag(cmd)

	filterConf := &store.FilterConfig{}
	cmd.Flag("min-time", "Start of time range limit to serve. Thanos tool bucket web will serve only blocks, which happened later than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z").SetValue(&filterConf.MinTime)
	cmd.Flag("max-time", "End of time range limit to serve. Thanos tool bucket web will serve only blocks, which happened earlier than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("9999-12-31T23:59:59Z").SetValue(&filterConf.MaxTime)
	selectorRelabelConf := *extkingpin.RegisterSelectorRelabelFlags(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		comp := component.Bucket
		httpProbe := prober.NewHTTP()
		statusProber := prober.Combine(
			httpProbe,
			prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
		)

		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(*httpBindAddr),
			httpserver.WithGracePeriod(time.Duration(*httpGracePeriod)),
			httpserver.WithTLSConfig(*httpTLSConfig),
		)

		if tbc.webRoutePrefix == "" {
			tbc.webRoutePrefix = tbc.webExternalPrefix
		}

		if tbc.webRoutePrefix != tbc.webExternalPrefix {
			level.Warn(logger).Log("msg", "different values for --web.route-prefix and --web.external-prefix detected, web UI may not work without a reverse-proxy.")
		}

		router := route.New()

		// RoutePrefix must always start with '/'.
		tbc.webRoutePrefix = "/" + strings.Trim(tbc.webRoutePrefix, "/")

		// Redirect from / to /webRoutePrefix.
		if tbc.webRoutePrefix != "/" {
			router.Get("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, tbc.webRoutePrefix+"/", http.StatusFound)
			})
			router.Get(tbc.webRoutePrefix, func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, tbc.webRoutePrefix+"/", http.StatusFound)
			})
			router = router.WithPrefix(tbc.webRoutePrefix)
		}

		ins := extpromhttp.NewInstrumentationMiddleware(reg, nil)

		bucketUI := ui.NewBucketUI(logger, tbc.webExternalPrefix, tbc.webPrefixHeaderName, component.Bucket)
		bucketUI.Register(router, ins)

		flagsMap := getFlagsMap(cmd.Flags())

		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, component.Bucket.String())
		if err != nil {
			return errors.Wrap(err, "bucket client")
		}
		insBkt := objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))

		api := v1.NewBlocksAPI(logger, tbc.webDisableCORS, tbc.label, flagsMap, insBkt)

		// Configure Request Logging for HTTP calls.
		opts := []logging.Option{logging.WithDecider(func(_ string, _ error) logging.Decision {
			return logging.NoLogCall
		})}
		logMiddleware := logging.NewHTTPServerMiddleware(logger, opts...)

		api.Register(router.WithPrefix("/api/v1"), tracer, logger, ins, logMiddleware)

		srv.Handle("/", router)

		if tbc.interval < 5*time.Minute {
			level.Warn(logger).Log("msg", "Refreshing more often than 5m could lead to large data transfers")
		}

		if tbc.timeout < time.Minute {
			level.Warn(logger).Log("msg", "Timeout less than 1m could lead to frequent failures")
		}

		if tbc.interval < (tbc.timeout * 2) {
			level.Warn(logger).Log("msg", "Refresh interval should be at least 2 times the timeout")
		}

		relabelContentYaml, err := selectorRelabelConf.Content()
		if err != nil {
			return errors.Wrap(err, "get content of relabel configuration")
		}

		relabelConfig, err := block.ParseRelabelConfig(relabelContentYaml, block.SelectorSupportedRelabelActions)
		if err != nil {
			return err
		}
		// TODO(bwplotka): Allow Bucket UI to visualize the state of block as well.
		baseBlockIDsFetcher := block.NewBaseBlockIDsFetcher(logger, insBkt)
		fetcher, err := block.NewMetaFetcher(logger, block.FetcherConcurrency, insBkt, baseBlockIDsFetcher, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg),
			[]block.MetadataFilter{
				block.NewTimePartitionMetaFilter(filterConf.MinTime, filterConf.MaxTime),
				block.NewLabelShardedMetaFilter(relabelConfig),
				block.NewDeduplicateFilter(block.FetcherConcurrency),
			})
		if err != nil {
			return err
		}
		fetcher.UpdateOnChange(func(blocks []metadata.Meta, err error) {
			api.SetGlobal(blocks, err)
		})

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			statusProber.Ready()
			defer runutil.CloseWithLogOnErr(logger, insBkt, "bucket client")
			return runutil.Repeat(tbc.interval, ctx.Done(), func() error {
				return runutil.RetryWithLog(logger, time.Minute, ctx.Done(), func() error {
					iterCtx, iterCancel := context.WithTimeout(ctx, tbc.timeout)
					defer iterCancel()

					_, _, err := fetcher.Fetch(iterCtx)
					return err
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
	})
}

// Provide a list of resolution, can not use Enum directly, since string does not implement int64 function.
func listResLevel() []string {
	return []string{
		time.Duration(downsample.ResLevel0).String(),
		time.Duration(downsample.ResLevel1).String(),
		time.Duration(downsample.ResLevel2).String()}
}

func registerBucketReplicate(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command("replicate", fmt.Sprintf("Replicate data from one object storage to another. NOTE: Currently it works only with Thanos blocks (%v has to have Thanos metadata).", block.MetaFilename))
	httpBindAddr, httpGracePeriod, httpTLSConfig := extkingpin.RegisterHTTPFlags(cmd)
	toObjStoreConfig := extkingpin.RegisterCommonObjStoreFlags(cmd, "-to", false, "The object storage which replicate data to.")

	tbc := &bucketReplicateConfig{}
	tbc.registerBucketReplicateFlag(cmd)

	minTime := model.TimeOrDuration(cmd.Flag("min-time", "Start of time range limit to replicate. Thanos Replicate will replicate only metrics, which happened later than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z"))
	maxTime := model.TimeOrDuration(cmd.Flag("max-time", "End of time range limit to replicate. Thanos Replicate will replicate only metrics, which happened earlier than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("9999-12-31T23:59:59Z"))
	ids := cmd.Flag("id", "Block to be replicated to the destination bucket. IDs will be used to match blocks and other matchers will be ignored. When specified, this command will be run only once after successful replication. Repeated field").Strings()
	ignoreMarkedForDeletion := cmd.Flag("ignore-marked-for-deletion", "Do not replicate blocks that have deletion mark.").Bool()

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		matchers, err := replicate.ParseFlagMatchers(tbc.matcherStrs)
		if err != nil {
			return errors.Wrap(err, "parse block label matchers")
		}

		var resolutionLevels []compact.ResolutionLevel
		for _, lvl := range tbc.resolutions {
			resolutionLevels = append(resolutionLevels, compact.ResolutionLevel(lvl.Milliseconds()))
		}

		if len(tbc.compactions) == 0 {
			if tbc.compactMin > tbc.compactMax {
				return errors.New("compaction-min must be less than or equal to compaction-max")
			}
			tbc.compactions = []int{}
			for compactionLevel := tbc.compactMin; compactionLevel <= tbc.compactMax; compactionLevel++ {
				tbc.compactions = append(tbc.compactions, compactionLevel)
			}
		}

		blockIDs := make([]ulid.ULID, 0, len(*ids))
		for _, id := range *ids {
			bid, err := ulid.Parse(id)
			if err != nil {
				return errors.Wrap(err, "invalid ULID found in --id flag")
			}
			blockIDs = append(blockIDs, bid)
		}

		return replicate.RunReplicate(
			g,
			logger,
			reg,
			tracer,
			*httpBindAddr,
			*httpTLSConfig,
			time.Duration(*httpGracePeriod),
			matchers,
			resolutionLevels,
			tbc.compactions,
			objStoreConfig,
			toObjStoreConfig,
			tbc.singleRun,
			minTime,
			maxTime,
			blockIDs,
			*ignoreMarkedForDeletion,
		)
	})
}

func registerBucketDownsample(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command(component.Downsample.String(), "Continuously downsamples blocks in an object store bucket.")
	httpAddr, httpGracePeriod, httpTLSConfig := extkingpin.RegisterHTTPFlags(cmd)

	tbc := &bucketDownsampleConfig{}
	tbc.registerBucketDownsampleFlag(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		return RunDownsample(g, logger, reg, *httpAddr, *httpTLSConfig, time.Duration(*httpGracePeriod), tbc.dataDir,
			tbc.waitInterval, tbc.downsampleConcurrency, tbc.blockFilesConcurrency, objStoreConfig, component.Downsample, metadata.HashFunc(tbc.hashFunc))
	})
}

func registerBucketCleanup(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command(component.Cleanup.String(), "Cleans up all blocks marked for deletion.")

	tbc := &bucketCleanupConfig{}
	tbc.registerBucketCleanupFlag(cmd)

	selectorRelabelConf := extkingpin.RegisterSelectorRelabelFlags(cmd)
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		relabelContentYaml, err := selectorRelabelConf.Content()
		if err != nil {
			return errors.Wrap(err, "get content of relabel configuration")
		}

		relabelConfig, err := block.ParseRelabelConfig(relabelContentYaml, block.SelectorSupportedRelabelActions)
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, component.Cleanup.String())
		if err != nil {
			return err
		}
		insBkt := objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		stubCounter := promauto.With(nil).NewCounter(prometheus.CounterOpts{})

		// While fetching blocks, we filter out blocks that were marked for deletion by using IgnoreDeletionMarkFilter.
		// The delay of deleteDelay/2 is added to ensure we fetch blocks that are meant to be deleted but do not have a replacement yet.
		// This is to make sure compactor will not accidentally perform compactions with gap instead.
		ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(logger, insBkt, tbc.deleteDelay/2, tbc.blockSyncConcurrency)
		duplicateBlocksFilter := block.NewDeduplicateFilter(tbc.blockSyncConcurrency)
		blocksCleaner := compact.NewBlocksCleaner(logger, insBkt, ignoreDeletionMarkFilter, tbc.deleteDelay, stubCounter, stubCounter)

		ctx := context.Background()

		var sy *compact.Syncer
		{
			baseBlockIDsFetcher := block.NewBaseBlockIDsFetcher(logger, insBkt)
			baseMetaFetcher, err := block.NewBaseFetcher(logger, tbc.blockSyncConcurrency, insBkt, baseBlockIDsFetcher, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg))
			if err != nil {
				return errors.Wrap(err, "create meta fetcher")
			}
			cf := baseMetaFetcher.NewMetaFetcher(
				extprom.WrapRegistererWithPrefix(extpromPrefix, reg), []block.MetadataFilter{
					block.NewLabelShardedMetaFilter(relabelConfig),
					block.NewConsistencyDelayMetaFilter(logger, tbc.consistencyDelay, extprom.WrapRegistererWithPrefix(extpromPrefix, reg)),
					ignoreDeletionMarkFilter,
					duplicateBlocksFilter,
				},
			)
			sy, err = compact.NewMetaSyncer(
				logger,
				reg,
				insBkt,
				cf,
				duplicateBlocksFilter,
				ignoreDeletionMarkFilter,
				stubCounter,
				stubCounter,
			)
			if err != nil {
				return errors.Wrap(err, "create syncer")
			}
		}

		level.Info(logger).Log("msg", "syncing blocks metadata")
		if err := sy.SyncMetas(ctx); err != nil {
			return errors.Wrap(err, "sync blocks")
		}

		level.Info(logger).Log("msg", "synced blocks done")

		compact.BestEffortCleanAbortedPartialUploads(ctx, logger, sy.Partial(), insBkt, stubCounter, stubCounter, stubCounter)
		if err := blocksCleaner.DeleteMarkedBlocks(ctx); err != nil {
			return errors.Wrap(err, "error cleaning blocks")
		}

		level.Info(logger).Log("msg", "cleanup done")
		return nil
	})
}

type tablePrinter func(w io.Writer, t Table) error

func printTable(w io.Writer, t Table) error {
	table := tablewriter.NewWriter(w)
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

func printCSV(w io.Writer, t Table) error {
	csv := csv.NewWriter(w)
	err := csv.Write(t.Header)
	if err != nil {
		return err
	}
	err = csv.WriteAll(t.Lines)
	if err != nil {
		return err
	}
	csv.Flush()
	return nil
}

func newTSVWriter(w io.Writer) *csv.Writer {
	writer := csv.NewWriter(w)
	writer.Comma = rune('\t')
	return writer
}

func printTSV(w io.Writer, t Table) error {
	tsv := newTSVWriter(w)
	err := tsv.Write(t.Header)
	if err != nil {
		return err
	}
	err = tsv.WriteAll(t.Lines)
	if err != nil {
		return err
	}
	tsv.Flush()
	return nil
}

func printBlockData(blockMetas []*metadata.Meta, selectorLabels labels.Labels, sortBy []string, printer tablePrinter) error {
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
		line = append(line,
			blockMeta.ULID.String(),
			time.Unix(blockMeta.MinTime/1000, 0).Format(time.RFC3339),
			time.Unix(blockMeta.MaxTime/1000, 0).Format(time.RFC3339),
			timeRange.String(),
			untilDown,
			p.Sprintf("%d", blockMeta.Stats.NumSeries),
			p.Sprintf("%d", blockMeta.Stats.NumSamples),
			p.Sprintf("%d", blockMeta.Stats.NumChunks),
			p.Sprintf("%d", blockMeta.Compaction.Level),
			p.Sprintf("%t", blockMeta.Compaction.Failed),
			strings.Join(labels, ","),
			time.Duration(blockMeta.Thanos.Downsample.Resolution*int64(time.Millisecond)).String(),
			string(blockMeta.Thanos.Source))

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
	err := printer(os.Stdout, t)
	if err != nil {
		return errors.Errorf("unable to write output.")
	}
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
	// Values can be either Time, Duration, comma-delimited integers or strings.
	s1Time, s1Err := time.Parse(time.RFC3339, s1)
	s2Time, s2Err := time.Parse(time.RFC3339, s2)
	if s1Err != nil || s2Err != nil {
		s1Duration, s1Err := time.ParseDuration(s1)
		s2Duration, s2Err := time.ParseDuration(s2)
		if s1Err != nil || s2Err != nil {
			s1Int, s1Err := strconv.ParseUint(strings.ReplaceAll(s1, ",", ""), 10, 64)
			s2Int, s2Err := strconv.ParseUint(strings.ReplaceAll(s2, ",", ""), 10, 64)
			if s1Err != nil || s2Err != nil {
				return s1 < s2
			}
			return s1Int < s2Int
		}
		return s1Duration < s2Duration
	}
	return s1Time.Before(s2Time)
}

func registerBucketMarkBlock(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command(component.Mark.String(), "Mark block for deletion or no-compact in a safe way. NOTE: If the compactor is currently running compacting same block, this operation would be potentially a noop.")

	tbc := &bucketMarkBlockConfig{}
	tbc.registerBucketMarkBlockFlag(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, component.Mark.String())
		if err != nil {
			return err
		}
		insBkt := objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))

		var ids []ulid.ULID
		for _, id := range tbc.blockIDs {
			u, err := ulid.Parse(id)
			if err != nil {
				return errors.Errorf("block.id is not a valid UUID, got: %v", id)
			}
			ids = append(ids, u)
		}

		if !tbc.removeMarker && tbc.details == "" {
			return errors.Errorf("required flag --details not provided")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		g.Add(func() error {
			for _, id := range ids {
				if tbc.removeMarker {
					err := block.RemoveMark(ctx, logger, insBkt, id, promauto.With(nil).NewCounter(prometheus.CounterOpts{}), tbc.marker)
					if err != nil {
						return errors.Wrapf(err, "remove mark %v for %v", id, tbc.marker)
					}
					continue
				}
				switch tbc.marker {
				case metadata.DeletionMarkFilename:
					if err := block.MarkForDeletion(ctx, logger, insBkt, id, tbc.details, promauto.With(nil).NewCounter(prometheus.CounterOpts{})); err != nil {
						return errors.Wrapf(err, "mark %v for %v", id, tbc.marker)
					}
				case metadata.NoCompactMarkFilename:
					if err := block.MarkForNoCompact(ctx, logger, insBkt, id, metadata.ManualNoCompactReason, tbc.details, promauto.With(nil).NewCounter(prometheus.CounterOpts{})); err != nil {
						return errors.Wrapf(err, "mark %v for %v", id, tbc.marker)
					}
				case metadata.NoDownsampleMarkFilename:
					if err := block.MarkForNoDownsample(ctx, logger, insBkt, id, metadata.ManualNoDownsampleReason, tbc.details, promauto.With(nil).NewCounter(prometheus.CounterOpts{})); err != nil {
						return errors.Wrapf(err, "mark %v for %v", id, tbc.marker)
					}
				default:
					return errors.Errorf("not supported marker %v", tbc.marker)
				}
			}
			level.Info(logger).Log("msg", "marking done", "marker", tbc.marker, "IDs", strings.Join(tbc.blockIDs, ","))
			return nil
		}, func(err error) {
			cancel()
		})
		return nil
	})
}

func registerBucketRewrite(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command(component.Rewrite.String(), "Rewrite chosen blocks in the bucket, while deleting or modifying series "+
		"Resulted block has modified stats in meta.json. Additionally compaction.sources are altered to not confuse readers of meta.json. "+
		"Instead thanos.rewrite section is added with useful info like old sources and deletion requests. "+
		"NOTE: It's recommended to turn off compactor while doing this operation. If the compactor is running and touching exactly same block that "+
		"is being rewritten, the resulted rewritten block might only cause overlap (mitigated by marking overlapping block manually for deletion) "+
		"and the data you wanted to rewrite could already part of bigger block.\n\n"+
		"Use FILESYSTEM type of bucket to rewrite block on disk (suitable for vanilla Prometheus) "+
		"After rewrite, it's caller responsibility to delete or mark source block for deletion to avoid overlaps. "+
		"WARNING: This procedure is *IRREVERSIBLE* after certain time (delete delay), so do backup your blocks first.")

	tbc := &bucketRewriteConfig{}
	tbc.registerBucketRewriteFlag(cmd)

	hashFunc := cmd.Flag("hash-func", "Specify which hash function to use when calculating the hashes of produced files. If no function has been specified, it does not happen. This permits avoiding downloading some files twice albeit at some performance cost. Possible values are: \"\", \"SHA256\".").
		Default("").Enum("SHA256", "")
	toDelete := extflag.RegisterPathOrContent(cmd, "rewrite.to-delete-config", "YAML file that contains []metadata.DeletionRequest that will be applied to blocks", extflag.WithEnvSubstitution())
	toRelabel := extflag.RegisterPathOrContent(cmd, "rewrite.to-relabel-config", "YAML file that contains relabel configs that will be applied to blocks", extflag.WithEnvSubstitution())
	provideChangeLog := cmd.Flag("rewrite.add-change-log", "If specified, all modifications are written to new block directory. Disable if latency is to high.").Default("true").Bool()
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, component.Rewrite.String())
		if err != nil {
			return err
		}
		insBkt := objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))

		var modifiers []compactv2.Modifier

		relabelYaml, err := toRelabel.Content()
		if err != nil {
			return err
		}
		var relabels []*relabel.Config
		if len(relabelYaml) > 0 {
			relabels, err = block.ParseRelabelConfig(relabelYaml, nil)
			if err != nil {
				return err
			}
			modifiers = append(modifiers, compactv2.WithRelabelModifier(relabels...))
		}

		deletionsYaml, err := toDelete.Content()
		if err != nil {
			return err
		}
		var deletions []metadata.DeletionRequest
		if len(deletionsYaml) > 0 {
			if err := yaml.Unmarshal(deletionsYaml, &deletions); err != nil {
				return err
			}
			modifiers = append(modifiers, compactv2.WithDeletionModifier(deletions...))
		}

		if len(modifiers) == 0 {
			return errors.New("rewrite configuration should be provided")
		}

		var ids []ulid.ULID
		for _, id := range tbc.blockIDs {
			u, err := ulid.Parse(id)
			if err != nil {
				return errors.Errorf("id is not a valid block ULID, got: %v", id)
			}
			ids = append(ids, u)
		}

		if err := os.RemoveAll(tbc.tmpDir); err != nil {
			return err
		}
		if err := os.MkdirAll(tbc.tmpDir, os.ModePerm); err != nil {
			return err
		}

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			chunkPool := chunkenc.NewPool()
			changeLog := compactv2.NewChangeLog(io.Discard)
			stubCounter := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			for _, id := range ids {
				// Delete series from block & modify.
				level.Info(logger).Log("msg", "downloading block", "source", id)
				if err := block.Download(ctx, logger, insBkt, id, filepath.Join(tbc.tmpDir, id.String())); err != nil {
					return errors.Wrapf(err, "download %v", id)
				}

				meta, err := metadata.ReadFromDir(filepath.Join(tbc.tmpDir, id.String()))
				if err != nil {
					return errors.Wrapf(err, "read meta of %v", id)
				}
				b, err := tsdb.OpenBlock(logger, filepath.Join(tbc.tmpDir, id.String()), chunkPool)
				if err != nil {
					return errors.Wrapf(err, "open block %v", id)
				}

				p := compactv2.NewProgressLogger(logger, int(b.Meta().Stats.NumSeries))
				newID := ulid.MustNew(ulid.Now(), rand.Reader)
				meta.ULID = newID
				meta.Thanos.Rewrites = append(meta.Thanos.Rewrites, metadata.Rewrite{
					Sources:          meta.Compaction.Sources,
					DeletionsApplied: deletions,
					RelabelsApplied:  relabels,
				})
				meta.Compaction.Sources = []ulid.ULID{newID}
				meta.Thanos.Source = metadata.BucketRewriteSource

				if err := os.MkdirAll(filepath.Join(tbc.tmpDir, newID.String()), os.ModePerm); err != nil {
					return err
				}

				if *provideChangeLog {
					f, err := os.OpenFile(filepath.Join(tbc.tmpDir, newID.String(), "change.log"), os.O_CREATE|os.O_WRONLY, os.ModePerm)
					if err != nil {
						return err
					}
					defer runutil.CloseWithLogOnErr(logger, f, "close changelog")

					changeLog = compactv2.NewChangeLog(f)
					level.Info(logger).Log("msg", "changelog will be available", "file", filepath.Join(tbc.tmpDir, newID.String(), "change.log"))
				}

				d, err := block.NewDiskWriter(ctx, logger, filepath.Join(tbc.tmpDir, newID.String()))
				if err != nil {
					return err
				}

				var comp *compactv2.Compactor
				if tbc.dryRun {
					comp = compactv2.NewDryRun(tbc.tmpDir, logger, changeLog, chunkPool)
				} else {
					comp = compactv2.New(tbc.tmpDir, logger, changeLog, chunkPool)
				}

				level.Info(logger).Log("msg", "starting rewrite for block", "source", id, "new", newID, "toDelete", string(deletionsYaml), "toRelabel", string(relabelYaml))
				if err := comp.WriteSeries(ctx, []block.Reader{b}, d, p, modifiers...); err != nil {
					return errors.Wrapf(err, "writing series from %v to %v", id, newID)
				}

				if tbc.dryRun {
					level.Info(logger).Log("msg", "dry run finished. Changes should be printed to stderr", "Block ID", id)
					continue
				}

				level.Info(logger).Log("msg", "wrote new block after modifications; flushing", "source", id, "new", newID)
				meta.Stats, err = d.Flush()
				if err != nil {
					return errors.Wrap(err, "flush")
				}
				if err := meta.WriteToDir(logger, filepath.Join(tbc.tmpDir, newID.String())); err != nil {
					return err
				}

				level.Info(logger).Log("msg", "uploading new block", "source", id, "new", newID)
				if tbc.promBlocks {
					if err := block.UploadPromBlock(ctx, logger, insBkt, filepath.Join(tbc.tmpDir, newID.String()), metadata.HashFunc(*hashFunc)); err != nil {
						return errors.Wrap(err, "upload")
					}
				} else {
					if err := block.Upload(ctx, logger, insBkt, filepath.Join(tbc.tmpDir, newID.String()), metadata.HashFunc(*hashFunc)); err != nil {
						return errors.Wrap(err, "upload")
					}
				}
				level.Info(logger).Log("msg", "uploaded", "source", id, "new", newID)

				if !tbc.dryRun && tbc.deleteBlocks {
					if err := block.MarkForDeletion(ctx, logger, insBkt, id, "block rewritten", stubCounter); err != nil {
						level.Error(logger).Log("msg", "failed to mark block for deletion", "id", id.String(), "err", err)
					}
				}
			}
			level.Info(logger).Log("msg", "rewrite done", "IDs", strings.Join(tbc.blockIDs, ","))
			return nil
		}, func(err error) {
			cancel()
		})
		return nil
	})
}

func registerBucketRetention(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	var (
		retentionRaw, retentionFiveMin, retentionOneHr prommodel.Duration
	)

	cmd := app.Command("retention", "Retention applies retention policies on the given bucket. Please make sure no compactor is running on the same bucket at the same time.")

	tbc := &bucketRetentionConfig{}
	tbc.registerBucketRetentionFlag(cmd)

	selectorRelabelConf := extkingpin.RegisterSelectorRelabelFlags(cmd)
	cmd.Flag("retention.resolution-raw",
		"How long to retain raw samples in bucket. Setting this to 0d will retain samples of this resolution forever").
		Default("0d").SetValue(&retentionRaw)
	cmd.Flag("retention.resolution-5m", "How long to retain samples of resolution 1 (5 minutes) in bucket. Setting this to 0d will retain samples of this resolution forever").
		Default("0d").SetValue(&retentionFiveMin)
	cmd.Flag("retention.resolution-1h", "How long to retain samples of resolution 2 (1 hour) in bucket. Setting this to 0d will retain samples of this resolution forever").
		Default("0d").SetValue(&retentionOneHr)
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		retentionByResolution := map[compact.ResolutionLevel]time.Duration{
			compact.ResolutionLevelRaw: time.Duration(retentionRaw),
			compact.ResolutionLevel5m:  time.Duration(retentionFiveMin),
			compact.ResolutionLevel1h:  time.Duration(retentionOneHr),
		}

		if retentionByResolution[compact.ResolutionLevelRaw].Seconds() != 0 {
			level.Info(logger).Log("msg", "retention policy of raw samples is enabled", "duration", retentionByResolution[compact.ResolutionLevelRaw])
		}
		if retentionByResolution[compact.ResolutionLevel5m].Seconds() != 0 {
			level.Info(logger).Log("msg", "retention policy of 5 min aggregated samples is enabled", "duration", retentionByResolution[compact.ResolutionLevel5m])
		}
		if retentionByResolution[compact.ResolutionLevel1h].Seconds() != 0 {
			level.Info(logger).Log("msg", "retention policy of 1 hour aggregated samples is enabled", "duration", retentionByResolution[compact.ResolutionLevel1h])
		}

		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		relabelContentYaml, err := selectorRelabelConf.Content()
		if err != nil {
			return errors.Wrap(err, "get content of relabel configuration")
		}

		relabelConfig, err := block.ParseRelabelConfig(relabelContentYaml, block.SelectorSupportedRelabelActions)
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, component.Retention.String())
		if err != nil {
			return err
		}
		insBkt := objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		defer runutil.CloseWithLogOnErr(logger, insBkt, "bucket client")

		// While fetching blocks, we filter out blocks that were marked for deletion by using IgnoreDeletionMarkFilter.
		// The delay of deleteDelay/2 is added to ensure we fetch blocks that are meant to be deleted but do not have a replacement yet.
		// This is to make sure compactor will not accidentally perform compactions with gap instead.
		ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(logger, insBkt, tbc.deleteDelay/2, tbc.blockSyncConcurrency)
		duplicateBlocksFilter := block.NewDeduplicateFilter(tbc.blockSyncConcurrency)
		stubCounter := promauto.With(nil).NewCounter(prometheus.CounterOpts{})

		var sy *compact.Syncer
		{
			baseBlockIDsFetcher := block.NewBaseBlockIDsFetcher(logger, insBkt)
			baseMetaFetcher, err := block.NewBaseFetcher(logger, tbc.blockSyncConcurrency, insBkt, baseBlockIDsFetcher, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg))
			if err != nil {
				return errors.Wrap(err, "create meta fetcher")
			}
			cf := baseMetaFetcher.NewMetaFetcher(
				extprom.WrapRegistererWithPrefix(extpromPrefix, reg), []block.MetadataFilter{
					block.NewLabelShardedMetaFilter(relabelConfig),
					block.NewConsistencyDelayMetaFilter(logger, tbc.consistencyDelay, extprom.WrapRegistererWithPrefix(extpromPrefix, reg)),
					duplicateBlocksFilter,
					ignoreDeletionMarkFilter,
				},
			)
			sy, err = compact.NewMetaSyncer(
				logger,
				reg,
				insBkt,
				cf,
				duplicateBlocksFilter,
				ignoreDeletionMarkFilter,
				stubCounter,
				stubCounter,
			)
			if err != nil {
				return errors.Wrap(err, "create syncer")
			}
		}

		ctx := context.Background()
		level.Info(logger).Log("msg", "syncing blocks metadata")
		if err := sy.SyncMetas(ctx); err != nil {
			return errors.Wrap(err, "sync blocks")
		}

		level.Info(logger).Log("msg", "synced blocks done")

		level.Warn(logger).Log("msg", "GLOBAL COMPACTOR SHOULD __NOT__ BE RUNNING ON THE SAME BUCKET")

		if err := compact.ApplyRetentionPolicyByResolution(ctx, logger, insBkt, sy.Metas(), retentionByResolution, stubCounter); err != nil {
			return errors.Wrap(err, "retention failed")
		}
		return nil
	})
}

func registerBucketUploadBlocks(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command("upload-blocks", "Upload blocks push blocks from the provided path to the object storage.")

	tbc := &bucketUploadBlocksConfig{}
	tbc.registerBucketUploadBlocksFlag(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		if len(tbc.labels) == 0 {
			return errors.New("no external labels configured, uniquely identifying external labels must be configured; see https://thanos.io/tip/thanos/storage.md#external-labels for details.")
		}

		lset, err := parseFlagLabels(tbc.labels)
		if err != nil {
			return errors.Wrap(err, "unable to parse external labels")
		}
		if err := promclient.IsDirAccessible(tbc.path); err != nil {
			return errors.Wrapf(err, "unable to access path '%s'", tbc.path)
		}

		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return errors.Wrap(err, "unable to parse objstore config")
		}

		bkt, err := client.NewBucket(logger, confContentYaml, component.Upload.String())
		if err != nil {
			return errors.Wrap(err, "unable to create bucket")
		}
		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		bkt = objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))

		s := shipper.New(logger, reg, tbc.path, bkt, func() labels.Labels { return lset }, metadata.BucketUploadSource,
			nil, false, metadata.HashFunc(""), shipper.DefaultMetaFilename)

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			n, err := s.Sync(ctx)
			if err != nil {
				return errors.Wrap(err, "unable to sync blocks")
			}
			level.Info(logger).Log("msg", "synced blocks", "uploaded", n)
			return nil
		}, func(error) {
			cancel()
		})

		return nil
	})
}
