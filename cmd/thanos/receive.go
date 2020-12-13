// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tags"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/logging"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/tls"
)

func registerReceive(app *extkingpin.App) {
	cmd := app.Command(component.Receive.String(), "Accept Prometheus remote write API requests and write to local tsdb.")

	httpBindAddr, httpGracePeriod := extkingpin.RegisterHTTPFlags(cmd)
	grpcBindAddr, grpcGracePeriod, grpcCert, grpcKey, grpcClientCA := extkingpin.RegisterGRPCFlags(cmd)

	dataDir := cmd.Flag("tsdb.path", "Data directory of TSDB.").
		Default("./data").String()

	labelStrs := cmd.Flag("label", "External labels to announce. This flag will be removed in the future when handling multiple tsdb instances is added.").PlaceHolder("key=\"value\"").Strings()

	objStoreConfig := extkingpin.RegisterCommonObjStoreFlags(cmd, "", false)

	retention := extkingpin.ModelDuration(cmd.Flag("tsdb.retention", "How long to retain raw samples on local storage. 0d - disables this retention.").Default("15d"))

	defaultTenantID := cmd.Flag("receive.default-tenant-id", "Default tenant ID to use when none is provided via a header.").Default(receive.DefaultTenant).String()

	tenantLabelName := cmd.Flag("receive.tenant-label-name", "Label name through which the tenant will be announced.").Default(receive.DefaultTenantLabel).String()

	tsdbMinBlockDuration := extkingpin.ModelDuration(cmd.Flag("tsdb.min-block-duration", "Min duration for local TSDB blocks").Default("2h").Hidden())
	tsdbMaxBlockDuration := extkingpin.ModelDuration(cmd.Flag("tsdb.max-block-duration", "Max duration for local TSDB blocks").Default("2h").Hidden())
	tsdbAllowOverlappingBlocks := cmd.Flag("tsdb.allow-overlapping-blocks", "Allow overlapping blocks, which in turn enables vertical compaction and vertical query merge.").Default("false").Bool()
	walCompression := cmd.Flag("tsdb.wal-compression", "Compress the tsdb WAL.").Default("true").Bool()
	noLockFile := cmd.Flag("tsdb.no-lockfile", "Do not create lockfile in TSDB data directory. In any case, the lockfiles will be deleted on next startup.").Default("false").Bool()

	hashFunc := cmd.Flag("hash-func", "Specify which hash function to use when calculating the hashes of produced files. If no function has been specified, it does not happen. This permits avoiding downloading some files twice albeit at some performance cost. Possible values are: \"\", \"SHA256\".").
		Default("").Enum("SHA256", "")

	ignoreBlockSize := cmd.Flag("shipper.ignore-unequal-block-size", "If true receive will not require min and max block size flags to be set to the same value. Only use this if you want to keep long retention and compaction enabled, as in the worst case it can result in ~2h data loss for your Thanos bucket storage.").Default("false").Hidden().Bool()
	allowOutOfOrderUpload := cmd.Flag("shipper.allow-out-of-order-uploads",
		"If true, shipper will skip failed block uploads in the given iteration and retry later. This means that some newer blocks might be uploaded sooner than older blocks."+
			"This can trigger compaction without those blocks and as a result will create an overlap situation. Set it to true if you have vertical compaction enabled and wish to upload blocks as soon as possible without caring"+
			"about order.").
		Default("false").Hidden().Bool()

	reqLogConfig := extkingpin.RegisterRequestLoggingFlags(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		lset, err := parseFlagLabels(*labelStrs)
		if err != nil {
			return errors.Wrap(err, "parse labels")
		}

		tagOpts, grpcLogOpts, err := logging.ParsegRPCOptions("", reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		tsdbOpts := &tsdb.Options{
			MinBlockDuration:       int64(time.Duration(*tsdbMinBlockDuration) / time.Millisecond),
			MaxBlockDuration:       int64(time.Duration(*tsdbMaxBlockDuration) / time.Millisecond),
			RetentionDuration:      int64(time.Duration(*retention) / time.Millisecond),
			NoLockfile:             *noLockFile,
			WALCompression:         *walCompression,
			AllowOverlappingBlocks: *tsdbAllowOverlappingBlocks,
		}

		return runReceive(
			g,
			logger,
			reg,
			tracer,
			grpcLogOpts, tagOpts,
			*grpcBindAddr,
			time.Duration(*grpcGracePeriod),
			*grpcCert,
			*grpcKey,
			*grpcClientCA,
			*httpBindAddr,
			time.Duration(*httpGracePeriod),
			*dataDir,
			objStoreConfig,
			tsdbOpts,
			*ignoreBlockSize,
			lset,
			*defaultTenantID,
			*tenantLabelName,
			*allowOutOfOrderUpload,
			component.Receive,
			metadata.HashFunc(*hashFunc),
		)
	})
}

func runReceive(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	grpcLogOpts []grpc_logging.Option,
	tagOpts []tags.Option,
	grpcBindAddr string,
	grpcGracePeriod time.Duration,
	grpcCert string,
	grpcKey string,
	grpcClientCA string,
	httpBindAddr string,
	httpGracePeriod time.Duration,
	dataDir string,
	objStoreConfig *extflag.PathOrContent,
	tsdbOpts *tsdb.Options,
	ignoreBlockSize bool,
	lset labels.Labels,
	defaultTenantID string,
	tenantLabelName string,
	allowOutOfOrderUpload bool,
	comp component.SourceStoreAPI,
	hashFunc metadata.HashFunc,
) error {
	logger = log.With(logger, "component", "receive")
	level.Warn(logger).Log("msg", "setting up receive")

	var bkt objstore.Bucket
	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}
	upload := len(confContentYaml) > 0
	if upload {
		if tsdbOpts.MinBlockDuration != tsdbOpts.MaxBlockDuration {
			if !ignoreBlockSize {
				return errors.Errorf("found that TSDB Max time is %d and Min time is %d. "+
					"Compaction needs to be disabled (tsdb.min-block-duration = tsdb.max-block-duration)", tsdbOpts.MaxBlockDuration, tsdbOpts.MinBlockDuration)
			}
			level.Warn(logger).Log("msg", "flag to ignore min/max block duration flags differing is being used. If the upload of a 2h block fails and a tsdb compaction happens that block may be missing from your Thanos bucket storage.")
		}
		// The background shipper continuously scans the data directory and uploads
		// new blocks to object storage service.
		bkt, err = client.NewBucket(logger, confContentYaml, reg, comp.String())
		if err != nil {
			return err
		}
	} else {
		level.Info(logger).Log("msg", "no supported bucket was configured, uploads will be disabled")
	}

	// TODO(brancz): remove after a couple of versions
	// Migrate non-multi-tsdb capable storage to multi-tsdb disk layout.
	if err := migrateLegacyStorage(logger, dataDir, defaultTenantID); err != nil {
		return errors.Wrapf(err, "migrate legacy storage in %v to default tenant %v", dataDir, defaultTenantID)
	}

	dbs := receive.NewMultiTSDB(
		dataDir,
		logger,
		reg,
		tsdbOpts,
		lset,
		tenantLabelName,
		bkt,
		allowOutOfOrderUpload,
		hashFunc,
	)
	writer := receive.NewWriter(log.With(logger, "component", "receive-writer"), dbs)
	webHandler := receive.NewHandler(writer, reg, defaultTenantID, tracer, log.With(logger, "component", "receive-handler"))
	grpcProbe := prober.NewGRPC()
	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		grpcProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	// Start all components while we wait for TSDB to open but only load
	// initial config and mark ourselves as ready after it completed.
	level.Debug(logger).Log("msg", "setting up tsdb")
	{
		log.With(logger, "component", "storage")

		level.Debug(logger).Log("msg", "removing storage lock files if any")
		if err := dbs.RemoveLockFilesIfAny(); err != nil {
			return errors.Wrap(err, "remove storage lock files")
		}
	}

	level.Debug(logger).Log("msg", "setting up http server")
	{
		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(httpBindAddr),
			httpserver.WithGracePeriod(httpGracePeriod),
		)
		g.Add(func() error {
			statusProber.Healthy()
			return srv.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			defer statusProber.NotHealthy(err)
			srv.Shutdown(err)
		})
	}

	level.Debug(logger).Log("msg", "setting up grpc server")
	{
		var s *grpcserver.Server
		startGRPC := make(chan struct{})
		g.Add(func() error {
			defer close(startGRPC)

			tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), grpcCert, grpcKey, grpcClientCA)
			if err != nil {
				return errors.Wrap(err, "setup gRPC server")
			}

			rw := store.ReadWriteTSDBStore{
				StoreServer: store.NewMultiTSDBStore(
					logger,
					reg,
					comp,
					dbs.TSDBStores,
				),
				WriteableStoreServer: webHandler,
			}

			s = grpcserver.New(logger, &receive.UnRegisterer{Registerer: reg}, tracer, grpcLogOpts, tagOpts, comp, grpcProbe,
				grpcserver.WithServer(store.RegisterStoreServer(rw)),
				grpcserver.WithServer(store.RegisterWritableStoreServer(rw)),
				grpcserver.WithListen(grpcBindAddr),
				grpcserver.WithGracePeriod(grpcGracePeriod),
				grpcserver.WithTLSConfig(tlsCfg),
			)

			level.Info(logger).Log("msg", "listening for StoreAPI and WritableStoreAPI gRPC", "address", grpcBindAddr)
			statusProber.Ready()
			if err := s.ListenAndServe(); err != nil {
				return errors.Wrap(err, "serve gRPC")
			}
			return nil
		}, func(err error) {
			s.Shutdown(err)
		})
		// We need to be able to start and stop the gRPC server
		// whenever the DB changes, thus it needs its own run group.
	}

	if upload {
		logger := log.With(logger, "component", "uploader")
		upload := func(ctx context.Context) error {
			level.Debug(logger).Log("msg", "upload phase starting")
			start := time.Now()

			uploaded, err := dbs.Sync(ctx)
			if err != nil {
				level.Warn(logger).Log("msg", "upload failed", "elapsed", time.Since(start), "err", err)
				return err
			}
			level.Debug(logger).Log("msg", "upload phase done", "uploaded", uploaded, "elapsed", time.Since(start))
			return nil
		}
		{
			level.Info(logger).Log("msg", "upload enabled, starting initial sync")
			if err := upload(context.Background()); err != nil {
				return errors.Wrap(err, "initial upload failed")
			}
			level.Info(logger).Log("msg", "initial sync done")
		}
		{
			ctx, cancel := context.WithCancel(context.Background())
			g.Add(func() error {
				// Ensure we clean up everything properly.
				defer func() {
					runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
				}()

				// Before quitting, ensure all blocks are uploaded.
				defer func() {
					level.Info(logger).Log("msg", "uploading the final cut block before exiting")
					ctx, cancel := context.WithCancel(context.Background())
					uploaded, err := dbs.Sync(ctx)
					if err != nil {
						cancel()
						level.Error(logger).Log("msg", "the final upload failed", "err", err)
						return
					}
					cancel()
					level.Info(logger).Log("msg", "the final cut block was uploaded", "uploaded", uploaded)
				}()

				// Run the uploader in a loop.
				tick := time.NewTicker(30 * time.Second)
				defer tick.Stop()

				for {
					select {
					case <-ctx.Done():
						return nil
					case <-tick.C:
						if err := upload(ctx); err != nil {
							level.Warn(logger).Log("msg", "recurring upload failed", "err", err)
						}
					}
				}
			}, func(error) {
				cancel()
			})
		}
	}

	level.Info(logger).Log("msg", "starting receiver")
	return nil
}

func migrateLegacyStorage(logger log.Logger, dataDir, defaultTenantID string) error {
	defaultTenantDataDir := path.Join(dataDir, defaultTenantID)

	if _, err := os.Stat(defaultTenantDataDir); !os.IsNotExist(err) {
		level.Info(logger).Log("msg", "default tenant data dir already present, not attempting to migrate storage")
		return nil
	}

	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		level.Info(logger).Log("msg", "no existing storage found, no data migration attempted")
		return nil
	}

	level.Info(logger).Log("msg", "found legacy storage, migrating to multi-tsdb layout with default tenant", "defaultTenantID", defaultTenantID)

	files, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return errors.Wrapf(err, "read legacy data dir: %v", dataDir)
	}

	if err := os.MkdirAll(defaultTenantDataDir, 0777); err != nil {
		return errors.Wrapf(err, "create default tenant data dir: %v", defaultTenantDataDir)
	}

	for _, f := range files {
		from := path.Join(dataDir, f.Name())
		to := path.Join(defaultTenantDataDir, f.Name())
		if err := os.Rename(from, to); err != nil {
			return errors.Wrapf(err, "migrate file from %v to %v", from, to)
		}
	}

	return nil
}
