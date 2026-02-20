// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"strings"
	"time"

	"github.com/alecthomas/units"
	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/compression"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	objstoretracing "github.com/thanos-io/objstore/tracing/opentracing"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/compressutil"
	"github.com/thanos-io/thanos/pkg/exemplars"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/extgrpc/snappy"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/info"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/status"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"github.com/thanos-io/thanos/pkg/tls"
)

const (
	compressionNone   = "none"
	metricNamesFilter = "metric-names-filter"
)

func registerReceive(app *extkingpin.App) {
	cmd := app.Command(component.Receive.String(), "Accept Prometheus remote write API requests and write to local tsdb.")

	conf := &receiveConfig{}
	conf.registerFlag(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, debugLogging bool) error {
		lset, err := parseFlagLabels(conf.labelStrs)
		if err != nil {
			return errors.Wrap(err, "parse labels")
		}

		if !model.UTF8Validation.IsValidLabelName(conf.tenantLabelName) {
			return errors.Errorf("unsupported format for tenant label name, got %s", conf.tenantLabelName)
		}
		if lset.Len() == 0 {
			return errors.New("no external labels configured for receive, uniquely identifying external labels must be configured (ideally with `receive_` prefix); see https://thanos.io/tip/thanos/storage.md#external-labels for details.")
		}

		grpcLogOpts, logFilterMethods, err := logging.ParsegRPCOptions(conf.reqLogConfig)

		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		tsdbOpts := &tsdb.Options{
			MinBlockDuration:               int64(time.Duration(*conf.tsdbMinBlockDuration) / time.Millisecond),
			MaxBlockDuration:               int64(time.Duration(*conf.tsdbMaxBlockDuration) / time.Millisecond),
			RetentionDuration:              int64(time.Duration(*conf.retention) / time.Millisecond),
			OutOfOrderTimeWindow:           int64(time.Duration(*conf.tsdbOutOfOrderTimeWindow) / time.Millisecond),
			MaxBytes:                       int64(conf.tsdbMaxBytes),
			OutOfOrderCapMax:               conf.tsdbOutOfOrderCapMax,
			NoLockfile:                     conf.noLockFile,
			WALCompression:                 compressutil.ParseCompressionType(conf.walCompression, compression.Snappy),
			MaxExemplars:                   conf.tsdbMaxExemplars,
			EnableExemplarStorage:          conf.tsdbMaxExemplars > 0,
			HeadChunksWriteQueueSize:       int(conf.tsdbWriteQueueSize),
			EnableMemorySnapshotOnShutdown: conf.tsdbMemorySnapshotOnShutdown,
		}

		// Are we running in IngestorOnly, RouterOnly or RouterIngestor mode?
		receiveMode := conf.determineMode()

		return runReceive(
			g,
			logger,
			debugLogging,
			reg,
			tracer,
			grpcLogOpts,
			logFilterMethods,
			tsdbOpts,
			lset,
			component.Receive,
			metadata.HashFunc(conf.hashFunc),
			receiveMode,
			conf,
		)
	})
}

func runReceive(
	g *run.Group,
	logger log.Logger,
	debugLogging bool,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	grpcLogOpts []grpc_logging.Option,
	logFilterMethods []string,
	tsdbOpts *tsdb.Options,
	lset labels.Labels,
	comp component.SourceStoreAPI,
	hashFunc metadata.HashFunc,
	receiveMode receive.ReceiverMode,
	conf *receiveConfig,
) error {
	logger = log.With(logger, "component", "receive")

	level.Info(logger).Log("mode", receiveMode, "msg", "running receive")

	multiTSDBOptions := []receive.MultiTSDBOption{
		receive.WithHeadExpandedPostingsCacheSize(conf.headExpandedPostingsCacheSize),
		receive.WithBlockExpandedPostingsCacheSize(conf.compactedBlocksExpandedPostingsCacheSize),
	}
	for _, feature := range *conf.featureList {
		if feature == metricNamesFilter {
			multiTSDBOptions = append(multiTSDBOptions, receive.WithMetricNameFilterEnabled())
			level.Info(logger).Log("msg", "metric name filter feature enabled")
		}
	}

	rwTLSConfig, err := tls.NewServerConfig(log.With(logger, "protocol", "HTTP"), conf.rwServerCert, conf.rwServerKey, conf.rwServerClientCA, conf.rwServerTlsMinVersion)
	if err != nil {
		return err
	}

	dialOpts, err := extgrpc.StoreClientGRPCOpts(
		logger,
		reg,
		tracer,
	)
	if err != nil {
		return err
	}

	// TODO(naman): pass min TLS version from config.
	tlsDialOpts, err := extgrpc.StoreClientTLSCredentials(logger, conf.rwClientSecure, conf.rwClientSkipVerify, conf.rwClientCert, conf.rwClientKey, conf.rwClientServerCA, conf.rwClientServerName, "")
	if err != nil {
		return err
	}
	dialOpts = append(dialOpts, tlsDialOpts)

	if conf.compression != compressionNone {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(conf.compression)))
	}

	if conf.grpcServiceConfig != "" {
		dialOpts = append(dialOpts, grpc.WithDefaultServiceConfig(conf.grpcServiceConfig))
	}

	var bkt objstore.Bucket
	confContentYaml, err := conf.objStoreConfig.Content()
	if err != nil {
		return err
	}

	// Has this thanos receive instance been configured to ingest metrics into a local TSDB?
	enableIngestion := receiveMode == receive.IngestorOnly || receiveMode == receive.RouterIngestor

	upload := len(confContentYaml) > 0
	if enableIngestion {
		if upload {
			// The background shipper continuously scans the data directory and uploads
			// new blocks to object storage service.
			bkt, err = client.NewBucket(logger, confContentYaml, comp.String(), nil)
			if err != nil {
				return err
			}
			bkt = objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))
		} else {
			level.Info(logger).Log("msg", "no supported bucket was configured, uploads will be disabled")
		}
	}

	// Create TSDB for the default tenant.
	if err := createDefautTenantTSDB(logger, conf.dataDir, conf.defaultTenantID); err != nil {
		return errors.Wrapf(err, "create default tenant tsdb in %v", conf.dataDir)
	}

	relabelContentYaml, err := conf.relabelConfigPath.Content()
	if err != nil {
		return errors.Wrap(err, "get content of relabel configuration")
	}
	var relabelConfig []*relabel.Config
	if err := yaml.Unmarshal(relabelContentYaml, &relabelConfig); err != nil {
		return errors.Wrap(err, "parse relabel configuration")
	}

	var cache = storecache.NoopMatchersCache
	if conf.matcherCacheSize > 0 {
		cache, err = storecache.NewMatchersCache(storecache.WithSize(conf.matcherCacheSize), storecache.WithPromRegistry(reg))
		if err != nil {
			return errors.Wrap(err, "failed to create matchers cache")
		}
		multiTSDBOptions = append(multiTSDBOptions, receive.WithMatchersCache(cache))
	}

	multiTSDBOptions = append(multiTSDBOptions, receive.WithUploadConcurrency(conf.uploadConcurrency))

	dbs := receive.NewMultiTSDB(
		conf.dataDir,
		logger,
		reg,
		tsdbOpts,
		lset,
		conf.tenantLabelName,
		bkt,
		conf.allowOutOfOrderUpload,
		conf.skipCorruptedBlocks,
		hashFunc,
		multiTSDBOptions...,
	)
	writer := receive.NewWriter(log.With(logger, "component", "receive-writer"), dbs, &receive.WriterOptions{
		Intern:                   conf.writerInterning,
		TooFarInFutureTimeWindow: int64(time.Duration(*conf.tsdbTooFarInFutureTimeWindow)),
	})

	var limitsConfig *receive.RootLimitsConfig
	if conf.writeLimitsConfig != nil {
		limitsContentYaml, err := conf.writeLimitsConfig.Content()
		if err != nil {
			return errors.Wrap(err, "get content of limit configuration")
		}
		limitsConfig, err = receive.ParseRootLimitConfig(limitsContentYaml)
		if err != nil {
			return errors.Wrap(err, "parse limit configuration")
		}
	}
	limiter, err := receive.NewLimiter(conf.writeLimitsConfig, reg, receiveMode, log.With(logger, "component", "receive-limiter"), conf.limitsConfigReloadTimer)
	if err != nil {
		return errors.Wrap(err, "creating limiter")
	}

	webHandler := receive.NewHandler(log.With(logger, "component", "receive-handler"), &receive.Options{
		Writer:               writer,
		ListenAddress:        conf.rwAddress,
		Registry:             reg,
		Endpoint:             conf.endpoint,
		TenantHeader:         conf.tenantHeader,
		TenantField:          conf.tenantField,
		DefaultTenantID:      conf.defaultTenantID,
		ReplicaHeader:        conf.replicaHeader,
		ReplicationFactor:    conf.replicationFactor,
		RelabelConfigs:       relabelConfig,
		ReceiverMode:         receiveMode,
		Tracer:               tracer,
		TLSConfig:            rwTLSConfig,
		SplitTenantLabelName: conf.splitTenantLabelName,
		DialOpts:             dialOpts,
		ForwardTimeout:       time.Duration(*conf.forwardTimeout),
		MaxBackoff:           time.Duration(*conf.maxBackoff),
		MaxArtificialDelay:   time.Duration(*conf.maxArtificialDelay),
		TSDBStats:            dbs,
		Limiter:              limiter,

		AsyncForwardWorkerCount: conf.asyncForwardWorkerCount,
		ReplicationProtocol:     receive.ReplicationProtocol(conf.replicationProtocol),
		OtlpEnableTargetInfo:    conf.otlpEnableTargetInfo,
		OtlpResourceAttributes:  conf.otlpResourceAttributes,
	})

	grpcProbe := prober.NewGRPC()
	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		grpcProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	// Start all components while we wait for TSDB to open but only load
	// initial config and mark ourselves as ready after it completes.

	// hashringChangedChan signals when TSDB needs to be flushed and updated due to hashring config change.
	hashringChangedChan := make(chan struct{}, 1)

	if enableIngestion {
		// uploadC signals when new blocks should be uploaded.
		uploadC := make(chan struct{}, 1)
		// uploadDone signals when uploading has finished.
		uploadDone := make(chan struct{}, 1)

		level.Debug(logger).Log("msg", "setting up TSDB")
		{
			if err := startTSDBAndUpload(g, logger, reg, dbs, uploadC, hashringChangedChan, upload, uploadDone, statusProber, bkt, receive.HashringAlgorithm(conf.hashringsAlgorithm)); err != nil {
				return err
			}
		}
	}

	level.Debug(logger).Log("msg", "setting up hashring")
	{
		if err := setupHashring(g, logger, reg, conf, hashringChangedChan, webHandler, statusProber, enableIngestion, dbs); err != nil {
			return err
		}
	}

	level.Debug(logger).Log("msg", "setting up HTTP server")
	{
		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(*conf.httpBindAddr),
			httpserver.WithGracePeriod(time.Duration(*conf.httpGracePeriod)),
			httpserver.WithTLSConfig(*conf.httpTLSConfig),
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

	level.Debug(logger).Log("msg", "setting up gRPC server")
	{
		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), conf.grpcConfig.tlsSrvCert, conf.grpcConfig.tlsSrvKey, conf.grpcConfig.tlsSrvClientCA, conf.grpcConfig.tlsMinVersion)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		if conf.lazyRetrievalMaxBufferedResponses <= 0 {
			return errors.New("--receive.lazy-retrieval-max-buffered-responses must be > 0")
		}
		options := []store.ProxyStoreOption{
			store.WithProxyStoreDebugLogging(debugLogging),
			store.WithMatcherCache(cache),
			store.WithoutDedup(),
			store.WithLazyRetrievalMaxBufferedResponsesForProxy(conf.lazyRetrievalMaxBufferedResponses),
		}

		proxy := store.NewProxyStore(
			logger,
			reg,
			dbs.TSDBLocalClients,
			comp,
			labels.Labels{},
			0,
			store.LazyRetrieval,
			options...,
		)
		mts := store.NewLimitedStoreServer(store.NewInstrumentedStoreServer(reg, proxy), reg, conf.storeRateLimits)
		rw := store.ReadWriteTSDBStore{
			StoreServer:          mts,
			WriteableStoreServer: webHandler,
		}

		infoSrv := info.NewInfoServer(
			component.Receive.String(),
			info.WithLabelSetFunc(func() []labelpb.ZLabelSet { return proxy.LabelSet() }),
			info.WithStoreInfoFunc(func() (*infopb.StoreInfo, error) {
				if httpProbe.IsReady() {
					minTime, maxTime := proxy.TimeRange()
					return &infopb.StoreInfo{
						MinTime:                      minTime,
						MaxTime:                      maxTime,
						SupportsSharding:             true,
						SupportsWithoutReplicaLabels: true,
						TsdbInfos:                    proxy.TSDBInfos(),
					}, nil
				}
				return nil, errors.New("Not ready")
			}),
			info.WithExemplarsInfoFunc(),
			info.WithStatusInfoFunc(),
		)

		statusSrv := status.NewServer(
			component.Receive.String(),
			status.WithTSDBStatisticsGetter(
				status.TSDBStatisticsGetterFunc(func(limit int, tenantID string) (map[string]tsdb.Stats, error) {
					if !httpProbe.IsReady() {
						return nil, errors.New("not ready")
					}

					var tenantIDs []string
					if tenantID != "" {
						tenantIDs = append(tenantIDs, tenantID)
					}

					stats := map[string]tsdb.Stats{}
					for _, ts := range dbs.TenantStats(limit, model.MetricNameLabel, tenantIDs...) {
						stats[ts.Tenant] = *ts.Stats
					}

					return stats, nil
				}),
			),
		)

		srv := grpcserver.New(logger, receive.NewUnRegisterer(reg), tracer, grpcLogOpts, logFilterMethods, comp, grpcProbe,
			grpcserver.WithServer(store.RegisterStoreServer(rw, logger)),
			grpcserver.WithServer(store.RegisterWritableStoreServer(rw)),
			grpcserver.WithServer(exemplars.RegisterExemplarsServer(exemplars.NewMultiTSDB(dbs.TSDBExemplars))),
			grpcserver.WithServer(status.RegisterStatusServer(statusSrv)),
			grpcserver.WithServer(info.RegisterInfoServer(infoSrv)),
			grpcserver.WithListen(conf.grpcConfig.bindAddress),
			grpcserver.WithGracePeriod(conf.grpcConfig.gracePeriod),
			grpcserver.WithMaxConnAge(conf.grpcConfig.maxConnectionAge),
			grpcserver.WithTLSConfig(tlsCfg),
		)

		g.Add(
			func() error {
				level.Info(logger).Log("msg", "listening for StoreAPI and WritableStoreAPI gRPC", "address", conf.grpcConfig.bindAddress)
				statusProber.Healthy()
				return srv.ListenAndServe()
			},
			func(err error) {
				statusProber.NotReady(err)
				defer statusProber.NotHealthy(err)

				srv.Shutdown(err)
			},
		)
	}

	level.Debug(logger).Log("msg", "setting up receive HTTP handler")
	{
		g.Add(
			func() error {
				return errors.Wrap(webHandler.Run(), "error starting web server")
			},
			func(err error) {
				webHandler.Close()
			},
		)
	}

	if limitsConfig.AreHeadSeriesLimitsConfigured() {
		level.Info(logger).Log("msg", "setting up periodic (every 15s) meta-monitoring query for limiting cache")
		{
			ctx, cancel := context.WithCancel(context.Background())
			g.Add(func() error {
				return runutil.Repeat(15*time.Second, ctx.Done(), func() error {
					if err := limiter.HeadSeriesLimiter().QueryMetaMonitoring(ctx); err != nil {
						level.Error(logger).Log("msg", "failed to query meta-monitoring", "err", err.Error())
					}
					return nil
				})
			}, func(err error) {
				cancel()
			})
		}
	}

	level.Debug(logger).Log("msg", "setting up periodic tenant pruning")
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			pruneInterval := 2 * time.Duration(tsdbOpts.MaxBlockDuration) * time.Millisecond
			return runutil.Repeat(time.Minute, ctx.Done(), func() error {
				currentTime := time.Now()
				currentTotalMinutes := currentTime.Hour()*60 + currentTime.Minute()
				if currentTotalMinutes%int(pruneInterval.Minutes()) != 0 {
					return nil
				}
				if err := dbs.Prune(ctx); err != nil {
					level.Error(logger).Log("err", err)
				}
				return nil
			})
		}, func(err error) {
			cancel()
		})
	}

	{
		if limiter.CanReload() {
			ctx, cancel := context.WithCancel(context.Background())
			g.Add(func() error {
				level.Debug(logger).Log("msg", "limits config initialized with file watcher.")
				if err := limiter.StartConfigReloader(ctx); err != nil {
					return err
				}
				<-ctx.Done()
				return nil
			}, func(err error) {
				cancel()
			})
		}
	}

	{
		capNProtoWriter := receive.NewCapNProtoWriter(logger, dbs, &receive.CapNProtoWriterOptions{
			TooFarInFutureTimeWindow: int64(time.Duration(*conf.tsdbTooFarInFutureTimeWindow)),
		})
		handler := receive.NewCapNProtoHandler(logger, capNProtoWriter)
		listener, err := net.Listen("tcp", conf.replicationAddr)
		if err != nil {
			return err
		}
		server := receive.NewCapNProtoServer(listener, handler, logger)
		g.Add(func() error {
			return server.ListenAndServe()
		}, func(err error) {
			server.Shutdown()
			if err := listener.Close(); err != nil {
				level.Warn(logger).Log("msg", "Cap'n Proto server did not shut down gracefully", "err", err.Error())
			}
		})
	}

	level.Info(logger).Log("msg", "starting receiver")
	return nil
}

// setupHashring sets up the hashring configuration provided.
// If no hashring is provided, we setup a single node hashring with local endpoint.
func setupHashring(g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	conf *receiveConfig,
	hashringChangedChan chan struct{},
	webHandler *receive.Handler,
	statusProber prober.Probe,
	enableIngestion bool,
	dbs *receive.MultiTSDB,
) error {
	// Note: the hashring configuration watcher
	// is the sender and thus closes the chan.
	// In the single-node case, which has no configuration
	// watcher, we close the chan ourselves.
	updates := make(chan []receive.HashringConfig, 1)
	algorithm := receive.HashringAlgorithm(conf.hashringsAlgorithm)

	// The Hashrings config file path is given initializing config watcher.
	if conf.hashringsFilePath != "" {
		cw, err := receive.NewConfigWatcher(log.With(logger, "component", "config-watcher"), reg, conf.hashringsFilePath, *conf.refreshInterval)
		if err != nil {
			return errors.Wrap(err, "failed to initialize config watcher")
		}

		// Check the hashring configuration on before running the watcher.
		if err := cw.ValidateConfig(); err != nil {
			cw.Stop()
			close(updates)
			return errors.Wrap(err, "failed to validate hashring configuration file")
		}

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return receive.ConfigFromWatcher(ctx, updates, cw)
		}, func(error) {
			cancel()
		})
	} else {
		var (
			cf  []receive.HashringConfig
			err error
		)
		// The Hashrings config file content given initialize configuration from content.
		if len(conf.hashringsFileContent) > 0 {
			cf, err = receive.ParseConfig([]byte(conf.hashringsFileContent))
			if err != nil {
				close(updates)
				return errors.Wrap(err, "failed to validate hashring configuration content")
			}
		}

		cancel := make(chan struct{})
		g.Add(func() error {
			defer close(updates)
			updates <- cf
			<-cancel
			return nil
		}, func(error) {
			close(cancel)
		})
	}

	cancel := make(chan struct{})
	g.Add(func() error {

		if enableIngestion {
			defer close(hashringChangedChan)
		}

		for {
			select {
			case c, ok := <-updates:
				if !ok {
					return nil
				}

				if c == nil {
					webHandler.Hashring(receive.SingleNodeHashring(conf.endpoint))
					level.Info(logger).Log("msg", "Empty hashring config. Set up single node hashring.")
				} else {
					h, err := receive.NewMultiHashring(algorithm, conf.replicationFactor, c, reg)
					if err != nil {
						return errors.Wrap(err, "unable to create new hashring from config")
					}
					webHandler.Hashring(h)
					level.Info(logger).Log("msg", "Set up hashring for the given hashring config.")
				}

				if err := dbs.SetHashringConfig(c); err != nil {
					return errors.Wrap(err, "failed to set hashring config in MultiTSDB")
				}

				// If ingestion is enabled, send a signal to TSDB to flush.
				if enableIngestion {
					hashringChangedChan <- struct{}{}
				} else {
					// If not, just signal we are ready (this is important during first hashring load)
					statusProber.Ready()
				}
			case <-cancel:
				return nil
			}
		}
	}, func(err error) {
		close(cancel)
	},
	)
	return nil
}

// startTSDBAndUpload starts the multi-TSDB and sets up the rungroup to flush the TSDB and reload on hashring change.
// It also upload blocks to object store, if upload is enabled.
func startTSDBAndUpload(g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	dbs *receive.MultiTSDB,
	uploadC chan struct{},
	hashringChangedChan chan struct{},
	upload bool,
	uploadDone chan struct{},
	statusProber prober.Probe,
	bkt objstore.Bucket,
	hashringAlgorithm receive.HashringAlgorithm,
) error {

	log.With(logger, "component", "storage")
	dbUpdatesStarted := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_receive_multi_db_updates_attempted_total",
		Help: "Number of Multi DB attempted reloads with flush and potential upload due to hashring changes",
	})
	dbUpdatesCompleted := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_receive_multi_db_updates_completed_total",
		Help: "Number of Multi DB completed reloads with flush and potential upload due to hashring changes",
	})

	level.Debug(logger).Log("msg", "removing storage lock files if any")
	if err := dbs.RemoveLockFilesIfAny(); err != nil {
		return errors.Wrap(err, "remove storage lock files")
	}

	// TSDBs reload logic, listening on hashring changes.
	cancel := make(chan struct{})
	g.Add(func() error {
		defer close(uploadC)

		// Before quitting, ensure the WAL is flushed and the DBs are closed.
		defer func() {
			level.Info(logger).Log("msg", "shutting down storage")
			if err := dbs.Flush(); err != nil {
				level.Error(logger).Log("err", err, "msg", "failed to flush storage")
			} else {
				level.Info(logger).Log("msg", "storage is flushed successfully")
			}
			if err := dbs.Close(); err != nil {
				level.Error(logger).Log("err", err, "msg", "failed to close storage")
				return
			}
			level.Info(logger).Log("msg", "storage is closed")
		}()

		var initialized bool
		for {
			select {
			case <-cancel:
				return nil
			case _, ok := <-hashringChangedChan:
				if !ok {
					return nil
				}

				// When using Ketama as the hashring algorithm, there is no need to flush the TSDB head.
				// If new receivers were added to the hashring, existing receivers will not need to
				// ingest additional series.
				// If receivers are removed from the hashring, existing receivers will only need
				// to ingest a subset of the series that were assigned to the removed receivers.
				// As a result, changing the hashring produces no churn, hence no need to force
				// head compaction and upload.
				flushHead := !initialized || hashringAlgorithm != receive.AlgorithmKetama
				if flushHead {
					msg := "hashring has changed; server is not ready to receive requests"
					statusProber.NotReady(errors.New(msg))
					level.Info(logger).Log("msg", msg)

					level.Info(logger).Log("msg", "updating storage")
					dbUpdatesStarted.Inc()
					if err := dbs.Flush(); err != nil {
						return errors.Wrap(err, "flushing storage")
					}
					if err := dbs.Open(); err != nil {
						return errors.Wrap(err, "opening storage")
					}
					if upload {
						uploadC <- struct{}{}
						<-uploadDone
					}
					dbUpdatesCompleted.Inc()
					statusProber.Ready()
					level.Info(logger).Log("msg", "storage started, and server is ready to receive requests")
					dbUpdatesCompleted.Inc()
				}
				initialized = true
			}
		}
	}, func(err error) {
		close(cancel)
	})

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
					<-uploadC // Closed by storage routine when it's done.
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

				defer close(uploadDone)

				// Run the uploader in a loop.
				tick := time.NewTicker(30 * time.Second)
				defer tick.Stop()

				for {
					select {
					case <-ctx.Done():
						return nil
					case <-uploadC:
						// Upload on demand.
						if err := upload(ctx); err != nil {
							level.Error(logger).Log("msg", "on demand upload failed", "err", err)
						}
						uploadDone <- struct{}{}
					case <-tick.C:
						if err := upload(ctx); err != nil {
							level.Error(logger).Log("msg", "recurring upload failed", "err", err)
						}
					}
				}
			}, func(error) {
				cancel()
			})
		}
	}

	return nil
}

func createDefautTenantTSDB(logger log.Logger, dataDir, defaultTenantID string) error {
	defaultTenantDataDir := path.Join(dataDir, defaultTenantID)

	if _, err := os.Stat(defaultTenantDataDir); !os.IsNotExist(err) {
		level.Info(logger).Log("msg", "default tenant data dir already present, will not create")
		return nil
	}

	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		level.Info(logger).Log("msg", "no existing storage found, not creating default tenant data dir")
		return nil
	}

	level.Info(logger).Log("msg", "default tenant data dir not found, creating", "defaultTenantID", defaultTenantID)

	if err := os.MkdirAll(defaultTenantDataDir, 0750); err != nil {
		return errors.Wrapf(err, "create default tenant data dir: %v", defaultTenantDataDir)
	}

	return nil
}

type receiveConfig struct {
	httpBindAddr    *string
	httpGracePeriod *model.Duration
	httpTLSConfig   *string

	grpcConfig grpcConfig

	replicationAddr       string
	rwAddress             string
	rwServerCert          string
	rwServerKey           string
	rwServerClientCA      string
	rwClientCert          string
	rwClientKey           string
	rwClientSecure        bool
	rwClientServerCA      string
	rwClientServerName    string
	rwClientSkipVerify    bool
	rwServerTlsMinVersion string

	dataDir   string
	labelStrs []string

	objStoreConfig *extflag.PathOrContent
	retention      *model.Duration

	hashringsFilePath    string
	hashringsFileContent string
	hashringsAlgorithm   string

	refreshInterval     *model.Duration
	endpoint            string
	tenantHeader        string
	tenantField         string
	tenantLabelName     string
	defaultTenantID     string
	replicaHeader       string
	replicationFactor   uint64
	forwardTimeout      *model.Duration
	maxBackoff          *model.Duration
	maxArtificialDelay  *model.Duration
	compression         string
	replicationProtocol string
	grpcServiceConfig   string

	tsdbMinBlockDuration         *model.Duration
	tsdbMaxBlockDuration         *model.Duration
	tsdbTooFarInFutureTimeWindow *model.Duration
	tsdbOutOfOrderTimeWindow     *model.Duration
	tsdbOutOfOrderCapMax         int64
	tsdbAllowOverlappingBlocks   bool
	tsdbMaxExemplars             int64
	tsdbMaxBytes                 units.Base2Bytes
	tsdbWriteQueueSize           int64
	tsdbMemorySnapshotOnShutdown bool
	tsdbEnableNativeHistograms   bool

	walCompression       bool
	noLockFile           bool
	writerInterning      bool
	splitTenantLabelName string

	hashFunc string

	allowOutOfOrderUpload bool
	skipCorruptedBlocks   bool
	uploadConcurrency     int

	reqLogConfig      *extflag.PathOrContent
	relabelConfigPath *extflag.PathOrContent

	writeLimitsConfig       *extflag.PathOrContent
	storeRateLimits         store.SeriesSelectLimits
	limitsConfigReloadTimer time.Duration

	asyncForwardWorkerCount uint

	matcherCacheSize int

	lazyRetrievalMaxBufferedResponses int

	featureList *[]string

	headExpandedPostingsCacheSize            uint64
	compactedBlocksExpandedPostingsCacheSize uint64
	otlpEnableTargetInfo                     bool
	otlpResourceAttributes                   []string
}

func (rc *receiveConfig) registerFlag(cmd extkingpin.FlagClause) {
	rc.httpBindAddr, rc.httpGracePeriod, rc.httpTLSConfig = extkingpin.RegisterHTTPFlags(cmd)
	rc.grpcConfig.registerFlag(cmd)
	rc.storeRateLimits.RegisterFlags(cmd)

	cmd.Flag("remote-write.address", "Address to listen on for remote write requests.").
		Default("0.0.0.0:19291").StringVar(&rc.rwAddress)

	cmd.Flag("remote-write.server-tls-cert", "TLS Certificate for HTTP server, leave blank to disable TLS.").Default("").StringVar(&rc.rwServerCert)

	cmd.Flag("remote-write.server-tls-key", "TLS Key for the HTTP server, leave blank to disable TLS.").Default("").StringVar(&rc.rwServerKey)

	cmd.Flag("remote-write.server-tls-client-ca", "TLS CA to verify clients against. If no client CA is specified, there is no client verification on server side. (tls.NoClientCert)").Default("").StringVar(&rc.rwServerClientCA)

	cmd.Flag("remote-write.server-tls-min-version", "TLS version for the gRPC server, leave blank to default to TLS 1.3, allow values: [\"1.0\", \"1.1\", \"1.2\", \"1.3\"]").Default("1.3").StringVar(&rc.rwServerTlsMinVersion)

	cmd.Flag("remote-write.client-tls-cert", "TLS Certificates to use to identify this client to the server.").Default("").StringVar(&rc.rwClientCert)

	cmd.Flag("remote-write.client-tls-key", "TLS Key for the client's certificate.").Default("").StringVar(&rc.rwClientKey)

	cmd.Flag("remote-write.client-tls-secure", "Use TLS when talking to the other receivers.").Default("false").BoolVar(&rc.rwClientSecure)

	cmd.Flag("remote-write.client-tls-skip-verify", "Disable TLS certificate verification when talking to the other receivers i.e self signed, signed by fake CA.").Default("false").BoolVar(&rc.rwClientSkipVerify)

	cmd.Flag("remote-write.client-tls-ca", "TLS CA Certificates to use to verify servers.").Default("").StringVar(&rc.rwClientServerCA)

	cmd.Flag("remote-write.client-server-name", "Server name to verify the hostname on the returned TLS certificates. See https://tools.ietf.org/html/rfc4366#section-3.1").Default("").StringVar(&rc.rwClientServerName)

	cmd.Flag("tsdb.path", "Data directory of TSDB.").
		Default("./data").StringVar(&rc.dataDir)

	cmd.Flag("label", "External labels to announce. This flag will be removed in the future when handling multiple tsdb instances is added.").PlaceHolder("key=\"value\"").StringsVar(&rc.labelStrs)

	rc.objStoreConfig = extkingpin.RegisterCommonObjStoreFlags(cmd, "", false)

	rc.retention = extkingpin.ModelDuration(cmd.Flag("tsdb.retention", "How long to retain raw samples on local storage. 0d - disables the retention policy (i.e. infinite retention). For more details on how retention is enforced for individual tenants, please refer to the Tenant lifecycle management section in the Receive documentation: https://thanos.io/tip/components/receive.md/#tenant-lifecycle-management").Default("15d"))

	cmd.Flag("receive.hashrings-file", "Path to file that contains the hashring configuration. A watcher is initialized to watch changes and update the hashring dynamically.").PlaceHolder("<path>").StringVar(&rc.hashringsFilePath)

	cmd.Flag("receive.hashrings", "Alternative to 'receive.hashrings-file' flag (lower priority). Content of file that contains the hashring configuration.").PlaceHolder("<content>").StringVar(&rc.hashringsFileContent)

	hashringAlgorithmsHelptext := strings.Join([]string{string(receive.AlgorithmHashmod), string(receive.AlgorithmKetama)}, ", ")
	cmd.Flag("receive.hashrings-algorithm", "The algorithm used when distributing series in the hashrings. Must be one of "+hashringAlgorithmsHelptext+". Will be overwritten by the tenant-specific algorithm in the hashring config.").
		Default(string(receive.AlgorithmHashmod)).
		EnumVar(&rc.hashringsAlgorithm, string(receive.AlgorithmHashmod), string(receive.AlgorithmKetama))

	rc.refreshInterval = extkingpin.ModelDuration(cmd.Flag("receive.hashrings-file-refresh-interval", "Refresh interval to re-read the hashring configuration file. (used as a fallback)").
		Default("5m"))

	cmd.Flag("receive.local-endpoint", "Endpoint of local receive node. Used to identify the local node in the hashring configuration. If it's empty AND hashring configuration was provided, it means that receive will run in RoutingOnly mode.").StringVar(&rc.endpoint)

	cmd.Flag("receive.tenant-header", "HTTP header to determine tenant for write requests.").Default(tenancy.DefaultTenantHeader).StringVar(&rc.tenantHeader)

	rc.maxArtificialDelay = extkingpin.ModelDuration(cmd.Flag("receive.artificial-max-delay", "Maximum artificial delay for the 2nd peer.").Default("0s").Hidden())

	cmd.Flag("receive.tenant-certificate-field", "Use TLS client's certificate field to determine tenant for write requests. Must be one of "+tenancy.CertificateFieldOrganization+", "+tenancy.CertificateFieldOrganizationalUnit+" or "+tenancy.CertificateFieldCommonName+". This setting will cause the receive.tenant-header flag value to be ignored.").Default("").EnumVar(&rc.tenantField, "", tenancy.CertificateFieldOrganization, tenancy.CertificateFieldOrganizationalUnit, tenancy.CertificateFieldCommonName)

	cmd.Flag("receive.default-tenant-id", "Default tenant ID to use when none is provided via a header.").Default(tenancy.DefaultTenant).StringVar(&rc.defaultTenantID)

	cmd.Flag("receive.split-tenant-label-name", "Label name through which the request will be split into multiple tenants. This takes precedence over the HTTP header.").Default("").StringVar(&rc.splitTenantLabelName)

	cmd.Flag("receive.tenant-label-name", "Label name through which the tenant will be announced.").Default(tenancy.DefaultTenantLabel).StringVar(&rc.tenantLabelName)

	cmd.Flag("receive.replica-header", "HTTP header specifying the replica number of a write request.").Default(receive.DefaultReplicaHeader).StringVar(&rc.replicaHeader)

	cmd.Flag("receive.forward.async-workers", "Number of concurrent workers processing forwarding of remote-write requests.").Default("5").UintVar(&rc.asyncForwardWorkerCount)
	compressionOptions := strings.Join([]string{snappy.Name, compressionNone}, ", ")
	cmd.Flag("receive.grpc-compression", "Compression algorithm to use for gRPC requests to other receivers. Must be one of: "+compressionOptions).Default(snappy.Name).EnumVar(&rc.compression, snappy.Name, compressionNone)

	cmd.Flag("receive.replication-factor", "How many times to replicate incoming write requests.").Default("1").Uint64Var(&rc.replicationFactor)

	replicationProtocols := []string{string(receive.ProtobufReplication), string(receive.CapNProtoReplication)}
	cmd.Flag("receive.replication-protocol", "The protocol to use for replicating remote-write requests. One of "+strings.Join(replicationProtocols, ", ")).
		Default(string(receive.ProtobufReplication)).
		EnumVar(&rc.replicationProtocol, replicationProtocols...)

	cmd.Flag("receive.capnproto-address", "Address for the Cap'n Proto server.").Default(fmt.Sprintf("0.0.0.0:%s", receive.DefaultCapNProtoPort)).StringVar(&rc.replicationAddr)

	cmd.Flag("receive.grpc-service-config", "gRPC service configuration file or content in JSON format. See https://github.com/grpc/grpc/blob/master/doc/service_config.md").PlaceHolder("<content>").Default("").StringVar(&rc.grpcServiceConfig)

	rc.forwardTimeout = extkingpin.ModelDuration(cmd.Flag("receive-forward-timeout", "Timeout for each forward request.").Default("5s").Hidden())

	rc.maxBackoff = extkingpin.ModelDuration(cmd.Flag("receive-forward-max-backoff", "Maximum backoff for each forward fan-out request").Default("5s").Hidden())

	rc.relabelConfigPath = extflag.RegisterPathOrContent(cmd, "receive.relabel-config", "YAML file that contains relabeling configuration.", extflag.WithEnvSubstitution())

	rc.tsdbMinBlockDuration = extkingpin.ModelDuration(cmd.Flag("tsdb.min-block-duration", "Min duration for local TSDB blocks").Default("2h").Hidden())

	rc.tsdbMaxBlockDuration = extkingpin.ModelDuration(cmd.Flag("tsdb.max-block-duration", "Max duration for local TSDB blocks").Default("2h").Hidden())

	rc.tsdbTooFarInFutureTimeWindow = extkingpin.ModelDuration(cmd.Flag("tsdb.too-far-in-future.time-window",
		"Configures the allowed time window for ingesting samples too far in the future. Disabled (0s) by default. "+
			"Please note enable this flag will reject samples in the future of receive local NTP time + configured duration due to clock skew in remote write clients.",
	).Default("0s"))

	rc.tsdbOutOfOrderTimeWindow = extkingpin.ModelDuration(cmd.Flag("tsdb.out-of-order.time-window",
		"[EXPERIMENTAL] Configures the allowed time window for ingestion of out-of-order samples. Disabled (0s) by default"+
			"Please note if you enable this option and you use compactor, make sure you have the --compact.enable-vertical-compaction flag enabled, otherwise you might risk compactor halt.",
	).Default("0s"))

	cmd.Flag("tsdb.out-of-order.cap-max",
		"[EXPERIMENTAL] Configures the maximum capacity for out-of-order chunks (in samples). If set to <=0, default value 32 is assumed.",
	).Default("0").Int64Var(&rc.tsdbOutOfOrderCapMax)

	cmd.Flag("tsdb.allow-overlapping-blocks", "Allow overlapping blocks, which in turn enables vertical compaction and vertical query merge. Does not do anything, enabled all the time.").Default("false").BoolVar(&rc.tsdbAllowOverlappingBlocks)

	cmd.Flag("tsdb.max-retention-bytes", "Maximum number of bytes that can be stored for blocks. A unit is required, supported units: B, KB, MB, GB, TB, PB, EB. Ex: \"512MB\". Based on powers-of-2, so 1KB is 1024B.").Default("0").BytesVar(&rc.tsdbMaxBytes)

	cmd.Flag("tsdb.wal-compression", "Compress the tsdb WAL.").Default("true").BoolVar(&rc.walCompression)

	cmd.Flag("tsdb.no-lockfile", "Do not create lockfile in TSDB data directory. In any case, the lockfiles will be deleted on next startup.").Default("false").BoolVar(&rc.noLockFile)

	cmd.Flag("tsdb.head.expanded-postings-cache-size", "[EXPERIMENTAL] If non-zero, enables expanded postings cache for the head block.").Default("0").Uint64Var(&rc.headExpandedPostingsCacheSize)
	cmd.Flag("tsdb.block.expanded-postings-cache-size", "[EXPERIMENTAL] If non-zero, enables expanded postings cache for compacted blocks.").Default("0").Uint64Var(&rc.compactedBlocksExpandedPostingsCacheSize)

	cmd.Flag("tsdb.max-exemplars",
		"Enables support for ingesting exemplars and sets the maximum number of exemplars that will be stored per tenant."+
			" In case the exemplar storage becomes full (number of stored exemplars becomes equal to max-exemplars),"+
			" ingesting a new exemplar will evict the oldest exemplar from storage. 0 (or less) value of this flag disables exemplars storage.").
		Default("0").Int64Var(&rc.tsdbMaxExemplars)

	cmd.Flag("tsdb.write-queue-size",
		"[EXPERIMENTAL] Enables configuring the size of the chunk write queue used in the head chunks mapper. "+
			"A queue size of zero (default) disables this feature entirely.").
		Default("0").Hidden().Int64Var(&rc.tsdbWriteQueueSize)

	cmd.Flag("tsdb.memory-snapshot-on-shutdown",
		"[EXPERIMENTAL] Enables feature to snapshot in-memory chunks on shutdown for faster restarts.").
		Default("false").Hidden().BoolVar(&rc.tsdbMemorySnapshotOnShutdown)

	cmd.Flag("tsdb.enable-native-histograms",
		"(Deprecated) Enables the ingestion of native histograms. This flag is a no-op now and will be removed in the future. Native histogram ingestion is always enabled.").
		Default("true").BoolVar(&rc.tsdbEnableNativeHistograms)

	cmd.Flag("writer.intern",
		"[EXPERIMENTAL] Enables string interning in receive writer, for more optimized memory usage.").
		Default("false").Hidden().BoolVar(&rc.writerInterning)

	cmd.Flag("hash-func", "Specify which hash function to use when calculating the hashes of produced files. If no function has been specified, it does not happen. This permits avoiding downloading some files twice albeit at some performance cost. Possible values are: \"\", \"SHA256\".").
		Default("").EnumVar(&rc.hashFunc, "SHA256", "")

	cmd.Flag("shipper.allow-out-of-order-uploads",
		"If true, shipper will skip failed block uploads in the given iteration and retry later. This means that some newer blocks might be uploaded sooner than older blocks."+
			"This can trigger compaction without those blocks and as a result will create an overlap situation. Set it to true if you have vertical compaction enabled and wish to upload blocks as soon as possible without caring"+
			"about order.").
		Default("false").Hidden().BoolVar(&rc.allowOutOfOrderUpload)

	cmd.Flag("shipper.skip-corrupted-blocks",
		"If true, shipper will skip corrupted blocks in the given iteration and retry later. This means that some newer blocks might be uploaded sooner than older blocks."+
			"This can trigger compaction without those blocks and as a result will create an overlap situation. Set it to true if you have vertical compaction enabled and wish to upload blocks as soon as possible without caring"+
			"about order.").
		Default("false").Hidden().BoolVar(&rc.skipCorruptedBlocks)

	cmd.Flag("shipper.upload-concurrency", "Number of goroutines to use when uploading block files to object storage.").Default("0").IntVar(&rc.uploadConcurrency)

	cmd.Flag("matcher-cache-size", "Max number of cached matchers items. Using 0 disables caching.").Default("0").IntVar(&rc.matcherCacheSize)

	rc.reqLogConfig = extkingpin.RegisterRequestLoggingFlags(cmd)

	rc.writeLimitsConfig = extflag.RegisterPathOrContent(cmd, "receive.limits-config", "YAML file that contains limit configuration.", extflag.WithEnvSubstitution(), extflag.WithHidden())
	cmd.Flag("receive.limits-config-reload-timer", "Minimum amount of time to pass for the limit configuration to be reloaded. Helps to avoid excessive reloads.").
		Default("1s").Hidden().DurationVar(&rc.limitsConfigReloadTimer)

	cmd.Flag("receive.otlp-enable-target-info", "Enables target information in OTLP metrics ingested by Receive. If enabled, it converts the resource to the target info metric").Default("true").BoolVar(&rc.otlpEnableTargetInfo)
	cmd.Flag("receive.otlp-promote-resource-attributes", "(Repeatable) Resource attributes to include in OTLP metrics ingested by Receive.").Default("").StringsVar(&rc.otlpResourceAttributes)

	rc.featureList = cmd.Flag("enable-feature", "Comma separated experimental feature names to enable. The current list of features is "+metricNamesFilter+".").Default("").Strings()

	cmd.Flag("receive.lazy-retrieval-max-buffered-responses", "The lazy retrieval strategy can buffer up to this number of responses. This is to limit the memory usage. This flag takes effect only when the lazy retrieval strategy is enabled.").
		Default("20").IntVar(&rc.lazyRetrievalMaxBufferedResponses)
}

// determineMode returns the ReceiverMode that this receiver is configured to run in.
// This is used to configure this Receiver's forwarding and ingesting behavior at runtime.
func (rc *receiveConfig) determineMode() receive.ReceiverMode {
	// Has the user provided some kind of hashring configuration?
	hashringSpecified := rc.hashringsFileContent != "" || rc.hashringsFilePath != ""
	// Has the user specified the --receive.local-endpoint flag?
	localEndpointSpecified := rc.endpoint != ""

	switch {
	case hashringSpecified && localEndpointSpecified:
		return receive.RouterIngestor
	case hashringSpecified && !localEndpointSpecified:
		// Be careful - if the hashring contains an address that routes to itself and does not specify a local
		// endpoint - you've just created an infinite loop / fork bomb :)
		return receive.RouterOnly
	default:
		// hashring configuration has not been provided so we ingest all metrics locally.
		return receive.IngestorOnly
	}
}
