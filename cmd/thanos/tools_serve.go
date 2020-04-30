// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tls"
	"google.golang.org/grpc"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func registerServe(m map[string]setupFunc, app *kingpin.CmdClause, pre string) {
	cmd := app.Command("serve", "Serving Utilities")

	pre += " serve"
	registerServeStoreAPI(m, cmd, pre)
}

func registerServeStoreAPI(m map[string]setupFunc, app *kingpin.CmdClause, pre string) {
	cmd := app.Command("storeapi", `Utilities allowing to serve certain files as StoreAPI
Example usage:

thanos tools serve storeapi --json=<path-to-json-file1> --json=<path-to-json-file2> --grpc.listen-address=localhost:1234

This will run StoreAPI serve that will serve the data from JSON-serialized stream of SeriesResponses proto. This is compatible 
with what grpcurl returns via our script in '/scripts/insecure_grpcurl_series.sh'

Once this is running you can point querier to it e.g:

thanos query --http.listen-address=localhost:9090 --store=localhost:1234
`)

	bindAddr, gracePeriod, grpcTLSSrvCert, grpcTLSSrvKey, grpcTLSSrvClientCA := regGRPCFlags(cmd)
	externalLabels := cmd.Flag("label", "Labels to be applied to all exposed metrics (repeated). Similar to external labels for Prometheus, used to identify this Store API as unique source.").
		PlaceHolder("<name>=\"<value>\"").Strings()
	jsonFilenames := cmd.Flag("json", "Glob or set of globs matching JSON files with StoreAPI streamed SeriesResponse format.").Required().ExistingFiles()
	// TODO(bwplotka): Add other formats like CSV someday.

	m[pre+" storeapi"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) (err error) {
		comp := component.Debug
		probe := prober.NewGRPC()

		extLset, err := parseFlagLabels(*externalLabels)
		if err != nil {
			return errors.Wrap(err, "parse labels")
		}

		if len(*jsonFilenames) == 0 {
			return errors.New("No JSON file matched.")
		}

		var stores []storepb.StoreServer
		defer func() {
			if err != nil {
				for _, s := range stores {
					if c, ok := s.(io.Closer); ok {
						runutil.CloseWithErrCapture(&err, c, "close json debug store")
					}
				}
			}
		}()

		for _, fn := range *jsonFilenames {
			s, err := store.NewLocalStoreFromJSONMmappableFile(logger, comp, extLset, fn, store.ScanGRPCCurlProtoStreamMessages)
			if err != nil {
				return errors.Wrapf(err, "create local json store for file %s", fn)
			}
			stores = append(stores, s)
		}

		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), *grpcTLSSrvCert, *grpcTLSSrvKey, *grpcTLSSrvClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		// Proxy and each file server is a separate gRPC server.
		// The recommended way to connect many serves is to start them on local unix socket.
		// Let's start them up and then start proxy that will have access to all of them.
		sockDir, err := ioutil.TempDir(os.TempDir(), "thanos-tools-serve-storeapi")
		if err != nil {
			return err
		}

		clients := make([]store.Client, 0, len(stores))
		var clientsWg sync.WaitGroup
		for i, s := range stores {
			clientsWg.Add(1)
			addr := fmt.Sprintf("%d.sock", i)
			logger = log.With(logger, "addr", addr)
			srv := grpcserver.New(logger, extprom.WrapRegistererWith(prometheus.Labels{"addr": addr}, reg), nil, comp, probe, s,
				grpcserver.WithListen(filepath.Join(sockDir, addr)),
				grpcserver.WithNetwork("unix"),
				grpcserver.WithGracePeriod(time.Duration(*gracePeriod)),
			)
			g.Add(func() error {
				return srv.ListenAndServe()
			}, func(err error) {
				level.Info(logger).Log("msg", "closing")
				srv.Shutdown(err)
				if c, ok := s.(io.Closer); ok {
					runutil.CloseWithErrCapture(&err, c, "close json debug store")
				}
				level.Info(logger).Log("msg", "closed")
				clientsWg.Done()
			})
			clients = append(clients, newStoreOnUnixSocket(logger, filepath.Join(sockDir, addr)))
		}

		srv := grpcserver.New(
			logger,
			extprom.WrapRegistererWith(prometheus.Labels{"addr": *bindAddr}, reg),
			nil,
			comp,
			probe,
			store.NewProxyStore(
				logger,
				nil,
				func() []store.Client { return clients },
				comp,
				nil,
				0,
			),
			grpcserver.WithListen(*bindAddr),
			grpcserver.WithGracePeriod(time.Duration(*gracePeriod)),
			grpcserver.WithTLSConfig(tlsCfg),
		)
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			level.Info(logger).Log("msg", "loading files")
			wg := sync.WaitGroup{}
			for _, c := range clients {
				wg.Add(1)
				go func(c *immutableStore) {
					c.Init(ctx)
					wg.Done()
				}(c.(*immutableStore))
			}
			wg.Wait()

			level.Info(logger).Log("msg", "files loaded; starting server")
			probe.Healthy()
			return srv.ListenAndServe()
		}, func(err error) {
			probe.NotReady(err)
			defer probe.NotHealthy(err)

			level.Info(logger).Log("msg", "closing proxy server")
			cancel()
			srv.Shutdown(err)

			level.Info(logger).Log("msg", "closed; waiting for file servers")
			clientsWg.Wait()
			level.Info(logger).Log("msg", "all file servers closed; removing unix socket")
			if err := os.RemoveAll(sockDir); err != nil {
				level.Error(logger).Log("msg", "failed to remove unix socket", "err", err)
			}
		})
		return nil
	}
}

type immutableStore struct {
	storepb.StoreClient

	logger     log.Logger
	cc         *grpc.ClientConn
	addr       string
	mint, maxt int64
	lbls       []storepb.LabelSet
}

func unixSockerDialer(_ context.Context, addr string) (net.Conn, error) {
	unixAddr, err := net.ResolveUnixAddr("unix", addr)
	if err != nil {
		return nil, err
	}
	return net.DialUnix("unix", nil, unixAddr)
}

func newStoreOnUnixSocket(logger log.Logger, addr string) store.Client {
	return &immutableStore{
		logger: logger,
		addr:   addr,
	}
}

func (l *immutableStore) Init(ctx context.Context) {
	var backoff bool
	var err error
	for ctx.Err() == nil {
		if backoff {
			select {
			case <-ctx.Done():
			case <-time.After(1 * time.Second):
			}
		}

		if l.cc != nil {
			_ = l.cc.Close()
		}
		l.cc, err = grpc.DialContext(ctx, l.addr, grpc.WithInsecure(), grpc.WithContextDialer(unixSockerDialer))
		if err != nil {
			level.Warn(l.logger).Log("err", err, "addr", l.addr)
			backoff = true
			continue
		}

		l.StoreClient = storepb.NewStoreClient(l.cc)

		resp, err := l.StoreClient.Info(ctx, &storepb.InfoRequest{})
		if err != nil {
			level.Warn(l.logger).Log("err", err, "addr", l.addr)
			backoff = true
			continue
		}

		l.mint = resp.MinTime
		l.maxt = resp.MaxTime
		l.lbls = resp.LabelSets
		return
	}

}
func (l *immutableStore) LabelSets() []storepb.LabelSet {
	return l.lbls
}

// Minimum and maximum time range of data in the store.
func (l *immutableStore) TimeRange() (mint int64, maxt int64) {
	return l.mint, l.maxt
}

func (l *immutableStore) String() string { return l.addr }
func (l *immutableStore) Addr() string   { return l.addr }

func (l *immutableStore) Close() error {
	if l.cc == nil {
		return nil
	}
	return l.cc.Close()
}
