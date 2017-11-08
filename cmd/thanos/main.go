package main

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/okgroup"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/version"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const defaultClusterAddr = "0.0.0.0:10900"

type setupFunc func(log.Logger, *prometheus.Registry) (okgroup.Group, error)

func main() {
	app := kingpin.New(filepath.Base(os.Args[0]), "A block storage based long-term storage for Prometheus")

	app.Version(version.Print("thanos"))
	app.HelpFlag.Short('h')

	logLevel := app.Flag("log.level", "log filtering level").
		Default("info").Enum("error", "warn", "info", "debug")

	cmds := map[string]setupFunc{}
	registerSidecar(cmds, app, "sidecar")
	registerStore(cmds, app, "store")
	registerQuery(cmds, app, "query")
	registerExample(cmds, app, "example")

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		app.Usage(os.Args[1:])
		os.Exit(2)
	}

	var logger log.Logger
	{
		var lvl level.Option
		switch *logLevel {
		case "error":
			lvl = level.AllowError()
		case "warn":
			lvl = level.AllowWarn()
		case "info":
			lvl = level.AllowInfo()
		case "debug":
			lvl = level.AllowDebug()
		default:
			panic("unexpected log level")
		}
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
		logger = level.NewFilter(logger, lvl)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	}

	metrics := prometheus.NewRegistry()
	metrics.MustRegister(
		version.NewCollector("prometheus"),
		prometheus.NewGoCollector(),
	)

	g, err := cmds[cmd](logger, metrics)
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrap(err, "command failed"))
		os.Exit(1)
	}

	// Listen for termination signals.
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			return interrupt(cancel)
		}, func(error) {
			close(cancel)
		})
	}

	if err := g.Run(); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrap(err, "command run failed"))
		os.Exit(1)
	}

}

func interrupt(cancel <-chan struct{}) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-c:
		return nil
	case <-cancel:
		return errors.New("canceled")
	}
}

func registerProfile(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
}

func registerMetrics(mux *http.ServeMux, g prometheus.Gatherer) {
	mux.Handle("/metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{}))
}

func joinCluster(logger log.Logger, typ cluster.PeerType, bindAddr, advertiseAddr string, peers []string) (*cluster.Peer, error) {
	bindHost, _, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return nil, err
	}
	var advertiseHost string

	if advertiseAddr != "" {
		advertiseHost, _, err = net.SplitHostPort(advertiseAddr)
		if err != nil {
			return nil, err
		}
	}

	if addr, err := cluster.CalculateAdvertiseAddress(bindHost, advertiseHost); err != nil {
		level.Warn(logger).Log("err", "couldn't deduce an advertise address: "+err.Error())
	} else if hasNonlocal(peers) && isUnroutable(addr.String()) {
		level.Warn(logger).Log("err", "this node advertises itself on an unroutable address", "addr", addr.String())
		level.Warn(logger).Log("err", "this node will be unreachable in the cluster")
		level.Warn(logger).Log("err", "provide --cluster.advertise-address as a routable IP address or hostname")
	}

	logger = log.With(logger, "component", "cluster")

	// TODO(fabxc): generate human-readable but random names?
	name, err := ulid.New(uint64(time.Now().Unix()), rand.New(rand.NewSource(0)))
	if err != nil {
		return nil, err
	}
	return cluster.NewPeer(logger, name.String(), typ, bindAddr, advertiseAddr, peers)
}

func hasNonlocal(clusterPeers []string) bool {
	for _, peer := range clusterPeers {
		if host, _, err := net.SplitHostPort(peer); err == nil {
			peer = host
		}
		if ip := net.ParseIP(peer); ip != nil && !ip.IsLoopback() {
			return true
		} else if ip == nil && strings.ToLower(peer) != "localhost" {
			return true
		}
	}
	return false
}

func isUnroutable(addr string) bool {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		addr = host
	}
	if ip := net.ParseIP(addr); ip != nil && (ip.IsUnspecified() || ip.IsLoopback()) {
		return true // typically 0.0.0.0 or localhost
	} else if ip == nil && strings.ToLower(addr) == "localhost" {
		return true
	}
	return false
}
