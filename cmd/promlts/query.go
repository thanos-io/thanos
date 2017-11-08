package main

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/promlts/pkg/okgroup"
	"github.com/improbable-eng/promlts/pkg/query"
	"github.com/improbable-eng/promlts/pkg/query/api"
	"github.com/improbable-eng/promlts/pkg/store/storepb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

// registerQuery registers a query command.
func registerQuery(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "query node exposing PromQL enabled Query API with data retrieved from multiple store nodes")

	apiAddr := cmd.Flag("api-address", "listen host:port address for the query API").
		Default("0.0.0.0:19099").String()

	queryTimeout := cmd.Flag("query.timeout", "maximum time to process query by query node").
		Default("2m").Duration()

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "maximum number of queries processed concurrently by query node").
		Default("20").Int()

	stores := cmd.Arg("store", "store APIs to get data from").Required().URL()

	m[name] = func(logger log.Logger, metrics *prometheus.Registry) (okgroup.Group, error) {
		return runQuery(logger, metrics, *apiAddr, query.Config{
			QueryTimeout:         *queryTimeout,
			MaxConcurrentQueries: *maxConcurrentQueries,
		}, *stores)
	}
}

// runQuery starts a server that exposes PromQL Query API. It is responsible for querying configured
// store nodes, merging and duplicating the data to satisfy user query.
func runQuery(
	logger log.Logger,
	reg *prometheus.Registry,
	apiAddr string,
	cfg query.Config,
	storesURL *url.URL,
) (
	okgroup.Group, error,
) {
	stores := &storePool{
		logger:   logger,
		discover: storesURL,
		stores:   map[string]*storeInfo{},
	}

	// Set up query API engine.
	queryable := query.NewQueryable(logger, stores.get)
	engine := promql.NewEngine(queryable, cfg.EngineOpts(logger))
	api := v1.NewAPI(engine, queryable, cfg)

	var g okgroup.Group

	// Discover stores and instantiate connections in the background.
	{
		ctx, cancel := context.WithCancel(context.Background())
		tick := time.NewTicker(30 * time.Second)

		g.Add(func() error {
			for {
				if err := stores.update(ctx); err != nil {
					level.Warn(logger).Log("msg", "syncing stores failed", "err", err)
				}
				select {
				case <-tick.C:
				case <-ctx.Done():
					return nil
				}
			}
		}, func(error) {
			cancel()
			tick.Stop()
		})
	}
	// Start query API HTTP server.
	{
		router := route.New().WithPrefix("/api/v1")
		api.Register(router)

		mux := http.NewServeMux()
		registerMetrics(mux, reg)
		registerProfile(mux)
		mux.Handle("/", router)

		l, err := net.Listen("tcp", apiAddr)
		if err != nil {
			return g, errors.Wrapf(err, "listen on address %s", apiAddr)
		}

		g.Add(func() error {
			return errors.Wrap(http.Serve(l, mux), "serve query")
		}, func(error) {
			l.Close()
		})
	}

	level.Info(logger).Log(
		"msg", "starting query node",
		"api-address", apiAddr,
		"store.addresses", strings.Join(cfg.StoreAddresses, ","),
		"query.timeout", cfg.QueryTimeout,
		"query.max-concurrent", cfg.MaxConcurrentQueries,
	)
	return g, nil
}

type storeInfo struct {
	logger log.Logger
	addr   string
	mtx    sync.RWMutex
	conn   *grpc.ClientConn
	labels labels.Labels
	stopc  chan struct{}
}

var _ query.StoreInfo = (*storeInfo)(nil)

func (s *storeInfo) Conn() *grpc.ClientConn {
	return s.conn
}

func (s *storeInfo) Labels() labels.Labels {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.labels
}

func (s *storeInfo) run(interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)

		resp, err := storepb.NewStoreClient(s.conn).Info(ctx, &storepb.InfoRequest{})
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed fetching store info", "err", err)
		} else {
			s.setLabels(resp.Labels)
		}
		cancel()

		select {
		case <-tick.C:
		case <-s.stopc:
			return
		}
	}
}

func (s *storeInfo) setLabels(lset []storepb.Label) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.labels = s.labels[:0]

	for _, l := range lset {
		s.labels = append(s.labels, labels.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	sort.Sort(s.labels)
}

func (s *storeInfo) stop() {
	close(s.stopc)
	s.conn.Close()
}

// storePool is a set of store targets that are discovered through DNS. It holds open connections
// to each store node.
type storePool struct {
	logger   log.Logger
	discover *url.URL
	mtx      sync.RWMutex
	stores   map[string]*storeInfo
}

func (p *storePool) update(ctx context.Context) error {
	addrs, err := discoverAddresses(ctx, p.discover)
	if err != nil {
		return errors.Wrap(err, "discover store addresses")
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	// Add new stores and establish connections.
	for addr := range addrs {
		if _, ok := p.stores[addr]; ok {
			continue
		}
		conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
		if err != nil {
			level.Warn(p.logger).Log("msg", "dialing connection failed; skipping", "store", addr, "err", err)
			continue
		}
		s := &storeInfo{
			logger: log.With(p.logger, "store", addr),
			addr:   addr,
			conn:   conn,
		}
		go s.run(30 * time.Second)

		p.stores[addr] = s
	}
	// Delete stores that no longer exist.
	for addr, s := range p.stores {
		if _, ok := addrs[addr]; !ok {
			s.stop()
			delete(p.stores, addr)
		}
	}
	return nil
}

func (p *storePool) get() []query.StoreInfo {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	res := make([]query.StoreInfo, 0, len(p.stores))
	for _, s := range p.stores {
		res = append(res, s)
	}
	return res
}

func (p *storePool) close() {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, s := range p.stores {
		s.stop()
	}
}

func discoverAddresses(ctx context.Context, target *url.URL) (map[string]struct{}, error) {
	host, port, err := net.SplitHostPort(target.Host)
	if err != nil {
		return nil, errors.Wrap(err, "split host/port")
	}
	var res net.Resolver

	addresses := map[string]struct{}{}

	switch target.Scheme {
	case "dns", "dnsip":
		ips, err := res.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, errors.Wrap(err, "LookupIP")
		}
		for _, ip := range ips {
			addresses[net.JoinHostPort(ip.String(), port)] = struct{}{}
		}
	case "dnssrv":
		_, records, err := res.LookupSRV(ctx, "", "tcp", host)
		if err != nil {
			return nil, errors.Wrap(err, "LookupSRV")
		}
		for _, rec := range records {
			addresses[net.JoinHostPort(rec.Target, strconv.Itoa(int(rec.Port)))] = struct{}{}
		}
	case "dnsaddr":
		names, err := res.LookupAddr(ctx, host)
		if err != nil {
			return nil, errors.Wrap(err, "LookupAddr")
		}
		for _, n := range names {
			addresses[net.JoinHostPort(n, port)] = struct{}{}
		}
	case "tcp":
		addresses[net.JoinHostPort(host, port)] = struct{}{}
	default:
		return nil, errors.Errorf("unsupported discovery scheme %s (one of dnsip, dnssrv, dnsaddr)", target.Scheme)
	}
	return addresses, nil
}
