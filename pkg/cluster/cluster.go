package cluster

import (
	"context"
	"io/ioutil"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Peer is a single peer in a gossip cluster.
type Peer struct {
	logger   log.Logger
	mlistMtx sync.RWMutex
	mlist    *memberlist.Memberlist
	stopc    chan struct{}

	cfg        *memberlist.Config
	addr       net.IP
	knownPeers []string

	data                 *data
	gossipMsgsReceived   prometheus.Counter
	gossipClusterMembers prometheus.Gauge
}

const (
	DefaultPushPullInterval = 5 * time.Second
	DefaultGossipInterval   = 5 * time.Second
)

// PeerType describes a peer's role in the cluster.
type PeerType string

// Constants holding valid PeerType values.
const (
	// PeerTypeStore is for peers that implements StoreAPI and are used for browsing historical data.
	PeerTypeStore = "store"
	// PeerTypeSource is for peers that implements StoreAPI and are used for scraping data. They tend to
	// have data accessible only for short period.
	PeerTypeSource = "source"

	// PeerTypeQuery is for peers that implements QueryAPI and are used for querying the metrics.
	PeerTypeQuery = "query"
)

// PeerState contains state for the peer.
type PeerState struct {
	Type    PeerType
	APIAddr string

	Metadata PeerMetadata
}

// PeerMetadata are the information that can change in runtime of the peer.
type PeerMetadata struct {
	Labels []storepb.Label

	// MinTime indicates the minTime of the oldest block available from this peer.
	MinTime int64
	// MaxTime indicates the maxTime of the youngest block available from this peer.
	MaxTime int64
}

// New returns "alone" peer that is ready to join.
func New(
	l log.Logger,
	reg *prometheus.Registry,
	bindAddr string,
	advertiseAddr string,
	knownPeers []string,
	waitIfEmpty bool,
	pushPullInterval time.Duration,
	gossipInterval time.Duration,
) (*Peer, error) {
	bindHost, bindPortStr, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return nil, err
	}
	bindPort, err := strconv.Atoi(bindPortStr)
	if err != nil {
		return nil, errors.Wrap(err, "invalid listen address")
	}
	var advertiseHost string
	var advertisePort int

	if advertiseAddr != "" {
		var advertisePortStr string
		advertiseHost, advertisePortStr, err = net.SplitHostPort(advertiseAddr)
		if err != nil {
			return nil, errors.Wrap(err, "invalid advertise address")
		}
		advertisePort, err = strconv.Atoi(advertisePortStr)
		if err != nil {
			return nil, errors.Wrap(err, "invalid advertise address, wrong port")
		}
	} else if bindHost == "" {
		return nil, errors.New("advertise address needs to be specified if gRPC address is in form of ':<port>'")
	}

	resolvedPeers, err := resolvePeers(context.Background(), knownPeers, advertiseAddr, net.Resolver{}, waitIfEmpty)
	if err != nil {
		return nil, errors.Wrap(err, "resolve peers")
	}
	level.Debug(l).Log("msg", "resolved peers to following addresses", "peers", strings.Join(resolvedPeers, ","))

	// Initial validation of user-specified advertise address.
	addr, err := calculateAdvertiseAddress(bindHost, advertiseHost)
	if err != nil {
		level.Warn(l).Log("err", "couldn't deduce an advertise address: "+err.Error())
	} else if hasNonlocal(resolvedPeers) && isUnroutable(addr.String()) {
		level.Warn(l).Log("err", "this node advertises itself on an unroutable address", "addr", addr.String())
		level.Warn(l).Log("err", "this node will be unreachable in the cluster")
		level.Warn(l).Log("err", "provide --cluster.advertise-address as a routable IP address or hostname")
	}

	l = log.With(l, "component", "cluster")

	// TODO(fabxc): generate human-readable but random names?
	name, err := ulid.New(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano())))
	if err != nil {
		return nil, err
	}

	cfg := memberlist.DefaultLANConfig()
	cfg.Name = name.String()
	cfg.BindAddr = bindHost
	cfg.BindPort = bindPort
	cfg.GossipInterval = gossipInterval
	cfg.PushPullInterval = pushPullInterval
	cfg.LogOutput = ioutil.Discard
	if advertiseAddr != "" {
		cfg.AdvertiseAddr = advertiseHost
		cfg.AdvertisePort = advertisePort
	}

	gossipMsgsReceived := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_gossip_messages_received_total",
		Help: "Total gossip NotifyMsg calls.",
	})
	gossipClusterMembers := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_cluster_members",
		Help: "Number indicating current number of members in cluster.",
	})

	reg.MustRegister(gossipMsgsReceived)
	reg.MustRegister(gossipClusterMembers)

	return &Peer{
		logger:               l,
		addr:                 addr,
		knownPeers:           knownPeers,
		cfg:                  cfg,
		gossipMsgsReceived:   gossipMsgsReceived,
		gossipClusterMembers: gossipClusterMembers,
		stopc:                make(chan struct{}),
		data:                 &data{data: map[string]PeerState{}},
	}, nil
}

// Join joins to the memberlist gossip cluster using knownPeers and initialState.
func (p *Peer) Join(initialState PeerState) error {
	if p.hasJoined() {
		return errors.New("peer already joined. Close it first to rejoin.")
	}

	var ml *memberlist.Memberlist
	d := newDelegate(p.logger, ml.NumMembers, p.data, p.gossipMsgsReceived, p.gossipClusterMembers)
	p.cfg.Delegate = d
	p.cfg.Events = d

	// If the API listens on 0.0.0.0, deduce it to the advertise IP.
	if initialState.APIAddr != "" {
		apiHost, apiPort, err := net.SplitHostPort(initialState.APIAddr)
		if err != nil {
			return errors.Wrap(err, "invalid API address")
		}
		if apiHost == "0.0.0.0" {
			initialState.APIAddr = net.JoinHostPort(p.addr.String(), apiPort)
		}
	}

	ml, err := memberlist.Create(p.cfg)
	if err != nil {
		return errors.Wrap(err, "create memberlist")
	}

	n, _ := ml.Join(p.knownPeers)
	level.Debug(p.logger).Log("msg", "joined cluster", "peers", n)

	if n > 0 {
		go warnIfAlone(p.logger, 10*time.Second, p.stopc, ml.NumMembers)
	}

	p.mlistMtx.Lock()
	p.mlist = ml
	p.mlistMtx.Unlock()

	// Initialize state with ourselves.
	p.data.Set(p.Name(), initialState)
	return nil
}

func (p *Peer) hasJoined() bool {
	p.mlistMtx.RLock()
	defer p.mlistMtx.RUnlock()

	return p.mlist != nil
}

func warnIfAlone(logger log.Logger, d time.Duration, stopc chan struct{}, numNodes func() int) {
	tick := time.NewTicker(d)
	defer tick.Stop()

	for {
		select {
		case <-stopc:
			return
		case <-tick.C:
			if n := numNodes(); n <= 1 {
				level.Warn(logger).Log("NumMembers", n, "msg", "I appear to be alone in the cluster")
			}
		}
	}
}

// SetLabels updates internal metadata's labels stored in PeerState for this peer.
// Note that this data will be propagated based on gossipInterval we set.
func (p *Peer) SetLabels(labels []storepb.Label) {
	if !p.hasJoined() {
		return
	}

	s, _ := p.data.Get(p.Name())
	s.Metadata.Labels = labels
	p.data.Set(p.Name(), s)
}

// SetTimestamps updates internal metadata's timestamps stored in PeerState for this peer.
// Note that this data will be propagated based on gossipInterval we set.
func (p *Peer) SetTimestamps(mint int64, maxt int64) {
	if !p.hasJoined() {
		return
	}

	s, _ := p.data.Get(p.Name())
	s.Metadata.MinTime = mint
	s.Metadata.MaxTime = maxt
	p.data.Set(p.Name(), s)
}

// Close leaves the cluster waiting up to timeout and shutdowns peer if cluster left.
// TODO(bplotka): Add this method into run.Group closing logic for each command. This will improve graceful shutdown.
func (p *Peer) Close(timeout time.Duration) {
	if !p.hasJoined() {
		return
	}

	err := p.mlist.Leave(timeout)
	if err != nil {
		level.Error(p.logger).Log("msg", "memberlist leave failed", "err", err)
	}
	close(p.stopc)
	p.mlist.Shutdown()
	p.mlist = nil
}

// Name returns the unique ID of this peer in the cluster.
func (p *Peer) Name() string {
	if !p.hasJoined() {
		return ""
	}

	return p.mlist.LocalNode().Name
}

// PeerTypesStoreAPIs gives a PeerType that allows all types that exposes StoreAPI.
func PeerTypesStoreAPIs() []PeerType {
	return []PeerType{PeerTypeStore, PeerTypeSource}
}

// PeerStates returns the custom state information for each peer by memberlist peer id (name).
func (p *Peer) PeerStates(types ...PeerType) map[string]PeerState {
	if !p.hasJoined() {
		return nil
	}

	ps := map[string]PeerState{}
	for _, o := range p.mlist.Members() {
		os, ok := p.data.Get(o.Name)
		if !ok {
			continue
		}

		if len(types) == 0 {
			ps[o.Name] = os
			continue
		}

		for _, t := range types {
			if os.Type == t {
				ps[o.Name] = os
				break
			}
		}
	}
	return ps
}

// PeerStates returns the custom state information by memberlist peer name.
func (p *Peer) PeerState(id string) (PeerState, bool) {
	if !p.hasJoined() {
		return PeerState{}, false
	}

	ps, ok := p.data.Get(id)
	if !ok {
		return PeerState{}, false
	}
	return ps, true
}

// Info returns a JSON-serializable dump of cluster state.
// Useful for debug.
func (p *Peer) Info() map[string]interface{} {
	if !p.hasJoined() {
		return nil
	}

	d := map[string]PeerState{}
	for k, v := range p.data.Data() {
		d[k] = v
	}

	return map[string]interface{}{
		"self":    p.mlist.LocalNode(),
		"members": p.mlist.Members(),
		"n":       p.mlist.NumMembers(),
		"state":   d,
	}
}

func resolvePeers(ctx context.Context, peers []string, myAddress string, res net.Resolver, waitIfEmpty bool) ([]string, error) {
	var resolvedPeers []string

	for _, peer := range peers {
		host, port, err := net.SplitHostPort(peer)
		if err != nil {
			return nil, errors.Wrapf(err, "split host/port for peer %s", peer)
		}

		retryCtx, cancel := context.WithCancel(ctx)
		ips, err := res.LookupIPAddr(ctx, host)
		if err != nil {
			// Assume direct address.
			resolvedPeers = append(resolvedPeers, peer)
			continue
		}

		if len(ips) == 0 {
			var lookupErrSpotted bool

			err := runutil.Retry(2*time.Second, retryCtx.Done(), func() error {
				if lookupErrSpotted {
					// We need to invoke cancel in next run of retry when lookupErrSpotted to preserve LookupIPAddr error.
					cancel()
				}

				ips, err = res.LookupIPAddr(retryCtx, host)
				if err != nil {
					lookupErrSpotted = true
					return errors.Wrapf(err, "IP Addr lookup for peer %s", peer)
				}

				ips = removeMyAddr(ips, port, myAddress)
				if len(ips) == 0 {
					if !waitIfEmpty {
						return nil
					}
					return errors.New("empty IPAddr result. Retrying")
				}

				return nil
			})
			if err != nil {
				return nil, err
			}
		}

		for _, ip := range ips {
			resolvedPeers = append(resolvedPeers, net.JoinHostPort(ip.String(), port))
		}
	}

	return resolvedPeers, nil
}

func removeMyAddr(ips []net.IPAddr, targetPort string, myAddr string) []net.IPAddr {
	var result []net.IPAddr

	for _, ip := range ips {
		if net.JoinHostPort(ip.String(), targetPort) == myAddr {
			continue
		}
		result = append(result, ip)
	}

	return result
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
