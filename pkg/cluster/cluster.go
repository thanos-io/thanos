package cluster

import (
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"encoding/json"

	"math/rand"
	"sync"

	"context"

	"io/ioutil"

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
	mlist *memberlist.Memberlist

	mtx   sync.RWMutex
	data  map[string]PeerState
	stopc chan struct{}
}

var (
	pushPullInterval = 5 * time.Second
	gossipInterval   = 5 * time.Second
)

// PeerType describes a peer's role in the cluster.
type PeerType string

// Constants holding valid PeerType values.
const (
	PeerTypeStoreGCS     = "store-gcs"
	PeerTypeStoreSidecar = "store-sidecar"
	PeerTypeQuery        = "query"
)

// PeerState contains state for the peer.
type PeerState struct {
	Type    PeerType ``
	APIAddr string

	Metadata PeerMetadata
}

// PeerMetadata are the information that can change in runtime of the peer.
type PeerMetadata struct {
	Labels []storepb.Label

	// LowTimestamp indicates the minTime of the oldest block available from this peer.
	LowTimestamp int64
	// HighTimestamp indicates the maxTime of the youngest block available from this peer.
	HighTimestamp int64
}

// MetadataUpdater is a function that allows to update metadata of the peer.
type MetadataUpdater interface {
	CurrentMetadata() PeerMetadata
	UpdateMetadata(PeerMetadata)
}

type nopMetadataUpdater struct{}

func (nopMetadataUpdater) CurrentMetadata() PeerMetadata { return PeerMetadata{} }
func (nopMetadataUpdater) UpdateMetadata(PeerMetadata)   {}

func NopMetadataUpdarter() MetadataUpdater {
	return nopMetadataUpdater{}
}

// Join creates a new peer that joins the cluster.
func Join(
	l log.Logger,
	reg *prometheus.Registry,
	bindAddr string,
	advertiseAddr string,
	knownPeers []string,
	initialState PeerState,
	waitIfEmpty bool,
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

	// If the API listens on 0.0.0.0, deduce it to the advertise IP.
	if initialState.APIAddr != "" {
		apiHost, apiPort, err := net.SplitHostPort(initialState.APIAddr)
		if err != nil {
			return nil, errors.Wrap(err, "invalid API address")
		}
		if apiHost == "0.0.0.0" {
			initialState.APIAddr = net.JoinHostPort(addr.String(), apiPort)
		}
	}

	l = log.With(l, "component", "cluster")

	// TODO(fabxc): generate human-readable but random names?
	name, err := ulid.New(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano())))
	if err != nil {
		return nil, err
	}

	p := &Peer{
		data:  map[string]PeerState{},
		stopc: make(chan struct{}),
	}
	d := newDelegate(l, reg, p)

	cfg := memberlist.DefaultLANConfig()
	cfg.Name = name.String()
	cfg.BindAddr = bindHost
	cfg.BindPort = bindPort
	cfg.Delegate = d
	cfg.Events = d
	cfg.GossipInterval = gossipInterval
	cfg.PushPullInterval = pushPullInterval
	cfg.LogOutput = ioutil.Discard
	if advertiseAddr != "" {
		cfg.AdvertiseAddr = advertiseHost
		cfg.AdvertisePort = advertisePort
	}

	ml, err := memberlist.Create(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "create memberlist")
	}
	p.mlist = ml

	n, _ := ml.Join(knownPeers)
	level.Debug(l).Log("msg", "joined cluster", "peers", n)

	if n > 0 {
		go p.warnIfAlone(l, 10*time.Second)
	}

	// Initialize state with ourselves.
	p.mtx.RLock()
	p.data[p.Name()] = initialState
	p.mtx.RUnlock()

	return p, nil
}

func (p *Peer) warnIfAlone(logger log.Logger, d time.Duration) {
	tick := time.NewTicker(d)
	defer tick.Stop()

	for {
		select {
		case <-p.stopc:
			return
		case <-tick.C:
			if n := p.mlist.NumMembers(); n <= 1 {
				level.Warn(logger).Log("NumMembers", n, "msg", "I appear to be alone in the cluster")
			}
		}
	}
}

// UpdateMetadata updates internal metadata stored in PeerState for this peer.
// Note that this data will be propagated based on gossipInterval we set.
func (p *Peer) UpdateMetadata(metadata PeerMetadata) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	s := p.data[p.Name()]
	s.Metadata = metadata
	p.data[p.Name()] = s
}

// CurrentMetadata returns state metadata for this peer.
func (p *Peer) CurrentMetadata() PeerMetadata {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.data[p.Name()].Metadata
}

// Leave the cluster, waiting up to timeout.
func (p *Peer) Leave(timeout time.Duration) error {
	close(p.stopc)
	return p.mlist.Leave(timeout)
}

// Name returns the unique ID of this peer in the cluster.
func (p *Peer) Name() string {
	return p.mlist.LocalNode().Name
}

// Peers returns a sorted address list of peers of the given type.
func (p *Peer) Peers(t PeerType) (ps []string) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	for _, o := range p.mlist.Members() {
		os, ok := p.data[o.Name]
		if !ok || os.Type != t {
			continue
		}
		ps = append(ps, o.Address())
	}
	sort.Strings(ps)
	return ps
}

func AnyStorePeerCond() func(PeerType) bool {
	return func(t PeerType) bool {
		return t == PeerTypeStoreGCS || t == PeerTypeStoreSidecar
	}
}

func PeerCond(wanted PeerType) func(PeerType) bool {
	return func(t PeerType) bool {
		return t == wanted
	}
}

// PeerStates returns the custom state information for each peer.
func (p *Peer) PeerStates(typeCond func(PeerType) bool) (ps []PeerState) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	for _, o := range p.mlist.Members() {
		os, ok := p.data[o.Name]
		if !ok || !typeCond(os.Type) {
			continue
		}
		ps = append(ps, os)
	}
	return ps
}

// ClusterSize returns the current number of alive members in the cluster.
func (p *Peer) ClusterSize() int {
	return p.mlist.NumMembers()
}

// Info returns a JSON-serializable dump of cluster state.
// Useful for debug.
func (p *Peer) Info() map[string]interface{} {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	d := map[string]PeerState{}
	for k, v := range p.data {
		d[k] = v
	}

	return map[string]interface{}{
		"self":    p.mlist.LocalNode(),
		"members": p.mlist.Members(),
		"n":       p.mlist.NumMembers(),
		"state":   d,
	}
}

// delegate implements memberlist.Delegate and memberlist.EventDelegate
// and broadcasts its peer's state in the cluster.
type delegate struct {
	*Peer

	logger log.Logger
	bcast  *memberlist.TransmitLimitedQueue

	gossipMsgsReceived   prometheus.Counter
	gossipClusterMembers prometheus.Gauge
}

func newDelegate(l log.Logger, reg *prometheus.Registry, p *Peer) *delegate {
	bcast := &memberlist.TransmitLimitedQueue{
		NumNodes:       p.ClusterSize,
		RetransmitMult: 3,
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

	return &delegate{
		logger:               l,
		Peer:                 p,
		bcast:                bcast,
		gossipMsgsReceived:   gossipMsgsReceived,
		gossipClusterMembers: gossipClusterMembers,
	}
}

func (d *delegate) init(self string, numMembers func() int) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	d.bcast = &memberlist.TransmitLimitedQueue{
		NumNodes:       numMembers,
		RetransmitMult: 3,
	}
}

// NodeMeta retrieves meta-data about the current node when broadcasting an alive message.
func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

// NotifyMsg is the callback invoked when a user-level gossip message is received.
func (d *delegate) NotifyMsg(b []byte) {
	var data map[string]PeerState
	if err := json.Unmarshal(b, &data); err != nil {
		level.Error(d.logger).Log("method", "NotifyMsg", "b", strings.TrimSpace(string(b)), "err", err)
		return
	}
	d.gossipMsgsReceived.Inc()

	d.mtx.Lock()
	defer d.mtx.Unlock()
	for k, v := range data {
		// Removing data is handled by NotifyLeave
		d.data[k] = v
	}
}

// GetBroadcasts is called when user data messages can be broadcasted.
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	return d.bcast.GetBroadcasts(overhead, limit)
}

// LocalState is called when gossip fetches local state.
func (d *delegate) LocalState(_ bool) []byte {
	d.mtx.RLock()
	defer d.mtx.RUnlock()

	b, err := json.Marshal(&d.data)
	if err != nil {
		panic(err)
	}
	return b
}

func (d *delegate) MergeRemoteState(buf []byte, _ bool) {
	var data map[string]PeerState
	if err := json.Unmarshal(buf, &data); err != nil {
		level.Error(d.logger).Log("method", "MergeRemoteState", "err", err)
		return
	}
	d.mtx.Lock()
	defer d.mtx.Unlock()
	for k, v := range data {
		d.data[k] = v
	}
}

// NotifyJoin is called if a peer joins the cluster.
func (d *delegate) NotifyJoin(n *memberlist.Node) {
	d.gossipClusterMembers.Inc()
	level.Debug(d.logger).Log("received", "NotifyJoin", "node", n.Name, "addr", n.Address())
}

// NotifyLeave is called if a peer leaves the cluster.
func (d *delegate) NotifyLeave(n *memberlist.Node) {
	d.gossipClusterMembers.Dec()
	level.Debug(d.logger).Log("received", "NotifyLeave", "node", n.Name, "addr", n.Address())
	d.mtx.Lock()
	defer d.mtx.Unlock()
	delete(d.data, n.Name)
}

// NotifyUpdate is called if a cluster peer gets updated.
func (d *delegate) NotifyUpdate(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyUpdate", "node", n.Name, "addr", n.Address())
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
