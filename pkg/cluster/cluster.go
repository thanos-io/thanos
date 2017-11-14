package cluster

import (
	"io/ioutil"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"encoding/json"

	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
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

// PeerType describes a peer's role in the cluster.
type PeerType string

// Constants holding valid PeerType values.
const (
	PeerTypeStore = "store"
	PeerTypeQuery = "query"
)

type JoinConfig struct {
	Name          string
	Addr          string
	AdvertiseAddr string
	KnownPeers    []string
}

// Join creates the peer and joins it to the cluster with the given state.
func Join(l log.Logger, joinCfg JoinConfig, state PeerState, reg *prometheus.Registry) (*Peer, error) {
	p := &Peer{
		data:  map[string]PeerState{},
		stopc: make(chan struct{}),
	}
	d := newDelegate(l, p, reg)

	cfg := memberlist.DefaultLANConfig()

	host, portStr, err := net.SplitHostPort(joinCfg.Addr)
	if err != nil {
		return nil, errors.Wrap(err, "invalid listen address")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, errors.Wrap(err, "invalid listen address")
	}
	cfg.Name = joinCfg.Name
	cfg.BindAddr = host
	cfg.BindPort = port
	cfg.Delegate = d
	cfg.Events = d
	cfg.GossipInterval = 5 * time.Second
	cfg.PushPullInterval = 5 * time.Second
	cfg.LogOutput = ioutil.Discard

	if joinCfg.AdvertiseAddr != "" {
		advertiseHost, advertisePortStr, err := net.SplitHostPort(joinCfg.AdvertiseAddr)
		if err != nil {
			return nil, errors.Wrap(err, "invalid advertise address")
		}
		advertisePort, err := strconv.Atoi(advertisePortStr)
		if err != nil {
			return nil, errors.Wrap(err, "invalid advertise address")
		}
		cfg.AdvertiseAddr = advertiseHost
		cfg.AdvertisePort = advertisePort
	}

	ml, err := memberlist.Create(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "create memberlist")
	}
	p.mlist = ml

	n, err := ml.Join(joinCfg.KnownPeers)
	if err != nil {
		return nil, errors.Wrap(err, "join cluster")
	}

	level.Debug(l).Log("msg", "joined cluster", "peers", n)
	if n > 0 {
		go p.warnIfAlone(l, 10*time.Second)
	}

	// Initialize state with ourselves.
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	p.data[p.Name()] = state

	return p, nil
}

// SetLabels sets the labels and notifies other members.
func (p *Peer) SetLabels(labels []storepb.Label, timeout time.Duration) error {
	p.mtx.RLock()

	state := p.data[p.Name()]
	state.Labels = labels
	p.data[p.Name()] = state

	p.mtx.RUnlock()
	return p.mlist.UpdateNode(timeout)
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

// PeerStates returns the custom state information for each peer.
func (p *Peer) PeerStates(t PeerType) (ps []PeerState) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	for _, o := range p.mlist.Members() {
		os, ok := p.data[o.Name]
		if !ok || os.Type != t {
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

type PeerState struct {
	Type    PeerType
	APIAddr string

	Labels []storepb.Label
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

func newDelegate(l log.Logger, p *Peer, reg *prometheus.Registry) *delegate {
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
