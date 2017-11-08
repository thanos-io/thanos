package cluster

import (
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
	"github.com/pkg/errors"
)

// Peer is a single peer in a gossip cluster.
type Peer struct {
	mlist *memberlist.Memberlist

	mtx   sync.RWMutex
	data  map[string]peerState
	stopc chan struct{}
}

// PeerType describes a peer's role in the cluster.
type PeerType string

// Constants holding valid PeerType values.
const (
	PeerTypeStore = "store"
	PeerTypeQuery = "query"
)

// NewPeer creates a new peer.
func NewPeer(
	l log.Logger,
	name string,
	typ PeerType,
	addr string,
	advertiseAddr string,
	knownPeers []string,
) (*Peer, error) {
	p := &Peer{
		data:  map[string]peerState{},
		stopc: make(chan struct{}),
	}
	d := newDelegate(l, p)

	cfg := memberlist.DefaultLANConfig()

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, errors.Wrap(err, "invalid listen address")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, errors.Wrap(err, "invalid listen address")
	}
	cfg.Name = name
	cfg.BindAddr = host
	cfg.BindPort = port
	cfg.Delegate = d
	cfg.Events = d
	cfg.GossipInterval = 5 * time.Second
	cfg.PushPullInterval = 60 * time.Second
	cfg.LogOutput = log.NewStdlibAdapter(level.Debug(l))

	if advertiseAddr != "" {
		advertiseHost, advertisePortStr, err := net.SplitHostPort(advertiseAddr)
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

	n, _ := ml.Join(knownPeers)
	level.Debug(l).Log("msg", "joined cluster", "peers", n)

	if n == 0 {
		go p.warnIfAlone(l, 5*time.Second)
	}

	// Initialize state with ourselves.
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	p.data[p.Name()] = peerState{Type: typ}

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

// ClusterSize returns the current number of alive members in the cluster.
func (p *Peer) ClusterSize() int {
	return p.mlist.NumMembers()
}

// Info returns a JSON-serializable dump of cluster state.
// Useful for debug.
func (p *Peer) Info() map[string]interface{} {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	d := map[string]peerState{}
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

type peerState struct {
	Type PeerType
}

// delegate implements memberlist.Delegate and memberlist.EventDelegate
// and broadcasts its peer's state in the cluster.
type delegate struct {
	*Peer

	logger log.Logger
	bcast  *memberlist.TransmitLimitedQueue
}

func newDelegate(l log.Logger, p *Peer) *delegate {
	bcast := &memberlist.TransmitLimitedQueue{
		NumNodes:       p.ClusterSize,
		RetransmitMult: 3,
	}
	return &delegate{
		logger: l,
		Peer:   p,
		bcast:  bcast,
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
	var data map[string]peerState
	if err := json.Unmarshal(b, &data); err != nil {
		level.Error(d.logger).Log("method", "NotifyMsg", "b", strings.TrimSpace(string(b)), "err", err)
		return
	}
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
	var data map[string]peerState
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
	level.Debug(d.logger).Log("received", "NotifyJoin", "node", n.Name, "addr", n.Address())
}

// NotifyLeave is called if a peer leaves the cluster.
func (d *delegate) NotifyLeave(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyLeave", "node", n.Name, "addr", n.Address())
	d.mtx.Lock()
	defer d.mtx.Unlock()
	delete(d.data, n.Name)
}

// NotifyUpdate is called if a cluster peer gets updated.
func (d *delegate) NotifyUpdate(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyUpdate", "node", n.Name, "addr", n.Address())
}
