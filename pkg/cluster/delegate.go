package cluster

import (
	"encoding/json"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
)

// delegate implements memberlist.Delegate and memberlist.EventDelegate
// and broadcasts its peer's state in the cluster.
type delegate struct {
	*memberlist.TransmitLimitedQueue

	logger log.Logger
	data   *data

	gossipMsgsReceived   prometheus.Counter
	gossipClusterMembers prometheus.Gauge
}

func newDelegate(l log.Logger, numNodes func() int, data *data, gossipMsgsReceived prometheus.Counter, gossipClusterMembers prometheus.Gauge) *delegate {
	return &delegate{
		TransmitLimitedQueue: &memberlist.TransmitLimitedQueue{
			NumNodes:       numNodes,
			RetransmitMult: 3,
		},
		logger:               l,
		data:                 data,
		gossipMsgsReceived:   gossipMsgsReceived,
		gossipClusterMembers: gossipClusterMembers,
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

	for k, v := range data {
		// Removing data is handled by NotifyLeave
		d.data.Set(k, v)
	}
}

// LocalState is called when gossip fetches local state.
func (d *delegate) LocalState(_ bool) []byte {
	b, err := json.Marshal(d.data.Data())
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
	for k, v := range data {
		// Removing data is handled by NotifyLeave
		d.data.Set(k, v)
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
	d.data.Del(n.Name)
}

// NotifyUpdate is called if a cluster peer gets updated.
func (d *delegate) NotifyUpdate(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyUpdate", "node", n.Name, "addr", n.Address())
}
