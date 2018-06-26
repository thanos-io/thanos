package cluster

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	"github.com/hashicorp/go-sockaddr"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/timestamp"
)

func joinPeer(num int, knownPeers []string) (peerAddr string, peer *Peer, err error) {
	port, err := testutil.FreePort()
	if err != nil {
		return "", nil, err
	}
	peerAddr = fmt.Sprintf("127.0.0.1:%d", port)
	now := time.Now()

	peer, err = New(
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		peerAddr,
		peerAddr,
		apiAddr(num),
		"",
		knownPeers,
		false,
		100*time.Millisecond,
		50*time.Millisecond,
		30*time.Millisecond,
	)
	if err != nil {
		return "", nil, err
	}
	err = peer.Join(PeerTypeSource, PeerMetadata{
		Labels: []storepb.Label{
			{
				Name:  "a",
				Value: fmt.Sprintf("%d", num),
			},
		},
		MinTime: timestamp.FromTime(now.Add(-10 * time.Minute)),
		MaxTime: timestamp.FromTime(now.Add(-1 * time.Second)),
	})
	if err != nil {
		return "", nil, err
	}
	return peerAddr, peer, nil
}

func apiAddr(num int) string {
	return fmt.Sprintf("sidecar-address:%d", num)
}

func TestPeers_PropagatingState(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	addr1, peer1, err := joinPeer(1, nil)
	testutil.Ok(t, err)
	defer peer1.Close(5 * time.Second)

	_, peer2, err := joinPeer(2, []string{addr1})
	testutil.Ok(t, err)
	defer peer2.Close(5 * time.Second)

	// peer2 should see two members with their data.
	expected := []string{apiAddr(1), apiAddr(2)}
	testutil.Equals(t, expected, apiAddrs(peer2.PeerStates(PeerTypeSource)))

	// Check if we have consistent info for PeerStates vs PeerState.
	for id, ps := range peer2.PeerStates() {
		directPs, ok := peer2.PeerState(id)
		testutil.Assert(t, ok, "listed id should be gettable")
		testutil.Equals(t, ps, directPs)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	testutil.Ok(t, runutil.Retry(1*time.Second, ctx.Done(), func() error {
		if len(peer1.data.Data()) > 1 {
			return nil
		}
		return errors.New("I am alone here")
	}))

	// peer1 should see two members with their data.
	testutil.Equals(t, expected, apiAddrs(peer1.PeerStates(PeerTypeSource)))

	// Check if we have consistent info for PeerStates vs PeerState.
	for id, ps := range peer1.PeerStates() {
		directPs, ok := peer1.PeerState(id)
		testutil.Assert(t, ok, "listed id should be gettable")
		testutil.Equals(t, ps, directPs)
	}

	// Update peer1 state.
	now := time.Now()
	newPeerMeta1 := PeerMetadata{
		Labels: []storepb.Label{
			{
				Name:  "b",
				Value: "1",
			},
		},
		MinTime: timestamp.FromTime(now.Add(-20 * time.Minute)),
		MaxTime: timestamp.FromTime(now.Add(-1 * time.Millisecond)),
	}
	peer1.SetLabels(newPeerMeta1.Labels)
	peer1.SetTimestamps(newPeerMeta1.MinTime, newPeerMeta1.MaxTime)

	// Check if peer2 got the updated meta about peer1.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	testutil.Ok(t, runutil.Retry(1*time.Second, ctx2.Done(), func() error {
		for _, st := range peer2.PeerStates(PeerTypeSource) {
			if st.StoreAPIAddr != "sidecar-address:1" {
				continue
			}

			if reflect.DeepEqual(st.Metadata, newPeerMeta1) {
				return nil
			}
		}
		return errors.New("outdated metadata")
	}))
}

func apiAddrs(states map[string]PeerState) (addrs []string) {
	for _, ps := range states {
		addrs = append(addrs, ps.StoreAPIAddr)
	}
	sort.Strings(addrs)
	return addrs
}

func TestCalculateAdvAddress(t *testing.T) {
	privateIP, err := sockaddr.GetPrivateIP()
	testutil.Ok(t, err)

	for _, tc := range []struct {
		bind      string
		advertise string

		expected string
		err      error
	}{
		{
			err: errors.New("invalid bind address '': missing port in address"),
		},
		{
			advertise: "l1234",
			err:       errors.New("invalid advertise address: address l1234: missing port in address"),
		},
		{
			advertise: "localhost:1234",
			expected:  "localhost:1234",
		},
		{
			bind:      "local:5555",
			advertise: "ext:1234",
			expected:  "ext:1234",
		},
		{
			bind:     "local:5555",
			expected: "local:5555",
		},
		{
			bind:     ":5555",
			expected: privateIP + ":5555",
		},
		{
			bind:     "0.0.0.0:5555",
			expected: privateIP + ":5555",
		},
	} {
		if ok := t.Run("", func(t *testing.T) {
			host, port, err := CalculateAdvertiseAddress(tc.bind, tc.advertise)
			if tc.err != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tc.err.Error(), err.Error())
				return
			}
			testutil.Ok(t, err)
			testutil.Equals(t, tc.expected, fmt.Sprintf("%s:%d", host, port))

		}); !ok {
			return
		}
	}
}
