// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package targets

import (
	"context"
	"io"
	"os"
	"sort"
	"testing"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/testutil"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
)

func TestMain(m *testing.M) {
	testutil.TolerantVerifyLeakMain(m)
}

type testTargetRecv struct {
	// A simulation of underlying grpc Recv behavior as per https://github.com/grpc/grpc-go/blob/7f2581f910fc21497091c4109b56d310276fc943/stream.go#L117-L125.
	err  error
	resp *targetspb.TargetDiscovery
}

type testTargetsClient struct {
	grpc.ClientStream
	targetsErr error
	recvs      []testTargetRecv
	i          int
}

func (t *testTargetsClient) String() string {
	return "test"
}

func (t *testTargetsClient) Recv() (*targetspb.TargetsResponse, error) {
	if t.i+1 >= len(t.recvs) {
		return nil, io.EOF
	}
	t.i++
	return targetspb.NewTargetsResponse(t.recvs[t.i].resp), t.recvs[t.i].err
}

func (t *testTargetsClient) Targets(context.Context, *targetspb.TargetsRequest, ...grpc.CallOption) (targetspb.Targets_TargetsClient, error) {
	t.i = -1
	return t, t.targetsErr
}

var _ targetspb.TargetsClient = &testTargetsClient{}

func TestProxy(t *testing.T) {
	activeTarget1 := &targetspb.ActiveTarget{GlobalUrl: "test1"}
	activeTarget2 := &targetspb.ActiveTarget{GlobalUrl: "test2"}
	activeTarget3 := &targetspb.ActiveTarget{GlobalUrl: "test3"}

	for _, tcase := range []struct {
		clients           []targetspb.TargetsClient
		expectedWarnNum   int
		expectedDiscovery *targetspb.TargetDiscovery
	}{
		{
			clients: []targetspb.TargetsClient{
				&testTargetsClient{targetsErr: errors.New("err1")},
				&testTargetsClient{targetsErr: errors.New("err2")},
			},
			expectedWarnNum: 2,
		},
		{
			clients: []targetspb.TargetsClient{
				&testTargetsClient{targetsErr: errors.New("err1")},
				&testTargetsClient{recvs: []testTargetRecv{{err: errors.New("err")}}},
			},
			expectedWarnNum: 2,
		},
		{
			clients: []targetspb.TargetsClient{
				&testTargetsClient{recvs: []testTargetRecv{{err: errors.New("err")}, {resp: &targetspb.TargetDiscovery{ActiveTargets: []*targetspb.ActiveTarget{activeTarget1}}}}},
			},
			expectedWarnNum: 1,
		},
		{
			clients: []targetspb.TargetsClient{
				&testTargetsClient{recvs: []testTargetRecv{
					{resp: &targetspb.TargetDiscovery{ActiveTargets: []*targetspb.ActiveTarget{activeTarget1}}},
					{resp: &targetspb.TargetDiscovery{ActiveTargets: []*targetspb.ActiveTarget{activeTarget2}}},
				}},
				&testTargetsClient{recvs: []testTargetRecv{
					{resp: &targetspb.TargetDiscovery{ActiveTargets: []*targetspb.ActiveTarget{activeTarget3}}},
				}},
				&testTargetsClient{recvs: []testTargetRecv{
					{err: errors.New("err")},
				}},
			},
			expectedWarnNum:   1,
			expectedDiscovery: &targetspb.TargetDiscovery{ActiveTargets: []*targetspb.ActiveTarget{activeTarget1, activeTarget2, activeTarget3}},
		},
		{
			// Reproduced the concurrent data race bug we had ( go test -race -run TestProxyDataRace -v ).
			clients: func() []targetspb.TargetsClient {
				size := 100
				endpoints := make([]targetspb.TargetsClient, 0, size)
				for i := 0; i < size; i++ {
					endpoints = append(endpoints, &testTargetsClient{recvs: []testTargetRecv{{err: errors.New("err")}}})
				}
				return endpoints
			}(),
			expectedWarnNum: 100,
		},
	} {
		if ok := t.Run("", func(t *testing.T) {
			if tcase.expectedDiscovery == nil {
				tcase.expectedDiscovery = &targetspb.TargetDiscovery{}
			}

			logger := log.NewLogfmtLogger(os.Stderr)
			p := NewProxy(logger, func() []targetspb.TargetsClient { return tcase.clients })
			req := &targetspb.TargetsRequest{
				State:                   targetspb.TargetsRequest_ANY,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			}

			// Test idempotency.
			for i := 0; i < 3; i++ {
				s := &targetsServer{ctx: context.Background(), targets: &targetspb.TargetDiscovery{}}
				testutil.Ok(t, p.Targets(req, s))
				testutil.Equals(t, tcase.expectedWarnNum, len(s.warnings))

				sort.Slice(s.targets.ActiveTargets, func(i, j int) bool {
					return s.targets.ActiveTargets[i].GlobalUrl < s.targets.ActiveTargets[j].GlobalUrl
				})
				testutil.Equals(t, tcase.expectedDiscovery, s.targets)
			}
		}); !ok {
			return
		}
	}

}
