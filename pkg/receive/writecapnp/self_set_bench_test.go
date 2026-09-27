// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"fmt"
	"testing"

	"capnproto.org/go/capnp/v3"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/symboltable"
)

// BenchmarkMultiTenantBuild builds a multi-tenant WriteRequest message the
// same way RemoteWriteClient.writeWithReconnect does and reports its
// marshaled size. The redundant_self_set variant additionally calls
// tl.Set(i, ttl) after BuildInto, i.e. it copies each list element onto
// itself: that deep-copies everything the element references into fresh
// allocations and orphans the originals, which are still serialized -
// doubling the message on the wire.
func BenchmarkMultiTenantBuild(b *testing.B) {
	const (
		numTenants = 3
		numSeries  = 100
	)
	tuples := make([]storepb.TimeSeriesTenantTuple, 0, numTenants)
	for t := range numTenants {
		series := make([]prompb.TimeSeries, 0, numSeries)
		for s := range numSeries {
			series = append(series, prompb.TimeSeries{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "instance", Value: fmt.Sprintf("host-%d:9090", s)},
					{Name: "job", Value: "api"},
				},
				Samples: []prompb.Sample{{Timestamp: int64(s), Value: float64(s)}},
			})
		}
		tuples = append(tuples, storepb.TimeSeriesTenantTuple{
			Tenant:     fmt.Sprintf("tenant-%d", t),
			Timeseries: series,
		})
	}

	for _, bench := range []struct {
		name             string
		redundantSelfSet bool
	}{
		{name: "in_place", redundantSelfSet: false},
		{name: "redundant_self_set", redundantSelfSet: true},
	} {
		b.Run(bench.name, func(b *testing.B) {
			var size int
			for b.Loop() {
				msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
				require.NoError(b, err)

				wr, err := NewRootWriteRequest(seg)
				require.NoError(b, err)
				sym, err := wr.NewSymbols()
				require.NoError(b, err)
				tl, err := NewTimeSeriesTenantTuple_List(wr.Segment(), int32(len(tuples)))
				require.NoError(b, err)

				builder := symboltable.NewBuilder()
				for i, d := range tuples {
					// At returns a view into the list's inline storage:
					// BuildInto fills the element (and the objects it points
					// to) in place.
					ttl := tl.At(i)
					require.NoError(b, BuildInto(&ttl, d.Tenant, d.Timeseries, builder))

					if bench.redundantSelfSet {
						require.NoError(b, tl.Set(i, ttl))
					}
				}
				require.NoError(b, marshalSymbols(builder, sym))
				require.NoError(b, wr.SetData(tl))

				data, err := msg.Marshal()
				require.NoError(b, err)
				size = len(data)
			}
			b.ReportMetric(float64(size), "wire_bytes")
		})
	}
}
