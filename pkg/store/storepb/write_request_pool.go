// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// Slices above these capacities are not reused, so one oversized request does not pin
// its memory in the pools.
const (
	maxPooledSeries           = 1 << 16
	maxPooledLabelsPerSeries  = 128
	maxPooledSamplesPerSeries = 1024
	maxPooledTenants          = 256
)

var (
	// Released elements keep the capacity of their inner slices.
	timeseriesPool sync.Pool // *[]prompb.TimeSeries
	tenantDataPool sync.Pool // *[]TimeSeriesTenantTuple
)

// UnmarshalPooled is Unmarshal with series slices taken from a pool. ReleasePooled must
// be called once m is no longer referenced, also when UnmarshalPooled fails.
func (m *WriteRequest) UnmarshalPooled(dAtA []byte) error {
	ts := getPooled[prompb.TimeSeries](&timeseriesPool)
	td := getPooled[TimeSeriesTenantTuple](&tenantDataPool)
	defer func() { m.Timeseries, m.TimeseriesTenantData = ts, td }()

	return forEachField(dAtA, func(num int, field, msg []byte) error {
		switch num {
		case 1:
			ts = nextElem(ts)
			return ts[len(ts)-1].Unmarshal(msg)
		case 4:
			td = nextElem(td)
			return td[len(td)-1].unmarshalPooled(msg)
		default:
			return m.Unmarshal(field)
		}
	})
}

func (m *TimeSeriesTenantTuple) unmarshalPooled(dAtA []byte) error {
	ts := m.Timeseries
	defer func() { m.Timeseries = ts }()

	return forEachField(dAtA, func(num int, field, msg []byte) error {
		if num == 1 {
			ts = nextElem(ts)
			return ts[len(ts)-1].Unmarshal(msg)
		}
		return m.Unmarshal(field)
	})
}

// ReleasePooled returns the slices taken by UnmarshalPooled. m must not be used afterwards.
func (m *WriteRequest) ReleasePooled() {
	ts, td := m.Timeseries, m.TimeseriesTenantData
	m.Timeseries, m.TimeseriesTenantData = nil, nil

	if ts = resetTimeseries(ts); cap(ts) > 0 {
		timeseriesPool.Put(&ts)
	}
	if cap(td) > 0 && cap(td) <= maxPooledTenants {
		for i := range td {
			td[i].Timeseries = resetTimeseries(td[i].Timeseries)
			td[i].Tenant = ""
		}
		td = td[:0]
		tenantDataPool.Put(&td)
	}
}

// forEachField calls fn for every field of dAtA: with num and the payload for nested
// messages in fields 1 and 4, with num 0 otherwise. Generated Unmarshal of a single field
// gives the same result as of the whole message.
func forEachField(dAtA []byte, fn func(num int, field, msg []byte) error) error {
	for len(dAtA) > 0 {
		n, err := skipRpc(dAtA)
		if err != nil {
			return err
		}
		if n > len(dAtA) {
			return io.ErrUnexpectedEOF
		}
		field := dAtA[:n]
		dAtA = dAtA[n:]

		tag, tagLen := binary.Uvarint(field)
		if tagLen <= 0 {
			return ErrIntOverflowRpc
		}
		if tag != 1<<3|2 && tag != 4<<3|2 {
			if err := fn(0, field, nil); err != nil {
				return err
			}
			continue
		}
		msgLen, lenLen := binary.Uvarint(field[tagLen:])
		if lenLen <= 0 || tagLen+lenLen+int(msgLen) != len(field) {
			return ErrInvalidLengthRpc
		}
		if err := fn(int(tag>>3), field, field[tagLen+lenLen:]); err != nil {
			return err
		}
	}
	return nil
}

func resetTimeseries(ts []prompb.TimeSeries) []prompb.TimeSeries {
	if cap(ts) > maxPooledSeries {
		return nil
	}
	for i := range ts {
		t := &ts[i]
		if cap(t.Labels) > maxPooledLabelsPerSeries {
			t.Labels = nil
		} else {
			// Label strings point into the request buffer.
			clear(t.Labels)
			t.Labels = t.Labels[:0]
		}
		if cap(t.Samples) > maxPooledSamplesPerSeries {
			t.Samples = nil
		} else {
			t.Samples = t.Samples[:0]
		}
		// The TSDB may keep inner slices of exemplars and histograms, do not reuse them.
		clear(t.Exemplars)
		t.Exemplars = t.Exemplars[:0]
		clear(t.Histograms)
		t.Histograms = t.Histograms[:0]
	}
	return ts[:0]
}

func getPooled[T any](p *sync.Pool) []T {
	if s, ok := p.Get().(*[]T); ok {
		return *s
	}
	return nil
}

func nextElem[T any](s []T) []T {
	if len(s) < cap(s) {
		return s[:len(s)+1]
	}
	var zero T
	return append(s, zero)
}
