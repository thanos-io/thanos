package query

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
)

type promSeriesSet struct {
	set        storepb.SeriesSet
	mint, maxt int64
}

func (s promSeriesSet) Next() bool { return s.set.Next() }
func (s promSeriesSet) Err() error { return s.set.Err() }

func (s promSeriesSet) At() storage.Series {
	lset, chunks := s.set.At()
	return newChunkSeries(lset, chunks, s.mint, s.maxt)
}

func translateMatcher(m *labels.Matcher) (storepb.LabelMatcher, error) {
	var t storepb.LabelMatcher_Type

	switch m.Type {
	case labels.MatchEqual:
		t = storepb.LabelMatcher_EQ
	case labels.MatchNotEqual:
		t = storepb.LabelMatcher_NEQ
	case labels.MatchRegexp:
		t = storepb.LabelMatcher_RE
	case labels.MatchNotRegexp:
		t = storepb.LabelMatcher_NRE
	default:
		return storepb.LabelMatcher{}, errors.Errorf("unrecognized matcher type %d", m.Type)
	}
	return storepb.LabelMatcher{Type: t, Name: m.Name, Value: m.Value}, nil
}

func translateMatchers(ms ...*labels.Matcher) ([]storepb.LabelMatcher, error) {
	res := make([]storepb.LabelMatcher, 0, len(ms))
	for _, m := range ms {
		r, err := translateMatcher(m)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}
