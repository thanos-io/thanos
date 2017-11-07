package query

import (
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb"
	tsdbLabels "github.com/prometheus/tsdb/labels"

	"github.com/improbable-eng/promlts/pkg/store/storepb"
)

type promSeriesSet struct {
	set tsdb.SeriesSet
}

func (s promSeriesSet) Next() bool         { return s.set.Next() }
func (s promSeriesSet) Err() error         { return s.set.Err() }
func (s promSeriesSet) At() storage.Series { return promSeries{s: s.set.At()} }

type promSeries struct {
	s tsdb.Series
}

func (s promSeries) Labels() labels.Labels            { return toPromLabels(s.s.Labels()) }
func (s promSeries) Iterator() storage.SeriesIterator { return storage.SeriesIterator(s.s.Iterator()) }

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

func toPromLabels(l tsdbLabels.Labels) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&l))
}
