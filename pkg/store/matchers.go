package store

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func translateMatcher(m storepb.LabelMatcher) (labels.Matcher, error) {
	switch m.Type {
	case storepb.LabelMatcher_EQ:
		return labels.NewEqualMatcher(m.Name, m.Value), nil

	case storepb.LabelMatcher_NEQ:
		return labels.Not(labels.NewEqualMatcher(m.Name, m.Value)), nil

	case storepb.LabelMatcher_RE:
		return labels.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")

	case storepb.LabelMatcher_NRE:
		m, err := labels.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
		if err != nil {
			return nil, err
		}
		return labels.Not(m), nil
	}
	return nil, errors.Errorf("unknown label matcher type %d", m.Type)
}

func translateMatchers(ms []storepb.LabelMatcher) (res []labels.Matcher, err error) {
	for _, m := range ms {
		r, err := translateMatcher(m)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}
