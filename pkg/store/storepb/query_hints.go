// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/storage"
)

func (m *QueryHints) MergePromHints(hints *storage.SelectHints) {
	if hints == nil {
		return
	}
	m.StartMillis = hints.Start
	m.EndMillis = hints.End
	m.StepMillis = hints.Step

	if hints.Grouping != nil {
		m.Grouping = &Grouping{
			By:     hints.By,
			Labels: hints.Grouping,
		}
	}
	if hints.Range != 0 {
		m.Range = &Range{Millis: hints.Range}
	}
}

func (m *QueryHints) RangeMillis() int64 {
	if m.Range == nil {
		return 0
	}

	return m.Range.Millis
}

func (m *QueryHints) AggrFuncName() string {
	if m.AggrFunc == nil {
		return ""
	}

	return m.AggrFunc.Name
}

func (m *QueryHints) TimeFuncName() string {
	if m.TimeFunc == nil {
		return ""
	}

	return m.TimeFunc.Name
}

func (m *QueryHints) toPromQL(labelMatchers []LabelMatcher) string {
	aggrFunc := m.AggrFuncName()
	timeFunc := m.TimeFuncName()
	grouping := m.Grouping.toPromQL()
	matchers := MatchersToString(labelMatchers...)
	queryRange := m.Range.toPromQL()

	var query string
	if timeFunc == "" {
		query = fmt.Sprintf("%s %s (%s%s)", aggrFunc, grouping, matchers, queryRange)
	} else {
		query = fmt.Sprintf("%s %s (%s(%s%s))", aggrFunc, grouping, timeFunc, matchers, queryRange)
	}

	// Remove double spaces if some expressions are missing.
	return strings.Join(strings.Fields(query), " ")
}

func (m *Grouping) toPromQL() string {
	if m == nil {
		return ""
	}

	if len(m.Labels) == 0 {
		return ""
	}
	var op string
	if m.By {
		op = "by"
	} else {
		op = "without"
	}

	return fmt.Sprintf("%s (%s)", op, strings.Join(m.Labels, ","))
}

func (m *Range) toPromQL() string {
	if m == nil {
		return ""
	}

	if m.Millis == 0 {
		return ""
	}
	return fmt.Sprintf("[%dms]", m.Millis)
}
