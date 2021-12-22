// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"fmt"
	"strings"
)

func (m *QueryHints) toPromQL(labelMatchers []LabelMatcher) string {
	grouping := m.Grouping.toPromQL()
	matchers := MatchersToString(labelMatchers...)
	queryRange := m.Range.toPromQL()

	query := fmt.Sprintf("%s %s (%s%s)", m.Func.Name, grouping, matchers, queryRange)
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
