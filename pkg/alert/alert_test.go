package alert

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
)

func TestQueue_Push_Relabelled(t *testing.T) {
	q := NewQueue(
		nil, nil, 10, 10,
		labels.FromStrings("a", "1", "replica", "A"), // Labels to be added.
		[]string{"b", "replica"},                     // Labels to be dropped (excluding those added).
	)

	q.Push([]*Alert{
		{Labels: labels.FromStrings("b", "2", "c", "3")},
		{Labels: labels.FromStrings("c", "3")},
		{Labels: labels.FromStrings("a", "2")},
	})

	testutil.Equals(t, 3, len(q.queue))
	testutil.Equals(t, labels.FromStrings("a", "1", "c", "3"), q.queue[0].Labels)
	testutil.Equals(t, labels.FromStrings("a", "1", "c", "3"), q.queue[1].Labels)
	testutil.Equals(t, labels.FromStrings("a", "1"), q.queue[2].Labels)
}
