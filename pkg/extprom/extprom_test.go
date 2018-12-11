package extprom

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestNilRegisterer(t *testing.T) {
	var r *SubsystemRegisterer

	testutil.Equals(t, "", r.Subsystem())
	testutil.Equals(t, nil, r.Registerer())
}
