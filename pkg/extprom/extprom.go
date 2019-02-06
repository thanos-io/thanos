// Package extprom is covering code that is used for extending native Prometheus packages functionality.

package extprom

import (
	"github.com/prometheus/client_golang/prometheus"
)

// SubsystemRegisterer type allows for subsystem specification. All packages that uses this type
// should use the gien subsystem.
// Registerer methods works even if registerer reference points to nil.
type SubsystemRegisterer struct {
	registerer prometheus.Registerer
	subsystem  string
}

func (r *SubsystemRegisterer) Registerer() prometheus.Registerer {
	if r == nil {
		return nil
	}
	return r.registerer
}

func (r *SubsystemRegisterer) Subsystem() string {
	if r == nil {
		return ""
	}
	return r.subsystem
}

func NewSubsystem(reg prometheus.Registerer, subsystem string) *SubsystemRegisterer {
	return &SubsystemRegisterer{registerer: reg, subsystem: subsystem}
}
