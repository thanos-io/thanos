package mocks

import (
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
)

type MetaUpdater struct {
	Meta cluster.PeerMetadata
}

func (u *MetaUpdater) SetLabels(labels []storepb.Label) {
	u.Meta.Labels = labels
}
func (u *MetaUpdater) SetTimestamps(lowTimestamp int64, highTimestamp int64) {
	u.Meta.LowTimestamp = lowTimestamp
	u.Meta.HighTimestamp = highTimestamp
}
