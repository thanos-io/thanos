package store

import "github.com/improbable-eng/thanos/pkg/store/storepb"

// ClientMock is a structure for mocking storepb.Client interface
type ClientMock struct {
	*storepb.StoreClientMock

	LabelsCallback    func() []storepb.Label
	TimeRangeCallback func() (mint int64, maxt int64)
	StringCallback    func() string
}

var _ Client = &ClientMock{}

func (c *ClientMock) Labels() []storepb.Label {
	return c.LabelsCallback()
}

func (c *ClientMock) TimeRange() (mint int64, maxt int64) {
	return c.TimeRangeCallback()
}

func (c *ClientMock) String() string {
	return c.StringCallback()
}
