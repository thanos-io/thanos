package store

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// streamSeriesSet iterates over incoming stream of series.
// All errors are sent out of band via warning channel.
type streamSeriesSet2 struct {
	series []*storepb.Series
	pos    int
}

func startStreamSeriesSet2(
	ctx context.Context,
	logger log.Logger,
	stream storepb.Store_SeriesClient,
) (*streamSeriesSet2, error) {
	var series []*storepb.Series

	for {
		r, err := stream.Recv()

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, errors.Wrap(err, "receive series")
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			series = append(series, r.GetSeries())
			continue
		}
	}

	return &streamSeriesSet2{
		series: series,
		pos:    -1,
	}, nil
}

// Next blocks until new message is received or stream is closed or operation is timed out.
func (s *streamSeriesSet2) Next() (ok bool) {
	s.pos++
	return s.pos < len(s.series)
}

func (s *streamSeriesSet2) At() ([]storepb.Label, []storepb.AggrChunk) {
	currSeries := s.series[s.pos]
	return currSeries.Labels, currSeries.Chunks
}

func (s *streamSeriesSet2) Err() error {
	return nil
}
