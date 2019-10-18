package store

import (
	"context"
	"time"
)

type interval struct {
	MinTime int64
	MaxTime int64
}

func chopTime(ctx context.Context, i interval, chopDuration time.Duration) <-chan interval {
	res := make(chan interval)

	go func() {
		defer close(res)

		if chopDuration <= 0 {
			res <- i
			return
		}

		mint := i.MinTime
		maxt := mint + chopDuration.Milliseconds() - 1

		for maxt <= i.MaxTime {
			select {
			case <-ctx.Done():
				return
			case res <- interval{MinTime: mint, MaxTime: maxt}:
				mint = maxt + 1
				maxt = mint + chopDuration.Milliseconds() - 1
				continue
			}
		}
	}()

	return res
}
