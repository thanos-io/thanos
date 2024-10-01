// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storetestutil

import (
	"context"
	"io"
	"time"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// StoreSeriesClient is test gRPC storeAPI series client.
type StoreSeriesClient struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesClient
	Ctx             context.Context
	i               int
	RespSet         []*storepb.SeriesResponse
	RespDur         time.Duration
	SlowSeriesIndex int

	InjectedError      error
	InjectedErrorIndex int
}

func (c *StoreSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	if c.RespDur != 0 && (c.SlowSeriesIndex == c.i || c.SlowSeriesIndex == 0) {
		select {
		case <-time.After(c.RespDur):
		case <-c.Ctx.Done():
			return nil, c.Ctx.Err()
		}
	}
	if c.InjectedError != nil && (c.InjectedErrorIndex == c.i || c.InjectedErrorIndex == 0) {
		return nil, c.InjectedError
	}

	if c.i >= len(c.RespSet) {
		return nil, io.EOF
	}
	s := c.RespSet[c.i]

	c.i++

	return s, nil
}

func (c *StoreSeriesClient) Context() context.Context {
	return c.Ctx
}
