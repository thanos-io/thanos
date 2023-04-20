// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storeutils

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	v1 "github.com/thanos-io/thanos/pkg/api/blocks"

	"github.com/imroc/req/v3"
)

type HttpOpts struct {
	Timeout time.Duration
}

type Client interface {
	GetInstanceInfo(ctx context.Context, instanceIp string) (*InstanceInfo, error)
}

type thanosClient struct {
	logger log.Logger
	opts   HttpOpts
	client *req.Client
}

func NewClient(logger log.Logger, opts HttpOpts) Client {
	return &thanosClient{
		logger: logger,
		opts:   opts,
		client: req.C().SetTimeout(opts.Timeout),
	}
}

func (c *thanosClient) GetInstanceInfo(ctx context.Context, instanceAddress string) (*InstanceInfo, error) {
	var result Response
	response, err := c.client.R().
		SetContext(ctx).
		SetQueryParam("view", "loaded").
		SetSuccessResult(&result).
		Get(fmt.Sprintf("http://%s/api/v1/blocks", instanceAddress))
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to get instance info", "err", err, "responseDump", response.Dump())
		return nil, err
	}

	if !response.IsSuccessState() {
		return nil, fmt.Errorf("http request failed. status code: %d", response.StatusCode)
	}

	return result.ToInstanceInfo(), nil
}

type Response struct {
	Status string        `json:"status"`
	Data   v1.BlocksInfo `json:"data"`
}

func (r Response) ToInstanceInfo() *InstanceInfo {
	i := &InstanceInfo{}

	i.Name = ""

	labelSet := make(map[string][]string)
	minTime := time.Now().Add(time.Hour * 24 * 365 * 10).UnixMilli()
	maxTime := time.Now().Add(-time.Hour * 24 * 365 * 10)
	blocks := make([]BlockInfo, len(r.Data.Blocks))
	for _, block := range r.Data.Blocks {
		blocks = append(blocks, BlockInfo{Meta: block})
		if block.MinTime < minTime {
			minTime = block.MinTime
		}
	}
	i.Blocks = blocks
	i.BlockSummary = BlockSummary{
		LabelSet: labelSet,
		// from milliseconds to time
		TotalMinTime: time.Unix(0, minTime*int64(time.Millisecond)),
		TotalMaxTime: maxTime,
	}

	return i
}
