// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/imroc/req/v3"
	v1 "github.com/thanos-io/thanos/pkg/api/blocks"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
)

type addressProvider interface {
	GetAddresses(context.Context) []string
}

type dnsProvider struct {
	provider     *dns.Provider
	dnsAddresses []string
	logger       log.Logger
}

func newAddressProvider(logger log.Logger, dnsAddresses []string) addressProvider {
	return &dnsProvider{
		logger:       logger,
		dnsAddresses: dnsAddresses,
		provider:     dns.NewProvider(logger, prometheus.DefaultRegisterer, dns.GolangResolverType),
	}
}

func (d *dnsProvider) GetAddresses(ctx context.Context) []string {
	level.Info(d.logger).Log("msg", "resolving SRV records", "addresses", d.dnsAddresses)
	err := d.provider.Resolve(ctx, d.dnsAddresses)
	if err != nil {
		level.Error(d.logger).Log("msg", "failed to resolve SRV records", "err", err)
	}

	return d.provider.Addresses()
}

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
	var result storeResponse
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

type storeResponse struct {
	Status string        `json:"status"`
	Data   v1.BlocksInfo `json:"data"`
}

func (r storeResponse) ToInstanceInfo() *InstanceInfo {
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

type labelSet map[string][]string

func (l labelSet) String() string {
	var s string
	if len(l) == 0 {
		return "No labels"
	}

	elipsisCutOut := 5
	for k, v := range l {
		valStrBuilder := strings.Builder{}
		valStrBuilder.WriteString("[")
		for i, val := range v {
			valStrBuilder.WriteString(val)
			if i >= elipsisCutOut {
				valStrBuilder.WriteString("...")
				valStrBuilder.WriteString(fmt.Sprintf(" (%d more)", len(v)-elipsisCutOut))
				break
			}
			if i != len(v)-1 {
				valStrBuilder.WriteString(", ")
			}
		}
		valStrBuilder.WriteString("]")

		s += fmt.Sprintf("\t\t%s: %s\n", k, valStrBuilder.String())
	}

	return s
}

type BlockSummary struct {
	LabelSet     labelSet  `json:"labelSet"`
	TotalMinTime time.Time `json:"totalMinTime"`
	TotalMaxTime time.Time `json:"totalMaxTime"`
}

type BlockInfo struct {
	metadata.Meta
}

func (b BlockInfo) HasLabelPair(key, value string) bool {
	for l, v := range b.Thanos.Labels {
		if l == key && v == value {
			return true
		}
	}

	return false
}

type InstanceInfo struct {
	Name         string       `json:"name"`
	Blocks       []BlockInfo  `json:"blocks"`
	BlockSummary BlockSummary `json:"blockSummary"`
}

func (i InstanceInfo) HasLabelPair(key, value string) bool {
	for _, block := range i.Blocks {
		if block.HasLabelPair(key, value) {
			return true
		}
	}

	return false
}

func (b BlockInfo) String() string {
	return fmt.Sprintf(
		"Block: %s\n\tMin time: %s\n\tMax time: %s\n\tLabelset: \n%s",
		b.Meta.ULID,
		time.Unix(b.Meta.MinTime/1000, 0).Format(time.RFC1123Z),
		time.Unix(b.Meta.MinTime/1000, 0).Format(time.RFC1123Z),
		b.Thanos.Labels,
	)
}
