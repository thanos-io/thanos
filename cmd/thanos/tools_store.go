// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/cmd/thanos/storeutils"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"go.uber.org/atomic"
)

type storeInspectConfig struct {
	timeout time.Duration
	http    storeutils.HttpOpts
	lister  Opts
}

func (sic *storeInspectConfig) registerInspectFlags(cmd extkingpin.FlagClause) *storeInspectConfig {
	cmd.Flag("timeout", "Timeout to fetch information from all stores").Default("5m").DurationVar(&sic.timeout)
	cmd.Flag("http.timeout", "Timeout for HTTP requests to store nodes").Default("2500ms").DurationVar(&sic.http.Timeout)
	cmd.Flag("lister.service", "Service name to discover Store nodes").
		Default("dnssrvnoa+_http._tcp.thanos-store.thanos.svc.cluster.local").
		StringVar(&sic.lister.StoreGatewayService)
	cmd.Flag("lister.label", "Label to discover Store nodes. This label can be repeated, the store MUST match all label pairs.").
		Required().PlaceHolder("<name>=\"<value>\"").StringsVar(&sic.lister.LabelPairs)

	return sic
}

func registerStoreTools(app extkingpin.AppClause) {
	cmd := app.Command("store", "Store utility commands")

	registerInspectCommand(cmd)
}

func registerInspectCommand(app extkingpin.AppClause) {
	subCmd := app.Command("inspect", "Inspect loaded blocks in stores using label matchers.")
	config := &storeInspectConfig{}
	config.registerInspectFlags(subCmd)

	subCmd.Setup(func(g *run.Group, logger log.Logger, _ *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		level.Info(logger).Log("msg", "started command")
		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		discovery := storeutils.NewAddressProvider(logger, []string{config.lister.StoreGatewayService})
		thanosClient := storeutils.NewClient(logger, config.http)
		storeLister := newLister(logger, discovery, thanosClient)

		ctx, cancel := context.WithTimeout(context.Background(), config.timeout)
		defer cancel()
		storeLister.LoadInfo(ctx)

		labelMap := map[string]string{}
		for _, labelPair := range config.lister.LabelPairs {
			parts := strings.Split(labelPair, "=")
			labelMap[parts[0]] = parts[1]
		}

		stores := storeLister.GetByLabelPairs(labelMap)
		lines := make([][]string, 0, len(stores))
		table := Table{Header: []string{"Name", "Min time", "Max time", "Matching blocks"}, Lines: lines}

		sort.Slice(stores, func(i, j int) bool {
			return stores[i].BlockSummary.TotalMinTime.Unix() < stores[j].BlockSummary.TotalMinTime.Unix()
		})
		for _, i := range stores {
			line := make([]string, 0, 4)
			line = append(line, i.Name)
			line = append(line, i.BlockSummary.TotalMinTime.Format(time.RFC3339))
			line = append(line, i.BlockSummary.TotalMaxTime.Format(time.RFC3339))

			matchingBlocks := 0

			for _, b := range i.Blocks {
				for l, v := range labelMap {
					if b.HasLabelPair(l, v) {
						matchingBlocks++
					}
				}
			}

			line = append(line, fmt.Sprintf("%d/%d", matchingBlocks, len(i.Blocks)))
			lines = append(lines, line)
		}
		table.Lines = lines

		err := printTable(os.Stdout, table)
		if err != nil {
			return err
		}

		return dummy()
	})
}

func dummy() error {
	return nil
}

type stringSet map[string]struct{}

func (s stringSet) values() []string {
	var values = make([]string, len(s))
	i := 0
	for v := range s {
		values[i] = v
		i++
	}
	return values
}

func (s stringSet) add(val string) {
	s[val] = struct{}{}
}

type Lister interface {
	GetStoreInfo() []*storeutils.InstanceInfo
	GetByLabelPairs(labelPair map[string]string) []*storeutils.InstanceInfo
	GetLabelMap() map[string][]string
	LoadInfo(ctx context.Context)
}

type Opts struct {
	LabelPairs          []string
	StoreGatewayService string
}

type InMemoryLister struct {
	m      *sync.RWMutex
	ap     storeutils.AddressProvider
	logger log.Logger
	client storeutils.Client

	storeInfo  map[string]*storeutils.InstanceInfo
	labelMap   map[string]stringSet // label -> []values
	lastUpdate time.Time
}

func newLister(logger log.Logger, ap storeutils.AddressProvider, client storeutils.Client) Lister {
	return &InMemoryLister{
		ap:        ap,
		logger:    logger,
		client:    client,
		storeInfo: make(map[string]*storeutils.InstanceInfo),
		labelMap:  make(map[string]stringSet),
		m:         &sync.RWMutex{},
	}
}

func (r *InMemoryLister) LoadInfo(ctx context.Context) {
	level.Info(r.logger).Log("msg", "Starting update of Store list...")
	start := time.Now()
	ips := r.ap.GetAddresses(ctx)

	// rewrite this log call
	level.Info(r.logger).Log("msg", "Found StoreAPIs", "count", len(ips), "elapsed", time.Since(start).String())

	var progress atomic.Int32
	wg := &sync.WaitGroup{}
	wg.Add(len(ips))

	for _, addr := range ips {
		// divide address and port
		fqdn, _ := splitAddress(addr)
		// create new goroutine for each address
		go func(fqdn string, addr string) {
			r.m.Lock()
			defer r.m.Unlock()
			defer wg.Done()

			level.Debug(r.logger).Log("msg", "updating store info", "store", fqdn)
			storeInfo, err := r.createOrUpdate(ctx, fqdn, addr)
			if err != nil {
				level.Error(r.logger).Log("msg", "error updating store info", "store", fqdn, "err", err)
				return
			}
			r.storeInfo[fqdn] = storeInfo
			progress.Inc()
			if progress.Load()%10 == 0 {
				level.Info(r.logger).Log(
					"msg",
					"updated store info",
					"progress",
					progress.Load(),
					"total",
					len(ips),
					"elapsed",
					time.Since(start).String(),
				)
			}
		}(fqdn, addr)
	}
	wg.Wait()

	r.updateLabelMap()
	r.lastUpdate = time.Now()
	level.Info(r.logger).Log("msg", "Finished update of Store list", "elapsed", time.Since(start).String(), "lastUpdate", r.lastUpdate.Format(time.RFC3339))
}

func splitAddress(addr string) (string, string) {
	parts := strings.Split(addr, ":")
	return parts[0], parts[1]
}

func (r *InMemoryLister) GetStoreInfo() []*storeutils.InstanceInfo {
	r.m.RLock()
	defer r.m.RUnlock()
	values := make([]*storeutils.InstanceInfo, len(r.storeInfo))
	i := 0
	for _, value := range r.storeInfo {
		values[i] = value
		i++
	}

	return values
}

func (r *InMemoryLister) GetLabelMap() map[string][]string {
	r.m.RLock()
	defer r.m.RUnlock()

	lm := make(map[string][]string, len(r.labelMap))
	for label, values := range r.labelMap {
		lm[label] = values.values()
	}

	return lm
}

func (r *InMemoryLister) GetByLabelPairs(pairs map[string]string) []*storeutils.InstanceInfo {
	r.m.Lock()
	defer r.m.Unlock()

	instances := make([]*storeutils.InstanceInfo, 0)
	for _, info := range r.storeInfo {
		matchAll := true
		for label, value := range pairs {
			if !info.HasLabelPair(label, value) {
				matchAll = false
				break
			}
		}
		if matchAll {
			instances = append(instances, info)
		}
	}

	return instances
}

func (r *InMemoryLister) createOrUpdate(ctx context.Context, name string, address string) (*storeutils.InstanceInfo, error) {
	level.Debug(r.logger).Log("msg", "creating or updating store info", "name", name, "address", address)
	storeInfo, err := r.client.GetInstanceInfo(ctx, address)
	if err != nil {
		return nil, err
	}
	storeInfo.Name = name

	return storeInfo, nil
}

func (r *InMemoryLister) updateLabelMap() {
	for _, info := range r.storeInfo {
		for _, block := range info.Blocks {
			for label, value := range block.Thanos.Labels {
				_, ok := r.labelMap[label]
				if !ok {
					r.labelMap[label] = stringSet{value: struct{}{}}
					continue
				}
				r.labelMap[label].add(value)
			}
		}
	}
}
