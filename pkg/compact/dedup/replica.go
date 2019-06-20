package dedup

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/objstore"
)

const (
	AggReplicaLabel = "_agg_replica_"
	ResolutionLabel = "resolution"
)

// Replica defines a group of blocks from same global configs and resolution.
// Ex, replica{name=r0,labels=[cluster=c0, shard=s0,resolution=0],blocks=[b0, b1]}
type Replica struct {
	Name   string            // the value specified by replica label
	Labels map[string]string // includes resolution and global configs
	Blocks []*metadata.Meta  // the underlying blocks
}

func NewReplica(name string, labels map[string]string, blocks []*metadata.Meta) *Replica {
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].MinTime < blocks[j].MinTime
	})
	return &Replica{
		Name:   name,
		Labels: labels,
		Blocks: blocks,
	}
}

func (r *Replica) MinTime() int64 {
	if len(r.Blocks) == 0 {
		return -1
	}
	return r.Blocks[0].MinTime
}

func (r *Replica) MaxTime() int64 {
	if len(r.Blocks) == 0 {
		return -1
	}
	return r.Blocks[len(r.Blocks)-1].MaxTime
}

func (r *Replica) Group() string {
	return replicaGroup(r.Labels)
}

func (r *Replica) isAggReplica() bool {
	return r.Name == AggReplicaLabel
}

func replicaGroup(labels map[string]string) string {
	names := make([]string, 0, len(labels))
	for k := range labels {
		names = append(names, k)
	}
	sort.Slice(names, func(i, j int) bool {
		return names[i] < names[j]
	})
	var b strings.Builder
	b.WriteString("[")
	for i, name := range names {
		if i != 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("%s=%s", name, labels[name]))
	}
	b.WriteString("]")
	return b.String()
}

type Replicas []*Replica

// Group blocks by their global configs and resolution into different replicas.
func NewReplicas(replicaLabelName string, blocks []*metadata.Meta) (Replicas, error) {
	m := make(map[string]map[string][]*metadata.Meta, 0)
	groupLabels := make(map[string]map[string]string)
	for _, b := range blocks {
		name, ok := b.Thanos.Labels[replicaLabelName]
		if !ok {
			return nil, errors.Errorf("not found replica label '%s' on block: %s", replicaLabelName, b.ULID.String())
		}
		labels := replicaLabels(replicaLabelName, b)
		group := replicaGroup(labels)
		groupLabels[group] = labels
		if _, ok := m[group]; !ok {
			m[group] = make(map[string][]*metadata.Meta, 0)
		}
		m[group][name] = append(m[group][name], b)
	}
	replicas := make(Replicas, 0)
	for group, v := range m {
		for name, blocks := range v {
			replicas = append(replicas, NewReplica(name, groupLabels[group], blocks))
		}
	}
	return replicas, nil
}

func replicaLabels(replicaLabelName string, b *metadata.Meta) map[string]string {
	labels := make(map[string]string)
	for k, v := range b.Thanos.Labels {
		if k == replicaLabelName {
			continue
		}
		labels[k] = v
	}
	labels[ResolutionLabel] = fmt.Sprint(b.Thanos.Downsample.Resolution)
	return labels
}

type ReplicaSyncer struct {
	logger               log.Logger
	metrics              *DedupMetrics
	bkt                  objstore.Bucket
	consistencyDelay     time.Duration
	blockSyncConcurrency int

	labelName string
	mtx       sync.Mutex
	blocksMtx sync.Mutex
}

func NewReplicaSyncer(logger log.Logger, metrics *DedupMetrics, bkt objstore.Bucket, labelName string,
	consistencyDelay time.Duration, blockSyncConcurrency int) *ReplicaSyncer {
	return &ReplicaSyncer{
		logger:               logger,
		metrics:              metrics,
		bkt:                  bkt,
		labelName:            labelName,
		consistencyDelay:     consistencyDelay,
		blockSyncConcurrency: blockSyncConcurrency,
	}
}

func (s *ReplicaSyncer) Sync(ctx context.Context) (Replicas, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	var wg sync.WaitGroup
	defer wg.Wait()

	blocks := make(map[ulid.ULID]*metadata.Meta)
	metaIdsChan := make(chan ulid.ULID)
	errChan := make(chan error, s.blockSyncConcurrency)

	syncCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := 0; i < s.blockSyncConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range metaIdsChan {
				s.blocksMtx.Lock()
				_, seen := blocks[id]
				s.blocksMtx.Unlock()
				if seen {
					continue
				}
				s.metrics.syncMetas.WithLabelValues(s.bkt.Name()).Inc()
				begin := time.Now()
				meta, err := s.download(syncCtx, id)
				s.metrics.syncMetaDuration.WithLabelValues(s.bkt.Name()).Observe(time.Since(begin).Seconds())
				if err != nil {
					errChan <- err
					s.metrics.syncMetaFailures.WithLabelValues(s.bkt.Name(), id.String()).Inc()
					return
				}
				if meta == nil {
					continue
				}
				s.blocksMtx.Lock()
				blocks[id] = meta
				s.blocksMtx.Unlock()
			}
		}()
	}

	remote := map[ulid.ULID]struct{}{}

	err := s.bkt.Iter(ctx, "", func(name string) error {
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}

		remote[id] = struct{}{}

		select {
		case <-ctx.Done():
		case metaIdsChan <- id:
		}
		return nil
	})

	close(metaIdsChan)

	if err != nil {
		return nil, compact.Retry(errors.Wrapf(err, "sync block metas from bucket %s", s.bkt.Name()))
	}

	wg.Wait()
	close(errChan)

	if err := <-errChan; err != nil {
		return nil, compact.Retry(errors.Wrapf(err, "download block metas from bucket %s", s.bkt.Name()))
	}

	result := make([]*metadata.Meta, 0)
	for id, b := range blocks {
		if _, ok := remote[id]; ok {
			result = append(result, b)
		}
	}
	return NewReplicas(s.labelName, result)
}

func (s *ReplicaSyncer) download(ctx context.Context, id ulid.ULID) (*metadata.Meta, error) {
	meta, err := block.DownloadMeta(ctx, s.logger, s.bkt, id)
	if err != nil {
		s.metrics.operateRemoteStorageFailures.WithLabelValues("get", s.bkt.Name(), id.String()).Inc()
		return nil, compact.Retry(errors.Wrapf(err, "downloading block meta %s", id))
	}
	if ulid.Now()-id.Time() < uint64(s.consistencyDelay/time.Millisecond) {
		level.Debug(s.logger).Log("msg", "block is too fresh for now", "consistency-delay", s.consistencyDelay, "block", id)
		return nil, nil
	}
	level.Debug(s.logger).Log("msg", "downloaded block meta", "block", id)
	return &meta, nil
}
