// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/compress/s2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/server/http/middleware"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/writev2pb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

func (h *Handler) storeV2(ctx context.Context, tLogger log.Logger, w http.ResponseWriter, r *http.Request, tenantHTTP string) {
	var err error
	requestLimiter := h.Limiter.RequestLimiter()
	// io.ReadAll dynamically adjust the byte slice for read data, starting from 512B.
	// Since this is receive hot path, grow upfront saving allocations and CPU time.
	compressed := bytes.Buffer{}
	if r.ContentLength >= 0 {
		if !requestLimiter.AllowSizeBytes(tenantHTTP, r.ContentLength) {
			http.Error(w, "write request too large", http.StatusRequestEntityTooLarge)
			return
		}
		compressed.Grow(int(r.ContentLength))
	} else {
		compressed.Grow(512)
	}
	_, err = io.Copy(&compressed, r.Body)
	if err != nil {
		http.Error(w, errors.Wrap(err, "read compressed request body").Error(), http.StatusInternalServerError)
		return
	}
	reqBuf, err := s2.Decode(nil, compressed.Bytes())
	if err != nil {
		level.Error(tLogger).Log("msg", "snappy decode error", "err", err)
		http.Error(w, errors.Wrap(err, "snappy decode error").Error(), http.StatusBadRequest)
		return
	}

	if !requestLimiter.AllowSizeBytes(tenantHTTP, int64(len(reqBuf))) {
		http.Error(w, "write request too large", http.StatusRequestEntityTooLarge)
		return
	}

	var wreq writev2pb.Request
	if err := proto.Unmarshal(reqBuf, &wreq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rep := uint64(0)
	// If the header is empty, we assume the request is not yet replicated.
	if replicaRaw := r.Header.Get(h.options.ReplicaHeader); replicaRaw != "" {
		if rep, err = strconv.ParseUint(replicaRaw, 10, 64); err != nil {
			http.Error(w, "could not parse replica header", http.StatusBadRequest)
			return
		}
	}

	if !requestLimiter.AllowSeries(tenantHTTP, int64(len(wreq.Timeseries))) {
		http.Error(w, "too many timeseries", http.StatusRequestEntityTooLarge)
		return
	}

	totalSamples := 0
	for _, timeseries := range wreq.Timeseries {
		totalSamples += len(timeseries.Samples)
	}

	if !requestLimiter.AllowSamples(tenantHTTP, int64(totalSamples)) {
		http.Error(w, "too many samples", http.StatusRequestEntityTooLarge)
		return
	}

	responseStatusCode := http.StatusOK
	tenantStats, err := h.handleRequestV2(ctx, tLogger, rep, &wreq, tenantHTTP)
	if err != nil {
		level.Debug(tLogger).Log("msg", "failed to handle request", "err", err.Error())
		switch errors.Cause(err) {
		case errNotReady:
			responseStatusCode = http.StatusServiceUnavailable
		case errUnavailable:
			responseStatusCode = http.StatusServiceUnavailable
		case errConflict:
			responseStatusCode = http.StatusConflict
		case errBadReplica:
			responseStatusCode = http.StatusBadRequest
		default:
			level.Error(tLogger).Log("err", err, "msg", "internal server error")
			responseStatusCode = http.StatusInternalServerError
		}
		http.Error(w, err.Error(), responseStatusCode)
	}

	for tenant, stats := range tenantStats {
		w.Header().Set(writtenSamplesHeader, strconv.Itoa(stats.totalSamples))
		w.Header().Set(writtenHistogramsHeader, strconv.Itoa(stats.totalHistograms))
		w.Header().Set(writtenExemplarsHeader, strconv.Itoa(stats.totalExemplars))
		h.writeTimeseriesTotal.WithLabelValues(strconv.Itoa(responseStatusCode), tenant).Observe(float64(stats.timeseries))
		h.writeSamplesTotal.WithLabelValues(strconv.Itoa(responseStatusCode), tenant).Observe(float64(stats.totalSamples))
	}
}

func (h *Handler) handleRequestV2(ctx context.Context, tLogger log.Logger, rep uint64, wreq *writev2pb.Request, tenantHTTP string) (tenantRequestStats, error) {
	symbolTable, tenantWreqs := h.relabelAndSplitTenant(wreq, tenantHTTP)
	if len(wreq.Timeseries) == 0 {
		level.Debug(tLogger).Log("msg", "remote write request dropped due to relabeling.")
		return tenantRequestStats{}, nil
	}

	// This replica value is used to detect cycles in cyclic topologies.
	// A non-zero value indicates that the request has already been replicated by a previous receive instance.
	// For almost all users, this is only used in fully connected topologies of IngestorRouter instances.
	// For acyclic topologies that use RouterOnly and IngestorOnly instances, this causes issues when replicating data.
	// See discussion in: https://github.com/thanos-io/thanos/issues/4359.
	if h.receiverMode == RouterOnly || h.receiverMode == IngestorOnly {
		rep = 0
	}

	// The replica value in the header is one-indexed, thus we need >.
	if rep > h.options.ReplicationFactor {
		level.Error(tLogger).Log("err", errBadReplica, "msg", "write request rejected",
			"request_replica", rep, "replication_factor", h.options.ReplicationFactor)
		return tenantRequestStats{}, errBadReplica
	}

	r := replica{n: rep, replicated: rep != 0}

	// On the wire, format is 1-indexed and in-code is 0-indexed, so we decrement the value if it was already replicated.
	if r.replicated {
		r.n--
	}

	// Forward any time series as necessary. All time series
	// destined for the local node will be written to the receiver.
	// Time series will be replicated as necessary.
	return h.fanoutForwardV2(ctx, r, symbolTable, tenantWreqs)
}

func (h *Handler) gatherWriteStatsV2(rf int, writes ...map[endpointReplica]map[string]trackedV2Series) tenantRequestStats {
	stats := make(tenantRequestStats)
	for _, write := range writes {
		for er := range write {
			for tenant, series := range write[er] {
				samples := 0
				exemplars := 0
				histograms := 0

				for _, ts := range series.timeSeries {
					samples += len(ts.Samples)
					exemplars += len(ts.Exemplars)
					histograms += len(ts.Histograms)
				}

				if st, ok := stats[tenant]; ok {
					st.timeseries += len(series.timeSeries)
					st.totalSamples += samples
					st.totalExemplars += exemplars
					st.totalHistograms += histograms

					stats[tenant] = st
				} else {
					stats[tenant] = requestStats{
						timeseries:      len(series.timeSeries),
						totalSamples:    samples,
						totalExemplars:  exemplars,
						totalHistograms: histograms,
					}
				}
			}
		}
	}

	// adjust counters by the replication factor
	for tenant, st := range stats {
		st.timeseries /= rf
		st.totalSamples /= rf
		st.totalExemplars /= rf
		st.totalHistograms /= rf
		stats[tenant] = st
	}

	return stats
}

func (h *Handler) fanoutForwardV2(ctx context.Context, r replica, symbolTable *writev2pb.SymbolsTable, tenantWreqs tenantWreq) (tenantRequestStats, error) {
	span, ctx := tracing.StartSpan(ctx, "receive_fanout_forward")
	defer span.Finish()

	var replicas []uint64
	if r.replicated {
		replicas = []uint64{r.n}
	} else {
		for rn := uint64(0); rn < h.options.ReplicationFactor; rn++ {
			replicas = append(replicas, rn)
		}
	}

	ctx, cancel := context.WithTimeout(tracing.CopyTraceContext(context.Background(), ctx), h.options.ForwardTimeout)

	var writeErrors writeErrors

	defer func() {
		if writeErrors.ErrOrNil() != nil {
			// NOTICE: The cancel function is not used on all paths intentionally,
			// if there is no error when quorum is reached,
			// let forward requests to optimistically run until timeout.
			cancel()
		}
	}()

	logTags := []interface{}{}
	for tenant := range tenantWreqs {
		logTags = append(logTags, "tenant", tenant)
	}

	if id, ok := middleware.RequestIDFromContext(ctx); ok {
		logTags = append(logTags, "request-id", id)
	}
	requestLogger := log.With(h.logger, logTags...)

	localWrites, remoteWrites, err := h.distributeTimeseriesToReplicasV2(symbolTable, tenantWreqs, replicas)
	if err != nil {
		level.Error(requestLogger).Log("msg", "failed to distribute timeseries to replicas", "err", err)
		return tenantRequestStats{}, err
	}

	stats := h.gatherWriteStatsV2(len(replicas), localWrites, remoteWrites)

	// Prepare a buffered channel to receive the responses from the local and remote writes. Remote writes will all go
	// asynchronously and with this capacity we will never block on writing to the channel.
	var maxBufferedResponses int
	for er := range localWrites {
		maxBufferedResponses += len(localWrites[er])
	}
	for er := range remoteWrites {
		maxBufferedResponses += len(remoteWrites[er])
	}

	responses := make(chan writeResponse, maxBufferedResponses)
	wg := sync.WaitGroup{}

	h.sendWritesV2(ctx, &wg, r.replicated, symbolTable, localWrites, remoteWrites, responses)

	go func() {
		wg.Wait()
		close(responses)
	}()

	// At the end, make sure to exhaust the channel, letting remaining unnecessary requests finish asynchronously.
	// This is needed if context is canceled or if we reached success of fail quorum faster.
	defer func() {
		go func() {
			for resp := range responses {
				if resp.err != nil {
					level.Debug(requestLogger).Log("msg", "request failed, but not needed to achieve quorum", "err", resp.err)
				}
			}
		}()
	}()

	quorum := h.writeQuorum()
	if r.replicated {
		quorum = 1
	}

	totalSeries := 0
	for _, timeseries := range tenantWreqs {
		totalSeries += len(timeseries)
	}
	successes := make([]int, totalSeries)
	seriesErrs := newReplicationErrors(quorum, totalSeries)
	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case resp, hasMore := <-responses:
			if !hasMore {
				for _, seriesErr := range seriesErrs {
					writeErrors.Add(seriesErr)
				}
				return stats, writeErrors.ErrOrNil()
			}

			if resp.err != nil {
				// Track errors and successes on a per-series basis.
				for _, seriesID := range resp.seriesIDs {
					seriesErrs[seriesID].Add(resp.err)
				}

				continue
			}
			// At the end, aggregate all errors if there are any and return them.
			for _, seriesID := range resp.seriesIDs {
				successes[seriesID]++
			}
			if quorumReached(successes, quorum) {
				return stats, nil
			}
		}
	}
}

type trackedV2Series struct {
	timeSeries []writev2pb.TimeSeries
	seriesIDs  []int
}

// distributeTimeseriesToReplicas distributes the given timeseries from the tenant to different endpoints in a manner
// that achieves the replication factor indicated by replicas.
// The first return value are the series that should be written to the local node. The second return value are the
// series that should be written to remote nodes.
func (h *Handler) distributeTimeseriesToReplicasV2(
	symbolTable *writev2pb.SymbolsTable,
	tenantWreqs tenantWreq,
	replicas []uint64,
) (map[endpointReplica]map[string]trackedV2Series, map[endpointReplica]map[string]trackedV2Series, error) {
	h.mtx.RLock()
	defer h.mtx.RUnlock()
	remoteWrites := make(map[endpointReplica]map[string]trackedV2Series)
	localWrites := make(map[endpointReplica]map[string]trackedV2Series)

	b := labels.NewScratchBuilder(0)

	tsIndex := 0
	for tenant, timeseries := range tenantWreqs {
		for _, ts := range timeseries {
			for _, rn := range replicas {
				endpoint, err := h.hashring.GetN(tenant, labelpb.ZLabelsFromPromLabels(writev2pb.DesymbolizeLabels(&b, ts.LabelsRefs, symbolTable.Symbols())), rn)
				if err != nil {
					return nil, nil, err
				}
				endpointReplica := endpointReplica{endpoint: endpoint, replica: rn}
				var writeDestination = remoteWrites
				if endpoint.HasAddress(h.options.Endpoint) {
					writeDestination = localWrites
				}
				writeableSeries, ok := writeDestination[endpointReplica]
				if !ok {
					writeDestination[endpointReplica] = map[string]trackedV2Series{
						tenant: {
							timeSeries: make([]writev2pb.TimeSeries, 0),
							seriesIDs:  make([]int, 0),
						},
					}
				}
				tenantSeries := writeableSeries[tenant]
				tenantSeries.timeSeries = append(tenantSeries.timeSeries, ts)
				tenantSeries.seriesIDs = append(tenantSeries.seriesIDs, tsIndex)
				writeDestination[endpointReplica][tenant] = tenantSeries
			}
			tsIndex++
		}
	}

	return localWrites, remoteWrites, nil
}

// sendWrites sends the local and remote writes to execute concurrently, controlling them through the provided sync.WaitGroup.
// The responses from the writes are sent to the responses channel.
func (h *Handler) sendWritesV2(
	ctx context.Context,
	wg *sync.WaitGroup,
	replicated bool,
	symbolTable *writev2pb.SymbolsTable,
	localWrites map[endpointReplica]map[string]trackedV2Series,
	remoteWrites map[endpointReplica]map[string]trackedV2Series,
	responses chan writeResponse,
) {
	// Do the writes to the local node first. This should be easy and fast.
	for writeDestination := range localWrites {
		func(writeDestination endpointReplica) {
			for tenant, trackedSeries := range localWrites[writeDestination] {
				h.sendLocalWriteV2(ctx, symbolTable, writeDestination, tenant, trackedSeries, responses)
			}
		}(writeDestination)
	}

	// Do the writes to remote nodes. Run them all in parallel.
	for writeDestination := range remoteWrites {
		for tenant, trackedSeries := range remoteWrites[writeDestination] {
			wg.Add(1)
			h.sendRemoteWriteV2(ctx, tenant, writeDestination, symbolTable, trackedSeries, replicated, responses, wg)
		}
	}
}

// sendLocalWrite sends a write request to the local node.
// The responses are sent to the responses channel.
func (h *Handler) sendLocalWriteV2(
	ctx context.Context,
	symbolTable *writev2pb.SymbolsTable,
	writeDestination endpointReplica,
	tenantHTTP string,
	trackedSeries trackedV2Series,
	responses chan<- writeResponse,
) {
	span, tracingCtx := tracing.StartSpan(ctx, "receive_local_tsdb_write")
	defer span.Finish()
	span.SetTag("endpoint", writeDestination.endpoint)
	span.SetTag("replica", writeDestination.replica)

	err := h.writer.WriteV2(tracingCtx, tenantHTTP, symbolTable, trackedSeries.timeSeries)
	if err != nil {
		span.SetTag("error", true)
		span.SetTag("error.msg", err.Error())
		responses <- newWriteResponse(trackedSeries.seriesIDs, err, writeDestination)
		return
	}

	responses <- newWriteResponse(trackedSeries.seriesIDs, nil, writeDestination)

}

// sendRemoteWrite sends a write request to the remote node. It takes care of checking whether the endpoint is up or not
// in the peerGroup, correctly marking them as up or down when appropriate.
// The responses are sent to the responses channel.
func (h *Handler) sendRemoteWriteV2(
	ctx context.Context,
	tenant string,
	endpointReplica endpointReplica,
	symbolTable *writev2pb.SymbolsTable,
	trackedSeries trackedV2Series,
	alreadyReplicated bool,
	responses chan writeResponse,
	wg *sync.WaitGroup,
) {
	endpoint := endpointReplica.endpoint
	cl, err := h.peers.getConnection(ctx, endpoint)
	if err != nil {
		if errors.Is(err, errUnavailable) {
			err = errors.Wrapf(errUnavailable, "backing off forward request for endpoint %v", endpointReplica)
		}
		responses <- newWriteResponse(trackedSeries.seriesIDs, err, endpointReplica)
		wg.Done()
		return
	}

	// This is called "real" because it's 1-indexed.
	realReplicationIndex := int64(endpointReplica.replica + 1)
	// Actually make the request against the endpoint we determined should handle these time series.
	cl.RemoteWriteAsyncV2(ctx, &storepb.WriteRequestV2{
		Timeseries: trackedSeries.timeSeries,
		Tenant:     tenant,
		Symbols:    symbolTable.Symbols(),
		// Increment replica since on-the-wire format is 1-indexed and 0 indicates un-replicated.
		Replica: realReplicationIndex,
	}, endpointReplica, trackedSeries.seriesIDs, responses, func(err error) {
		if err == nil {
			h.forwardRequests.WithLabelValues(labelSuccess).Inc()
			if !alreadyReplicated {
				h.replications.WithLabelValues(labelSuccess).Inc()
			}
			h.peers.markPeerAvailable(endpoint)
		} else {
			// Check if peer connection is unavailable, update the peer state to avoid spamming that peer.
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Unavailable {
					h.peers.markPeerUnavailable(endpointReplica.endpoint)
				}
			}
		}
		wg.Done()
	})
}

type tenantWreq map[string][]writev2pb.TimeSeries

// relabel relabels the time series labels in the remote write request.
// It also splits the tenant label if the splitTenantLabelName is set.
// This is the point where we construct a new symbol table for the request, to reflect changes.
// The original symbol table is dropped.
func (h *Handler) relabelAndSplitTenant(wreq *writev2pb.Request, tenantHTTP string) (*writev2pb.SymbolsTable, tenantWreq) {
	tenantWreqs := make(tenantWreq)
	b := labels.NewScratchBuilder(0)
	for _, ts := range wreq.Timeseries {
		tenant := tenantHTTP

		if h.splitTenantLabelName != "" {
			lbls := writev2pb.DesymbolizeLabels(&b, ts.LabelsRefs, wreq.Symbols)
			tenantLabel := lbls.Get(h.splitTenantLabelName)
			if tenantLabel != "" {
				tenant = tenantLabel
			}
		}

		tenantWreqs[tenant] = append(tenantWreqs[tenant], ts)
	}

	if h.splitTenantLabelName != "" {
		h.options.RelabelConfigs = append(h.options.RelabelConfigs, &relabel.Config{
			Action: relabel.LabelDrop,
			Regex:  relabel.MustNewRegexp(h.splitTenantLabelName),
		})
	}

	if len(h.options.RelabelConfigs) == 0 {
		st := writev2pb.NewSymbolTableFromSymbols(wreq.Symbols)
		wreq.Symbols = wreq.Symbols[:0]
		return st, tenantWreqs
	}

	st := writev2pb.NewSymbolTable()
	relabelledTenantWreqs := make(tenantWreq)

	b = labels.NewScratchBuilder(0)
	buf := make([]uint32, 0, len(wreq.Symbols)*2)

	for tenant, timeseries := range tenantWreqs {
		relabelledTimeseries := make([]writev2pb.TimeSeries, 0)
		for _, ts := range timeseries {
			lbls, keep := relabel.Process(writev2pb.DesymbolizeLabels(&b, ts.LabelsRefs, wreq.Symbols), h.options.RelabelConfigs...)
			if !keep {
				continue
			}

			ts.LabelsRefs = st.SymbolizeLabels(lbls, buf)
			ts.Metadata.HelpRef, ts.Metadata.UnitRef = st.SymbolizeMetadata(wreq.Symbols[ts.Metadata.HelpRef], wreq.Symbols[ts.Metadata.UnitRef])
			relabelledTimeseries = append(relabelledTimeseries, ts)
		}

		if len(relabelledTimeseries) > 0 {
			relabelledTenantWreqs[tenant] = relabelledTimeseries
		}
	}

	wreq.Symbols = wreq.Symbols[:0]
	return st, relabelledTenantWreqs
}
