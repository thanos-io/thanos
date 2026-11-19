// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compliance

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/exp/api/remote"
	"github.com/prometheus/compliance/remotewrite/sender"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"google.golang.org/grpc"
)

type internalThanosRuleSender struct {
	binary string
}

func (internalThanosRuleSender) Name() string { return "internal-thanos-rule" }

func (r internalThanosRuleSender) Run(ctx context.Context, opts sender.Options) error {
	dir, err := os.MkdirTemp("", "test-thanos-rule-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	dataDir := filepath.Join(dir, "data")
	// NOTE: Pre-create the initial WAL segment so remoteStore.ApplyConfig can tail
	// segment 00000000 immediately on startup without waiting for the 5s WAL watcher retry.
	w, err := wlog.New(nil, nil, filepath.Join(dataDir, "wal"), compression.Snappy)
	if err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	srv := &scrapeQueryServer{opts: opts}
	infopb.RegisterInfoServer(grpcSrv, srv)
	querypb.RegisterQueryServer(grpcSrv, srv)

	go func() {
		_ = grpcSrv.Serve(lis)
	}()
	defer grpcSrv.Stop()

	ruleFile := filepath.Join(dir, "rules.yaml")
	const ruleContent = `groups:
  - name: compliance
    interval: 1s
    rules:
      - record: compliance:recording_rule
        expr: vector(1)
`
	if err := os.WriteFile(ruleFile, []byte(ruleContent), 0600); err != nil {
		return err
	}

	protoMsg := opts.RemoteWriteMessage
	if protoMsg == "" {
		protoMsg = remote.WriteV2MessageType
	}
	rwConfigFile := filepath.Join(dir, "rw.yaml")
	rwConfig := fmt.Sprintf(`remote_write:
  - url: %s
    protobuf_message: %s
    send_exemplars: true
    send_native_histograms: true
    queue_config:
      batch_send_deadline: 100ms
      min_backoff: 20ms
      max_backoff: 100ms
      retry_on_http_429: true
    write_relabel_configs:
      - regex: __tmp_orig_name(__name__)
        replacement: $1
        action: labelmap
      - regex: __tmp_orig_name__name__
        action: labeldrop
`, opts.RemoteWriteEndpointURL, protoMsg)
	if err := os.WriteFile(rwConfigFile, []byte(rwConfig), 0600); err != nil {
		return err
	}

	return sender.RunCommand(ctx, ".", nil,
		r.binary,
		"rule",
		"--http-address=127.0.0.1:0",
		"--grpc-address=127.0.0.1:0",
		fmt.Sprintf("--data-dir=%s", dataDir),
		fmt.Sprintf("--rule-file=%s", ruleFile),
		"--eval-interval=1s",
		fmt.Sprintf("--grpc-query-endpoint=%s", lis.Addr().String()),
		fmt.Sprintf("--remote-write.config-file=%s", rwConfigFile),
		`--label=replica="0"`,
	)
}

var _ sender.Sender = internalThanosRuleSender{}

type scrapeQueryServer struct {
	infopb.UnimplementedInfoServer
	querypb.UnimplementedQueryServer
	opts sender.Options
}

func (s *scrapeQueryServer) Info(context.Context, *infopb.InfoRequest) (*infopb.InfoResponse, error) {
	return &infopb.InfoResponse{
		ComponentType: "query",
		Query:         &infopb.QueryAPIInfo{},
	}, nil
}

func (s *scrapeQueryServer) Query(req *querypb.QueryRequest, srv querypb.Query_QueryServer) error {
	httpReq, err := http.NewRequestWithContext(srv.Context(), http.MethodGet, fmt.Sprintf("http://%s/metrics", s.opts.ScrapeTargetHostPort), nil)
	if err != nil {
		return err
	}
	httpReq.Header.Set("Accept", "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.7,application/openmetrics-text;version=1.0.0;escaping=allow-utf-8;q=0.6,application/openmetrics-text;version=0.0.1;q=0.5,text/plain;version=1.0.0;escaping=allow-utf-8;q=0.4,text/plain;version=0.0.4;q=0.3,*/*;q=0.2")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected scrape status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	p, err := textparse.New(body, resp.Header.Get("Content-Type"), labels.NewSymbolTable(), textparse.ParserOptions{})
	if err != nil {
		return err
	}

	tsMillis := req.TimeSeconds * 1000
	for {
		et, err := p.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		switch et {
		case textparse.EntrySeries:
			_, parsedTS, val := p.Series()
			sampleTS := tsMillis
			if parsedTS != nil {
				sampleTS = *parsedTS
			}
			var lset labels.Labels
			p.Labels(&lset)
			zlabels := buildRuleQueryLabels(lset, s.opts.ScrapeTargetJobName, s.opts.ScrapeTargetHostPort)
			if err := srv.Send(querypb.NewQueryResponse(&prompb.TimeSeries{
				Labels:  zlabels,
				Samples: []prompb.Sample{{Value: val, Timestamp: sampleTS}},
			})); err != nil {
				return err
			}
		case textparse.EntryHistogram:
			_, parsedTS, h, fh := p.Histogram()
			sampleTS := tsMillis
			if parsedTS != nil {
				sampleTS = *parsedTS
			}
			var lset labels.Labels
			p.Labels(&lset)
			zlabels := buildRuleQueryLabels(lset, s.opts.ScrapeTargetJobName, s.opts.ScrapeTargetHostPort)
			var protoHist prompb.Histogram
			if h != nil {
				protoHist = prompb.HistogramToHistogramProto(sampleTS, h)
			} else {
				protoHist = prompb.FloatHistogramToHistogramProto(sampleTS, fh)
			}
			if err := srv.Send(querypb.NewQueryResponse(&prompb.TimeSeries{
				Labels:     zlabels,
				Histograms: []prompb.Histogram{protoHist},
			})); err != nil {
				return err
			}
		}
	}
	return nil
}

func buildRuleQueryLabels(lset labels.Labels, job, instance string) []labelpb.ZLabel {
	lb := labels.NewBuilder(lset)
	lset.Range(func(l labels.Label) {
		if l.Value == "" {
			lb.Del(l.Name)
		}
	})
	// NOTE: RecordingRule.Eval overwrites __name__ with the recording rule name, so stash
	// the scraped metric name here and restore it via write_relabel_configs before sending.
	lb.Set("__tmp_orig_name__name__", lset.Get(labels.MetricName))

	if prevJob := lset.Get("job"); prevJob != "" {
		lb.Set("exported_job", prevJob)
	}
	if job != "" {
		lb.Set("job", job)
	}
	if prevInst := lset.Get("instance"); prevInst != "" {
		lb.Set("exported_instance", prevInst)
	}
	if instance != "" {
		lb.Set("instance", instance)
	}

	finalLabels := lb.Labels()
	res := make([]labelpb.ZLabel, 0, finalLabels.Len())
	finalLabels.Range(func(l labels.Label) {
		res = append(res, labelpb.ZLabel{Name: l.Name, Value: l.Value})
	})
	return res
}

// NOTE: Keep patterns ordered by slash count ascending so longer wildcard patterns do not
// return a partial match in testing.alternationMatch before shorter exact patterns are checked.
var senderSkips = []string{
	// 4 segments: Thanos Ruler evaluates instant queries at a single evaluation timestamp per cycle
	// and does not emit exemplars from rule evaluations.
	`^TestRemoteWriteSender/internal-thanos-rule/(samples_sorted|native_histograms_ordering|exemplars)/rw2$`,
	// 5 segments: Recording/alerting rules in Thanos Ruler produce gauge samples or float histograms
	// without start timestamps, integer histogram buckets, or scraped metric family metadata.
	`^TestRemoteWriteSender/internal-thanos-rule/samples/rw2/start_timestamp_for_(counters|histograms|summaries)$`,
	`^TestRemoteWriteSender/internal-thanos-rule/native_histograms/rw2/(positive_spans_and_deltas|negative_spans_and_deltas|zero_bucket)$`,
	`^TestRemoteWriteSender/internal-thanos-rule/metadata/rw2/type_(counter|gauge_and_unit|histogram|summary|info_and_stateset)$`,
}

func TestRemoteWriteSender(t *testing.T) {
	sender.RunTests(
		t,
		internalThanosRuleSender{binary: buildThanos(t)},
		sender.ComplianceTests(),
	)
}
