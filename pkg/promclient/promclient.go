// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package promclient offers helper client function for various API endpoints.

package promclient

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	promlabels "github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/prometheus/promql"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
	yaml "gopkg.in/yaml.v2"
)

var ErrFlagEndpointNotFound = errors.New("no flag endpoint found")

// IsWALDirAccessible returns no error if WAL dir can be found. This helps to tell
// if we have access to Prometheus TSDB directory.
func IsWALDirAccessible(dir string) error {
	const errMsg = "WAL dir is not accessible. Is this dir a TSDB directory? If yes it is shared with TSDB?"

	f, err := os.Stat(filepath.Join(dir, "wal"))
	if err != nil {
		return errors.Wrap(err, errMsg)
	}

	if !f.IsDir() {
		return errors.New(errMsg)
	}

	return nil
}

// ExternalLabels returns external labels from /api/v1/status/config Prometheus endpoint.
// Note that configuration can be hot reloadable on Prometheus, so this config might change in runtime.
func ExternalLabels(ctx context.Context, logger log.Logger, base *url.URL) (labels.Labels, error) {
	u := *base
	u.Path = path.Join(u.Path, "/api/v1/status/config")

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "create request")
	}
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrapf(err, "request flags against %s", u.String())
	}
	defer runutil.ExhaustCloseWithLogOnErr(logger, resp.Body, "query body")

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("failed to read body")
	}

	if resp.StatusCode != 200 {
		return nil, errors.Errorf("got non-200 response code: %v, response: %v", resp.StatusCode, string(b))
	}

	var d struct {
		Data struct {
			YAML string `json:"yaml"`
		} `json:"data"`
	}
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, errors.Wrapf(err, "unmarshal response: %v", string(b))
	}
	var cfg struct {
		Global struct {
			ExternalLabels map[string]string `yaml:"external_labels"`
		} `yaml:"global"`
	}
	if err := yaml.Unmarshal([]byte(d.Data.YAML), &cfg); err != nil {
		return nil, errors.Wrapf(err, "parse Prometheus config: %v", d.Data.YAML)
	}
	return labels.FromMap(cfg.Global.ExternalLabels), nil
}

type Flags struct {
	TSDBPath           string         `json:"storage.tsdb.path"`
	TSDBRetention      model.Duration `json:"storage.tsdb.retention"`
	TSDBMinTime        model.Duration `json:"storage.tsdb.min-block-duration"`
	TSDBMaxTime        model.Duration `json:"storage.tsdb.max-block-duration"`
	WebEnableAdminAPI  bool           `json:"web.enable-admin-api"`
	WebEnableLifecycle bool           `json:"web.enable-lifecycle"`
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (f *Flags) UnmarshalJSON(b []byte) error {
	// TODO(bwplotka): Avoid this custom unmarshal by:
	// - prometheus/common: adding unmarshalJSON to modelDuration
	// - prometheus/prometheus: flags should return proper JSON (not bool in string).
	parsableFlags := struct {
		TSDBPath           string        `json:"storage.tsdb.path"`
		TSDBRetention      modelDuration `json:"storage.tsdb.retention"`
		TSDBMinTime        modelDuration `json:"storage.tsdb.min-block-duration"`
		TSDBMaxTime        modelDuration `json:"storage.tsdb.max-block-duration"`
		WebEnableAdminAPI  modelBool     `json:"web.enable-admin-api"`
		WebEnableLifecycle modelBool     `json:"web.enable-lifecycle"`
	}{}

	if err := json.Unmarshal(b, &parsableFlags); err != nil {
		return err
	}

	*f = Flags{
		TSDBPath:           parsableFlags.TSDBPath,
		TSDBRetention:      model.Duration(parsableFlags.TSDBRetention),
		TSDBMinTime:        model.Duration(parsableFlags.TSDBMinTime),
		TSDBMaxTime:        model.Duration(parsableFlags.TSDBMaxTime),
		WebEnableAdminAPI:  bool(parsableFlags.WebEnableAdminAPI),
		WebEnableLifecycle: bool(parsableFlags.WebEnableLifecycle),
	}
	return nil
}

type modelDuration model.Duration

// UnmarshalJSON implements the json.Unmarshaler interface.
func (d *modelDuration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	dur, err := model.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = modelDuration(dur)
	return nil
}

type modelBool bool

// UnmarshalJSON implements the json.Unmarshaler interface.
func (m *modelBool) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	boolean, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	*m = modelBool(boolean)
	return nil
}

// ConfiguredFlags returns configured flags from /api/v1/status/flags Prometheus endpoint.
// Added to Prometheus from v2.2.
func ConfiguredFlags(ctx context.Context, logger log.Logger, base *url.URL) (Flags, error) {
	u := *base
	u.Path = path.Join(u.Path, "/api/v1/status/flags")

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return Flags{}, errors.Wrap(err, "create request")
	}

	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return Flags{}, errors.Wrapf(err, "request config against %s", u.String())
	}
	defer runutil.ExhaustCloseWithLogOnErr(logger, resp.Body, "query body")

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return Flags{}, errors.New("failed to read body")
	}

	switch resp.StatusCode {
	case 404:
		return Flags{}, ErrFlagEndpointNotFound
	case 200:
		var d struct {
			Data Flags `json:"data"`
		}

		if err := json.Unmarshal(b, &d); err != nil {
			return Flags{}, errors.Wrapf(err, "unmarshal response: %v", string(b))
		}

		return d.Data, nil
	default:
		return Flags{}, errors.Errorf("got non-200 response code: %v, response: %v", resp.StatusCode, string(b))
	}

}

// Snapshot will request Prometheus to perform snapshot in directory returned by this function.
// Returned directory is relative to Prometheus data-dir.
// NOTE: `--web.enable-admin-api` flag has to be set on Prometheus.
// Added to Prometheus from v2.1.
// TODO(bwplotka): Add metrics.
func Snapshot(ctx context.Context, logger log.Logger, base *url.URL, skipHead bool) (string, error) {
	u := *base
	u.Path = path.Join(u.Path, "/api/v1/admin/tsdb/snapshot")

	req, err := http.NewRequest(
		http.MethodPost,
		u.String(),
		strings.NewReader(url.Values{"skip_head": []string{strconv.FormatBool(skipHead)}}.Encode()),
	)
	if err != nil {
		return "", errors.Wrap(err, "create request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return "", errors.Wrapf(err, "request snapshot against %s", u.String())
	}
	defer runutil.ExhaustCloseWithLogOnErr(logger, resp.Body, "query body")

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.New("failed to read body")
	}

	if resp.StatusCode != 200 {
		return "", errors.Errorf("is 'web.enable-admin-api' flag enabled? got non-200 response code: %v, response: %v", resp.StatusCode, string(b))
	}

	var d struct {
		Data struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := json.Unmarshal(b, &d); err != nil {
		return "", errors.Wrapf(err, "unmarshal response: %v", string(b))
	}

	return path.Join("snapshots", d.Data.Name), nil
}

type QueryOptions struct {
	Deduplicate             bool
	PartialResponseStrategy storepb.PartialResponseStrategy
}

func (p *QueryOptions) AddTo(values url.Values) error {
	values.Add("dedup", fmt.Sprintf("%v", p.Deduplicate))

	var partialResponseValue string
	switch p.PartialResponseStrategy {
	case storepb.PartialResponseStrategy_WARN:
		partialResponseValue = strconv.FormatBool(true)
	case storepb.PartialResponseStrategy_ABORT:
		partialResponseValue = strconv.FormatBool(false)
	default:
		return errors.Errorf("unknown partial response strategy %v", p.PartialResponseStrategy)
	}

	// TODO(bwplotka): Apply change from bool to strategy in Query API as well.
	values.Add("partial_response", partialResponseValue)

	return nil
}

// HTTPClient sends an HTTP request and returns the response.
type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

// Client represents a Prometheus API client.
type Client struct {
	httpClient HTTPClient
	logger     log.Logger
}

// NewClient returns a new Prometheus API client.
func NewClient(logger log.Logger, c HTTPClient) *Client {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Client{
		httpClient: c,
		logger:     logger,
	}
}

func defaultClient(logger log.Logger) *Client {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return NewClient(
		logger,
		&http.Client{
			Transport: tracing.HTTPTripperware(logger, http.DefaultTransport),
		},
	)
}

// QueryInstant performs an instant query and returns results in model.Vector type.
func (c *Client) QueryInstant(ctx context.Context, base *url.URL, query string, t time.Time, opts QueryOptions) (model.Vector, []string, error) {
	params, err := url.ParseQuery(base.RawQuery)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "parse raw query %s", base.RawQuery)
	}
	params.Add("query", query)
	params.Add("time", t.Format(time.RFC3339Nano))
	if err := opts.AddTo(params); err != nil {
		return nil, nil, errors.Wrap(err, "add thanos opts query params")
	}

	u := *base
	u.Path = path.Join(u.Path, "/api/v1/query")
	u.RawQuery = params.Encode()

	level.Debug(c.logger).Log("msg", "querying instant", "url", u.String())

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, nil, errors.Wrap(err, "create GET request")
	}

	req = req.WithContext(ctx)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "perform GET request against %s", u.String())
	}
	defer runutil.ExhaustCloseWithLogOnErr(c.logger, resp.Body, "query body")

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, errors.Wrap(err, "read query instant response")
	}

	// Decode only ResultType and load Result only as RawJson since we don't know
	// structure of the Result yet.
	var m struct {
		Data struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`

		Error     string `json:"error,omitempty"`
		ErrorType string `json:"errorType,omitempty"`
		// Extra field supported by Thanos Querier.
		Warnings []string `json:"warnings"`
	}

	if err = json.Unmarshal(body, &m); err != nil {
		return nil, nil, errors.Wrap(err, "unmarshal query instant response")
	}

	var vectorResult model.Vector

	// Decode the Result depending on the ResultType
	// Currently only `vector` and `scalar` types are supported.
	switch m.Data.ResultType {
	case promql.ValueTypeVector:
		if err = json.Unmarshal(m.Data.Result, &vectorResult); err != nil {
			return nil, nil, errors.Wrap(err, "decode result into ValueTypeVector")
		}
	case promql.ValueTypeScalar:
		vectorResult, err = convertScalarJSONToVector(m.Data.Result)
		if err != nil {
			return nil, nil, errors.Wrap(err, "decode result into ValueTypeScalar")
		}
	default:
		if m.Warnings != nil {
			return nil, nil, errors.Errorf("error: %s, type: %s, warning: %s", m.Error, m.ErrorType, strings.Join(m.Warnings, ", "))
		}
		if m.Error != "" {
			return nil, nil, errors.Errorf("error: %s, type: %s", m.Error, m.ErrorType)
		}

		return nil, nil, errors.Errorf("received status code: %d, unknown response type: '%q'", resp.StatusCode, m.Data.ResultType)
	}
	return vectorResult, m.Warnings, nil
}

// QueryInstant performs an instant query using a default HTTP client and returns results in model.Vector type.
func QueryInstant(ctx context.Context, logger log.Logger, base *url.URL, query string, t time.Time, opts QueryOptions) (model.Vector, []string, error) {
	return defaultClient(logger).QueryInstant(ctx, base, query, t, opts)
}

// PromqlQueryInstant performs instant query and returns results in promql.Vector type that is compatible with promql package.
func (c *Client) PromqlQueryInstant(ctx context.Context, base *url.URL, query string, t time.Time, opts QueryOptions) (promql.Vector, []string, error) {
	vectorResult, warnings, err := c.QueryInstant(ctx, base, query, t, opts)
	if err != nil {
		return nil, nil, err
	}

	vec := make(promql.Vector, 0, len(vectorResult))

	for _, e := range vectorResult {
		lset := make(promlabels.Labels, 0, len(e.Metric))

		for k, v := range e.Metric {
			lset = append(lset, promlabels.Label{
				Name:  string(k),
				Value: string(v),
			})
		}
		sort.Sort(lset)

		vec = append(vec, promql.Sample{
			Metric: lset,
			Point:  promql.Point{T: int64(e.Timestamp), V: float64(e.Value)},
		})
	}

	return vec, warnings, nil
}

// PromqlQueryInstant performs instant query and returns results in promql.Vector type that is compatible with promql package.
func PromqlQueryInstant(ctx context.Context, logger log.Logger, base *url.URL, query string, t time.Time, opts QueryOptions) (promql.Vector, []string, error) {
	return defaultClient(logger).PromqlQueryInstant(ctx, base, query, t, opts)
}

// Scalar response consists of array with mixed types so it needs to be
// unmarshaled separately.
func convertScalarJSONToVector(scalarJSONResult json.RawMessage) (model.Vector, error) {
	var (
		// Do not specify exact length of the expected slice since JSON unmarshaling
		// would make the length fit the size and we won't be able to check the length afterwards.
		resultPointSlice []json.RawMessage
		resultTime       model.Time
		resultValue      model.SampleValue
	)
	if err := json.Unmarshal(scalarJSONResult, &resultPointSlice); err != nil {
		return nil, err
	}
	if len(resultPointSlice) != 2 {
		return nil, errors.Errorf("invalid scalar result format %v, expected timestamp -> value tuple", resultPointSlice)
	}
	if err := json.Unmarshal(resultPointSlice[0], &resultTime); err != nil {
		return nil, errors.Wrapf(err, "unmarshaling scalar time from %v", resultPointSlice)
	}
	if err := json.Unmarshal(resultPointSlice[1], &resultValue); err != nil {
		return nil, errors.Wrapf(err, "unmarshaling scalar value from %v", resultPointSlice)
	}
	return model.Vector{&model.Sample{
		Metric:    model.Metric{},
		Value:     resultValue,
		Timestamp: resultTime}}, nil
}

// MetricValues returns current value of  instant query and returns results in model.Vector type.
func MetricValues(ctx context.Context, logger log.Logger, base *url.URL, perMetricFn func(metric promlabels.Labels, val float64) error) error {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	u := *base
	u.Path = path.Join(u.Path, "/metrics")

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return errors.Wrap(err, "create GET request")
	}

	req.Header.Add("Accept", `application/openmetrics-text; version=0.0.1,text/plain;version=0.0.4;q=0.5,*/*;q=0.1`)
	req.Header.Add("Accept-Encoding", "gzip")

	req = req.WithContext(ctx)

	client := &http.Client{
		Transport: tracing.HTTPTripperware(logger, http.DefaultTransport),
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrapf(err, "perform GET request against %s", u.String())
	}
	defer runutil.ExhaustCloseWithLogOnErr(logger, resp.Body, "metrics body")

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("server returned HTTP status %s", resp.Status)
	}

	b := &bytes.Buffer{}
	if resp.Header.Get("Content-Encoding") != "gzip" {
		_, err = io.Copy(b, resp.Body)
		if err != nil {
			return err
		}
	} else {
		buf := bufio.NewReader(resp.Body)
		gzipr, err := gzip.NewReader(buf)
		if err != nil {
			return err
		}
		_, err = io.Copy(b, gzipr)
		_ = gzipr.Close()
		if err != nil {
			return err
		}
	}

	p := textparse.New(b.Bytes(), resp.Header.Get("Content-Type"))
	for {
		var et textparse.Entry
		if et, err = p.Next(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if et != textparse.EntrySeries {
			continue
		}

		var lset promlabels.Labels
		_ = p.Metric(&lset)
		_, _, v := p.Series()

		if err := perMetricFn(lset, v); err != nil {
			return err
		}
	}
}
