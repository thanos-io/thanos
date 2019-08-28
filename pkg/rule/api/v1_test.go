package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/tsdb"
	qapi "github.com/thanos-io/thanos/pkg/query/api"
	thanosrule "github.com/thanos-io/thanos/pkg/rule"
)

// NewStorage returns a new storage for testing purposes
// that removes all associated files on closing.
func newStorage(t *testing.T) storage.Storage {
	dir, err := ioutil.TempDir("", "test_storage")
	if err != nil {
		t.Fatalf("Opening test dir failed: %s", err)
	}

	// Tests just load data for a series sequentially. Thus we
	// need a long appendable window.
	db, err := tsdb.Open(dir, nil, nil, &tsdb.Options{
		MinBlockDuration: model.Duration(24 * time.Hour),
		MaxBlockDuration: model.Duration(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("Opening test storage failed: %s", err)
	}
	return testStorage{Storage: tsdb.Adapter(db, int64(0)), dir: dir}
}

type testStorage struct {
	storage.Storage
	dir string
}

func (s testStorage) Close() error {
	if err := s.Storage.Close(); err != nil {
		return err
	}
	return os.RemoveAll(s.dir)
}

type rulesRetrieverMock struct {
	testing *testing.T
}

func (m rulesRetrieverMock) RuleGroups() []thanosrule.Group {
	var ar rulesRetrieverMock
	arules := ar.AlertingRules()
	storage := newStorage(m.testing)
	//defer storage.Close()

	engineOpts := promql.EngineOpts{
		Logger:        nil,
		Reg:           nil,
		MaxConcurrent: 10,
		MaxSamples:    10,
		Timeout:       100 * time.Second,
	}

	engine := promql.NewEngine(engineOpts)
	opts := &rules.ManagerOptions{
		QueryFunc:  rules.EngineQueryFunc(engine, storage),
		Appendable: storage,
		Context:    context.Background(),
		Logger:     log.NewNopLogger(),
	}

	var r []rules.Rule

	for _, alertrule := range arules {
		r = append(r, alertrule)
	}

	recordingExpr, err := promql.ParseExpr(`vector(1)`)
	if err != nil {
		m.testing.Fatalf("unable to parse alert expression: %s", err)
	}
	recordingRule := rules.NewRecordingRule("recording-rule-1", recordingExpr, labels.Labels{})
	r = append(r, recordingRule)

	group := rules.NewGroup("grp", "/path/to/file", time.Second, r, false, opts)
	return []thanosrule.Group{{Group: group}}
}

func (m rulesRetrieverMock) AlertingRules() []thanosrule.AlertingRule {
	expr1, err := promql.ParseExpr(`absent(test_metric3) != 1`)
	if err != nil {
		m.testing.Fatalf("unable to parse alert expression: %s", err)
	}
	expr2, err := promql.ParseExpr(`up == 1`)
	if err != nil {
		m.testing.Fatalf("Unable to parse alert expression: %s", err)
	}

	rule1 := rules.NewAlertingRule(
		"test_metric3",
		expr1,
		time.Second,
		labels.Labels{},
		labels.Labels{},
		labels.Labels{},
		true,
		log.NewNopLogger(),
	)
	rule2 := rules.NewAlertingRule(
		"test_metric4",
		expr2,
		time.Second,
		labels.Labels{},
		labels.Labels{},
		labels.Labels{},
		true,
		log.NewNopLogger(),
	)
	var r []thanosrule.AlertingRule
	r = append(r, thanosrule.AlertingRule{AlertingRule: rule1})
	r = append(r, thanosrule.AlertingRule{AlertingRule: rule2})
	return r
}

func TestEndpoints(t *testing.T) {
	suite, err := promql.NewTest(t, `
		load 1m
			test_metric1{foo="bar"} 0+100x100
			test_metric1{foo="boo"} 1+0x100
			test_metric2{foo="boo"} 1+0x100
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer suite.Close()

	if err := suite.Run(); err != nil {
		t.Fatal(err)
	}

	var algr rulesRetrieverMock
	algr.testing = t
	algr.AlertingRules()
	algr.RuleGroups()

	t.Run("local", func(t *testing.T) {
		var algr rulesRetrieverMock
		algr.testing = t
		algr.AlertingRules()
		algr.RuleGroups()
		api := NewAPI(
			nil,
			prometheus.DefaultRegisterer,
			algr,
		)
		testEndpoints(t, api)
	})
}

func testEndpoints(t *testing.T, api *API) {

	type test struct {
		endpoint qapi.ApiFunc
		params   map[string]string
		query    url.Values
		response interface{}
	}
	var tests = []test{
		{
			endpoint: api.rules,
			response: &RuleDiscovery{
				RuleGroups: []*RuleGroup{
					{
						Name:                    "grp",
						File:                    "/path/to/file",
						Interval:                1,
						PartialResponseStrategy: "WARN",
						Rules: []rule{
							alertingRule{
								Name:                    "test_metric3",
								Query:                   "absent(test_metric3) != 1",
								Duration:                1,
								Labels:                  labels.Labels{},
								Annotations:             labels.Labels{},
								Alerts:                  []*Alert{},
								Health:                  "unknown",
								Type:                    "alerting",
								PartialResponseStrategy: "WARN",
							},
							alertingRule{
								Name:                    "test_metric4",
								Query:                   "up == 1",
								Duration:                1,
								Labels:                  labels.Labels{},
								Annotations:             labels.Labels{},
								Alerts:                  []*Alert{},
								Health:                  "unknown",
								Type:                    "alerting",
								PartialResponseStrategy: "WARN",
							},
							recordingRule{
								Name:   "recording-rule-1",
								Query:  "vector(1)",
								Labels: labels.Labels{},
								Health: "unknown",
								Type:   "recording",
							},
						},
					},
				},
			},
		},
	}

	methods := func(f qapi.ApiFunc) []string {
		return []string{http.MethodGet}
	}

	request := func(m string, q url.Values) (*http.Request, error) {
		return http.NewRequest(m, fmt.Sprintf("http://example.com?%s", q.Encode()), nil)
	}
	for i, test := range tests {
		for _, method := range methods(test.endpoint) {
			// Build a context with the correct request params.
			ctx := context.Background()
			for p, v := range test.params {
				ctx = route.WithParam(ctx, p, v)
			}
			t.Logf("run %d\t%s\t%q", i, method, test.query.Encode())

			req, err := request(method, test.query)
			if err != nil {
				t.Fatal(err)
			}
			endpoint, errors, apiError := test.endpoint(req.WithContext(ctx))

			if errors != nil {
				t.Fatalf("Unexpected errors: %s", errors)
				return
			}
			assertAPIError(t, apiError)
			assertAPIResponse(t, endpoint, test.response)
		}
	}
}

func assertAPIError(t *testing.T, got *qapi.ApiError) {
	if got != nil {
		t.Fatalf("Unexpected error: %s", got)
		return
	}
}

func assertAPIResponse(t *testing.T, got interface{}, exp interface{}) {
	if !reflect.DeepEqual(exp, got) {
		respJSON, err := json.Marshal(got)
		if err != nil {
			t.Fatalf("failed to marshal response as JSON: %v", err.Error())
		}

		expectedRespJSON, err := json.Marshal(exp)
		if err != nil {
			t.Fatalf("failed to marshal expected response as JSON: %v", err.Error())
		}

		t.Fatalf(
			"Response does not match, expected:\n%+v\ngot:\n%+v",
			string(expectedRespJSON),
			string(respJSON),
		)
	}
}
