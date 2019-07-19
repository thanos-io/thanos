package thanosrule

import (
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/rules"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	yaml "gopkg.in/yaml.v2"
)

func TestUpdate(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_rule_rule_groups")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	testutil.Ok(t, ioutil.WriteFile(path.Join(dir, "no_strategy.yaml"), []byte(`
groups:
- name: "something1"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(path.Join(dir, "abort.yaml"), []byte(`
groups:
- name: "something2"
  partial_response_strategy: "abort"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(path.Join(dir, "warn.yaml"), []byte(`
groups:
- name: "something3"
  partial_response_strategy: "warn"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(path.Join(dir, "wrong.yaml"), []byte(`
groups:
- name: "something4"
  partial_response_strategy: "afafsdgsdgs" # Err 1
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(path.Join(dir, "combined.yaml"), []byte(`
groups:
- name: "something5"
  partial_response_strategy: "warn"
  rules:
  - alert: "some"
    expr: "up"
- name: "something6"
  partial_response_strategy: "abort"
  rules:
  - alert: "some"
    expr: "up"
- name: "something7"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(path.Join(dir, "combined-wrong.yaml"), []byte(`
groups:
- name: "something8"
  partial_response_strategy: "warn"
  rules:
  - alert: "some"
    expr: "up"
- name: "something9"
  partial_response_strategy: "adad" # Err 2
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))

	opts := rules.ManagerOptions{
		Logger: log.NewLogfmtLogger(os.Stderr),
	}
	m := Managers{
		storepb.PartialResponseStrategy_ABORT: rules.NewManager(&opts),
		storepb.PartialResponseStrategy_WARN:  rules.NewManager(&opts),
	}

	err = m.Update(dir, 10*time.Second, []string{
		path.Join(dir, "no_strategy.yaml"),
		path.Join(dir, "abort.yaml"),
		path.Join(dir, "warn.yaml"),
		path.Join(dir, "wrong.yaml"),
		path.Join(dir, "combined.yaml"),
		path.Join(dir, "combined_wrong.yaml"),
	})

	testutil.NotOk(t, err)
	testutil.Assert(t, strings.HasPrefix(err.Error(), "2 errors: failed to unmarshal 'partial_response_strategy'"), err.Error())

	g := m[storepb.PartialResponseStrategy_WARN].RuleGroups()
	testutil.Equals(t, 2, len(g))

	sort.Slice(g, func(i, j int) bool {
		return g[i].Name() < g[j].Name()
	})
	testutil.Equals(t, "something3", g[0].Name())
	testutil.Equals(t, "something5", g[1].Name())

	g = m[storepb.PartialResponseStrategy_ABORT].RuleGroups()
	testutil.Equals(t, 4, len(g))

	sort.Slice(g, func(i, j int) bool {
		return g[i].Name() < g[j].Name()
	})
	testutil.Equals(t, "something1", g[0].Name())
	testutil.Equals(t, "something2", g[1].Name())
	testutil.Equals(t, "something6", g[2].Name())
	testutil.Equals(t, "something7", g[3].Name())
}

func TestRuleGroupMarshalYAML(t *testing.T) {
	const expected = `groups:
- name: something1
  rules:
  - alert: some
    expr: up
- name: something2
  rules:
  - alert: some
    expr: up
  partial_response_strategy: ABORT
`

	a := storepb.PartialResponseStrategy_ABORT
	var input = RuleGroups{
		Groups: []RuleGroup{
			{
				RuleGroup: rulefmt.RuleGroup{
					Name: "something1",
					Rules: []rulefmt.Rule{
						{
							Alert: "some",
							Expr:  "up",
						},
					},
				},
			},
			{
				RuleGroup: rulefmt.RuleGroup{
					Name: "something2",
					Rules: []rulefmt.Rule{
						{
							Alert: "some",
							Expr:  "up",
						},
					},
				},
				PartialResponseStrategy: &a,
			},
		},
	}

	b, err := yaml.Marshal(input)
	testutil.Ok(t, err)

	testutil.Equals(t, expected, string(b))
}
