package main

import (
	"context"
	"net/url"
	"testing"

	"github.com/improbable-eng/thanos/pkg/discovery/dns"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
)

func Test_parseFlagLabels(t *testing.T) {
	var tData = []struct {
		s   []string
		err error
	}{
		{
			s:   []string{`labelName="LabelVal"`, `_label_Name="LabelVal"`, `label_name="LabelVal"`, `LAb_el_Name="LabelValue"`, `lab3l_Nam3="LabelValue"`}, // Valid
			err: nil,
		},
		{
			s:   []string{`label-Name="LabelVal"`}, //Unsupported labelname
			err: errors.New(""),
		},
		{
			s:   []string{`label:Name="LabelVal"`}, //Unsupported labelname
			err: errors.New(""),
		},
		{
			s:   []string{`1abelName="LabelVal"`}, //Unsupported labelname
			err: errors.New(""),
		},
		{
			s:   []string{`label_Name"LabelVal"`}, //Missing "=" seprator
			err: errors.New(""),
		},
		{
			s:   []string{`label_Name= "LabelVal"`}, //whitespace invalid syntax
			err: errors.New(""),
		},
		{
			s:   []string{`label_name=LabelVal`}, //Missing quotes invalid syntax
			err: errors.New(""),
		},
	}
	for _, td := range tData {
		_, err := parseFlagLabels(td.s)
		testutil.Equals(t, err == nil, td.err == nil)
	}
}

func TestRule_AlertmanagerResolveWithoutPort(t *testing.T) {
	mockResolver := mockResolver{
		resultIPs: map[string][]string{
			"alertmanager.com:9093": {"1.1.1.1:9300"},
		},
	}
	am := alertmanagerSet{resolver: mockResolver, addrs: []string{"dns+http://alertmanager.com"}}

	ctx := context.TODO()
	err := am.update(ctx)
	testutil.Ok(t, err)

	expected := []*url.URL{
		{
			Scheme: "http",
			Host:   "1.1.1.1:9300",
		},
	}
	gotURLs := am.get()
	testutil.Equals(t, expected, gotURLs)
}

func TestRule_AlertmanagerResolveWithPort(t *testing.T) {
	mockResolver := mockResolver{
		resultIPs: map[string][]string{
			"alertmanager.com:19093": {"1.1.1.1:9300"},
		},
	}
	am := alertmanagerSet{resolver: mockResolver, addrs: []string{"dns+http://alertmanager.com:19093"}}

	ctx := context.TODO()
	err := am.update(ctx)
	testutil.Ok(t, err)

	expected := []*url.URL{
		{
			Scheme: "http",
			Host:   "1.1.1.1:9300",
		},
	}
	gotURLs := am.get()
	testutil.Equals(t, expected, gotURLs)
}

type mockResolver struct {
	resultIPs map[string][]string
	err       error
}

func (m mockResolver) Resolve(ctx context.Context, name string, qtype dns.QType) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	if res, ok := m.resultIPs[name]; ok {
		return res, nil
	}
	return nil, errors.Errorf("mockResolver not found response for name: %s", name)
}
