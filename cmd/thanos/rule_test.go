// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func Test_parseFlagLabels(t *testing.T) {
	var tData = []struct {
		s         []string
		expectErr bool
	}{
		{
			s:         []string{`labelName="LabelVal"`, `_label_Name="LabelVal"`, `label_name="LabelVal"`, `LAb_el_Name="LabelValue"`, `lab3l_Nam3="LabelValue"`},
			expectErr: false,
		},
		{
			s:         []string{`label-Name="LabelVal"`}, // Unsupported labelname.
			expectErr: true,
		},
		{
			s:         []string{`label:Name="LabelVal"`}, // Unsupported labelname.
			expectErr: true,
		},
		{
			s:         []string{`1abelName="LabelVal"`}, // Unsupported labelname.
			expectErr: true,
		},
		{
			s:         []string{`label_Name"LabelVal"`}, // Missing "=" seprator.
			expectErr: true,
		},
		{
			s:         []string{`label_Name= "LabelVal"`}, // Whitespace invalid syntax.
			expectErr: true,
		},
		{
			s:         []string{`label_name=LabelVal`}, // Missing quotes invalid syntax.
			expectErr: true,
		},
	}
	for _, td := range tData {
		_, err := parseFlagLabels(td.s)
		testutil.Equals(t, err != nil, td.expectErr)
	}
}

func Test_LoadRemoteWriteConfig(t *testing.T) {
	config := `remote_write:
- url: "http://remote-url/api/prom/push"
  remote_timeout: "5s"
  write_relabel_configs:
  - source_labels: [prometheus_replica]
    separator: ";"
    regex: "prometheus-k8s-[01]"
    replacement: "$1"
    action: "keep"
  bearer_token: "token"
  queue_config:
    capacity: 500
    max_shards: 10
    min_shards: 5
    max_samples_per_send: 100
    batch_send_deadline: 30s
    min_backoff: 30ms
    max_backoff: 100ms
  metadata_config:
    send: true
    send_interval: 1m
`
	remoteWriter := &remoteWriteConfigs{
		logger:      log.NewNopLogger(),
		yamlContent: []byte(config),
		externalLabels: labels.New(labels.Label{
			Name:  "external",
			Value: "1",
		}),
	}

	conf, err := remoteWriter.load()
	testutil.Equals(t, err, nil)
	testutil.Equals(t, len(conf.RemoteWriteConfigs), 1)
	testutil.Equals(t, conf.RemoteWriteConfigs[0].URL.String(), "http://remote-url/api/prom/push")
	testutil.Equals(t, len(conf.GlobalConfig.ExternalLabels), 1)
	testutil.Equals(t, conf.GlobalConfig.ExternalLabels.Get("external"), "1")

}
