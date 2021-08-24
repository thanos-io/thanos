// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package azure

import (
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/testutil"
)

type TestCase struct {
	name             string
	config           []byte
	wantFailParse    bool
	wantFailValidate bool
}

var validConfig = []byte(`storage_account: "myStorageAccount"
storage_account_key: "abc123"
container: "MyContainer"
endpoint: "blob.core.windows.net"
reader_config:
  "max_retry_requests": 100
pipeline_config:
  "try_timeout": 0`)

var tests = []TestCase{
	{
		name:             "validConfig",
		config:           validConfig,
		wantFailParse:    false,
		wantFailValidate: false,
	},
	{
		name: "Missing storage account",
		config: []byte(`storage_account: ""
storage_account_key: "abc123"
container: "MyContainer"
endpoint: "blob.core.windows.net"
reader_config:
  "max_retry_requests": 100
pipeline_config:
  "try_timeout": 0`),
		wantFailParse:    false,
		wantFailValidate: true,
	},
	{
		name: "Missing storage account key",
		config: []byte(`storage_account: "asdfasdf"
storage_account_key: ""
container: "MyContainer"
endpoint: "blob.core.windows.net"
reader_config:
  "max_retry_requests": 100
pipeline_config:
  "try_timeout": 0`),
		wantFailParse:    false,
		wantFailValidate: true,
	},
	{
		name: "Negative max_tries",
		config: []byte(`storage_account: "asdfasdf"
storage_account_key: "asdfsdf"
container: "MyContainer"
endpoint: "not.valid"
reader_config:
  "max_retry_requests": 100
pipeline_config:
  "max_tries": -1
  "try_timeout": 0`),
		wantFailParse:    false,
		wantFailValidate: true,
	},
	{
		name: "Negative max_retry_requests",
		config: []byte(`storage_account: "asdfasdf"
storage_account_key: "asdfsdf"
container: "MyContainer"
endpoint: "not.valid"
reader_config:
  "max_retry_requests": -100
pipeline_config:
  "try_timeout": 0`),
		wantFailParse:    false,
		wantFailValidate: true,
	},
	{
		name: "Not a Duration",
		config: []byte(`storage_account: "asdfasdf"
storage_account_key: "asdfsdf"
container: "MyContainer"
endpoint: "not.valid"
reader_config:
  "max_retry_requests": 100
pipeline_config:
  "try_timeout": 10`),
		wantFailParse:    true,
		wantFailValidate: true,
	},
	{
		name: "Valid Duration",
		config: []byte(`storage_account: "asdfasdf"
storage_account_key: "asdfsdf"
container: "MyContainer"
endpoint: "not.valid"
reader_config:
  "max_retry_requests": 100
pipeline_config:
  "try_timeout": "10s"`),
		wantFailParse:    false,
		wantFailValidate: false,
	},
	{
		name: "msi resource used with storage accounts",
		config: []byte(`storage_account: "asdfasdf"
storage_account_key: "asdfsdf"
msi_resource: "https://example.blob.core.windows.net"
container: "MyContainer"
endpoint: "not.valid"
reader_config:
  "max_retry_requests": 100
pipeline_config:
  "try_timeout": "10s"`),
		wantFailParse:    false,
		wantFailValidate: true,
	},
	{
		name: "Valid MSI Resource",
		config: []byte(`storage_account: "myAccount"
storage_account_key: ""
msi_resource: "https://example.blob.core.windows.net"
container: "MyContainer"
endpoint: "not.valid"
reader_config:
  "max_retry_requests": 100
pipeline_config:
  "try_timeout": "10s"`),
		wantFailParse:    false,
		wantFailValidate: false,
	},
}

func TestConfig_validate(t *testing.T) {

	for _, testCase := range tests {

		conf, err := parseConfig(testCase.config)

		if (err != nil) != testCase.wantFailParse {
			t.Errorf("%s error = %v, wantFailParse %v", testCase.name, err, testCase.wantFailParse)
			continue
		}

		validateErr := conf.validate()
		if (validateErr != nil) != testCase.wantFailValidate {
			t.Errorf("%s error = %v, wantFailValidate %v", testCase.name, validateErr, testCase.wantFailValidate)
		}
	}

}

func TestParseConfig_DefaultHTTPConfig(t *testing.T) {

	cfg, err := parseConfig(validConfig)
	testutil.Ok(t, err)

	if time.Duration(cfg.HTTPConfig.IdleConnTimeout) != time.Duration(90*time.Second) {
		t.Errorf("parsing of idle_conn_timeout failed: got %v, expected %v",
			time.Duration(cfg.HTTPConfig.IdleConnTimeout), time.Duration(90*time.Second))
	}

	if time.Duration(cfg.HTTPConfig.ResponseHeaderTimeout) != time.Duration(2*time.Minute) {
		t.Errorf("parsing of response_header_timeout failed: got %v, expected %v",
			time.Duration(cfg.HTTPConfig.IdleConnTimeout), time.Duration(2*time.Minute))
	}

	if cfg.HTTPConfig.InsecureSkipVerify {
		t.Errorf("parsing of insecure_skip_verify failed: got %v, expected %v", cfg.HTTPConfig.InsecureSkipVerify, false)
	}
}
