package aws

import (
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestParseConfig(t *testing.T) {
	input := []byte(`bucket: abcd`)

	cfg, err := parseConfig(input)
	testutil.Ok(t, err)

	if cfg.Bucket != "abcd" {
		t.Errorf("parsing of bucket failed: got %v, expected %v", cfg.Bucket, "abcd")
	}
}

func TestParseConfig_DefaultHTTPConfig(t *testing.T) {
	input := []byte(`bucket: abcd`)

	cfg, err := parseConfig(input)
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

func TestParseConfig_CustomHTTPConfig(t *testing.T) {
	input := []byte(`bucket: abcd
http_config:
  insecureSkipVerify: true
  idleConnectTimeout: 50s
  responseHeaderTimeout: 1m`)
	cfg, err := parseConfig(input)
	testutil.Ok(t, err)

	if time.Duration(cfg.HTTPConfig.IdleConnTimeout) != time.Duration(50*time.Second) {
		t.Errorf("parsing of idle_conn_timeout failed: got %v, expected %v",
			time.Duration(cfg.HTTPConfig.IdleConnTimeout), time.Duration(50*time.Second))
	}

	if time.Duration(cfg.HTTPConfig.ResponseHeaderTimeout) != time.Duration(1*time.Minute) {
		t.Errorf("parsing of response_header_timeout failed: got %v, expected %v",
			time.Duration(cfg.HTTPConfig.IdleConnTimeout), time.Duration(1*time.Minute))
	}

	if !cfg.HTTPConfig.InsecureSkipVerify {
		t.Errorf("parsing of insecure_skip_verify failed: got %v, expected %v", cfg.HTTPConfig.InsecureSkipVerify, false)
	}
}

func TestValidate_OK(t *testing.T) {
	input := []byte(`bucket: "bucket-name"
accessKey: "access_key"
encryptSSE: false
secretKey: "secret_key"
httpConfig:
  insecureSkipVerify: false
  idleConnectTimeout: 50s`)
	cfg, err := parseConfig(input)
	testutil.Ok(t, err)
	testutil.Ok(t, validate(cfg))
	testutil.Assert(t, cfg.UserMetadata != nil, "map should not be nil")

	input2 := []byte(`bucket: "bucket-name"
accessKey: "access_key"
encrypt_sse: false
secretKey: "secret_key"
userMetadata:
  "X-Amz-Acl": "bucket-owner-full-control"
httpConfig:
  idle_conn_timeout: 0s`)
	cfg2, err := parseConfig(input2)
	testutil.Ok(t, err)
	testutil.Ok(t, validate(cfg2))

	testutil.Equals(t, "bucket-owner-full-control", cfg2.UserMetadata["X-Amz-Acl"])
}

func TestParseConfig_PartSize(t *testing.T) {
	input := []byte(`bucket: "bucket-name"
accessKey: "access_key"
encryptSSE: false
secretKey: "secret_key"
httpConfig:
  insecureSkipVerify: false
  idleConnectTimeout: 50s`)

	cfg, err := parseConfig(input)
	testutil.Ok(t, err)
	testutil.Assert(t, cfg.PartSize == 1024*1024*5, "when part size not set it should default to 5MiB")

	input2 := []byte(`bucket: "bucket-name"
accessKey: "access_key"
encryptSSE: true
secretKey: "secret_key"
maxPartSize: 104857600
httpConfig:
  insecureSkipVerify: false
  idleConnectTimeout: 50s`)

	cfg2, err := parseConfig(input2)
	testutil.Ok(t, err)
	testutil.Assert(t, cfg2.PartSize == 1024*1024*100, "when part size should be set to 100MiB")
}
