package s3

import (
	"testing"
	"time"

	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestParseConfig_DefaultHTTPOpts(t *testing.T) {
	input := []byte(`bucket: abcd
insecure: false
http_config:
  idle_conn_timeout: 50s`)
	cfg, err := parseConfig(input)
	testutil.Ok(t, err)

	if time.Duration(cfg.HTTPConfig.IdleConnTimeout) != time.Duration(50*time.Second) {
		t.Errorf("parsing of idle_conn_timeout failed: got %v, expected %v",
			time.Duration(cfg.HTTPConfig.IdleConnTimeout), time.Duration(50*time.Second))
	}

	if cfg.Bucket != "abcd" {
		t.Errorf("parsing of bucket failed: got %v, expected %v", cfg.Bucket, "abcd")
	}
	if cfg.Insecure != false {
		t.Errorf("parsing of insecure failed: got %v, expected %v", cfg.Insecure, false)
	}

}

func TestValidate_OK(t *testing.T) {
	input := []byte(`bucket: "bucket-name"
endpoint: "s3-endpoint"
access_key: "access_key"
insecure: false
signature_version2: false
encrypt_sse: false
secret_key: "secret_key"
http_config:
  idle_conn_timeout: 0s`)
	cfg, err := parseConfig(input)
	testutil.Ok(t, err)

	testutil.Ok(t, validate(cfg))
}
