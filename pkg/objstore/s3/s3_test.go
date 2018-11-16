package s3

import (
	"testing"
	"time"
)

func TestParseConfig(t *testing.T) {
	inputConfig := []byte(`bucket: abcd
insecure: false
http_config:
  idle_conn_timeout: 50s`)
	cfg, err := ParseConfig(inputConfig)
	if err != nil {
		t.Errorf("failed to parse config: %s", err)
	}
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
