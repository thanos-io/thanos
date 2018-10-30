package s3

import (
	"testing"
	"time"
)

func TestParseConfig(t *testing.T) {
	inputConfig := []byte(`bucket: abcd
http_config:
  idle_conn_timeout: 50s
  max_idle_conns: 123`)
	cfg, err := ParseConfig(inputConfig)
	if err != nil {
		t.Errorf("failed to parse config: %s", err)
	}
	if time.Duration(cfg.HTTPConfig.IdleConnTimeout) != time.Duration(50*time.Second) {
		t.Errorf("parsing of idle_conn_timeout failed: got %v, expected %v",
			time.Duration(cfg.HTTPConfig.IdleConnTimeout), time.Duration(50*time.Second))
	}
	if time.Duration(cfg.HTTPConfig.TLSHandshakeTimeout) != time.Duration(10*time.Second) {
		t.Errorf("overwritten defaults of tls_handshake_timeout: got %v, expected %v",
			time.Duration(cfg.HTTPConfig.TLSHandshakeTimeout), time.Duration(10*time.Second))
	}
	if cfg.Bucket != "abcd" {
		t.Errorf("parsing of bucket failed: got %v, expected %v", cfg.Bucket, "abcd")
	}
	if cfg.HTTPConfig.MaxIdleConns != 123 {
		t.Errorf("parsing of max_idle_conns failed: got %v, expected %v", cfg.HTTPConfig.MaxIdleConns, 123)
	}
}
