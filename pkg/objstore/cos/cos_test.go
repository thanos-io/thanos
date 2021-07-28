// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cos

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func Test_parseConfig(t *testing.T) {
	type args struct {
		conf []byte
	}
	tests := []struct {
		name    string
		args    args
		want    Config
		wantErr bool
	}{
		{
			name: "empty",
			args: args{
				conf: []byte(""),
			},
			want:    DefaultConfig,
			wantErr: false,
		},
		{
			name: "max_idle_conns",
			args: args{
				conf: []byte(`
http_config:
  max_idle_conns: 200
`),
			},
			want: Config{
				HTTPConfig: HTTPConfig{
					IdleConnTimeout:       model.Duration(90 * time.Second),
					ResponseHeaderTimeout: model.Duration(2 * time.Minute),
					TLSHandshakeTimeout:   model.Duration(10 * time.Second),
					ExpectContinueTimeout: model.Duration(1 * time.Second),
					MaxIdleConns:          200,
					MaxIdleConnsPerHost:   100,
					MaxConnsPerHost:       0,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseConfig(tt.args.conf)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			testutil.Equals(t, tt.want, got)
		})
	}
}
