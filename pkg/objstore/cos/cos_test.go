// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cos

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/exthttp"
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
				HTTPConfig: exthttp.HTTPConfig{
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

func TestConfig_validate(t *testing.T) {
	type fields struct {
		Bucket     string
		Region     string
		AppId      string
		Endpoint   string
		SecretKey  string
		SecretId   string
		HTTPConfig exthttp.HTTPConfig
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "ok endpoint",
			fields: fields{
				Endpoint:  "http://bucket-123.cos.ap-beijing.myqcloud.com",
				SecretId:  "sid",
				SecretKey: "skey",
			},
			wantErr: false,
		},
		{
			name: "ok bucket-appid-region",
			fields: fields{
				Bucket:    "bucket",
				AppId:     "123",
				Region:    "ap-beijing",
				SecretId:  "sid",
				SecretKey: "skey",
			},
			wantErr: false,
		},
		{
			name: "missing skey",
			fields: fields{
				Bucket: "bucket",
				AppId:  "123",
				Region: "ap-beijing",
			},
			wantErr: true,
		},
		{
			name: "missing bucket",
			fields: fields{
				AppId:     "123",
				Region:    "ap-beijing",
				SecretId:  "sid",
				SecretKey: "skey",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := &Config{
				Bucket:     tt.fields.Bucket,
				Region:     tt.fields.Region,
				AppId:      tt.fields.AppId,
				Endpoint:   tt.fields.Endpoint,
				SecretKey:  tt.fields.SecretKey,
				SecretId:   tt.fields.SecretId,
				HTTPConfig: tt.fields.HTTPConfig,
			}
			if err := conf.validate(); (err != nil) != tt.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
