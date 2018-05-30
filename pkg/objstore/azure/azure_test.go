package azure

import (
	"context"
	"encoding/base64"
	"testing"

	blob "github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"github.com/prometheus/client_golang/prometheus"
)

func TestBucket_Exists(t *testing.T) {
	type fields struct {
		containerURL blob.ContainerURL
		config       *Config
		opsTotal     *prometheus.CounterVec
	}
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "testNonExistedBucket",
			fields: fields{
				containerURL: blob.ContainerURL{},
				config: &Config{
					"NotName",
					base64.StdEncoding.EncodeToString([]byte("NoKey")),
					"NoContainer",
				},
				opsTotal: &prometheus.CounterVec{},
			},
			args: args{
				ctx:  context.Background(),
				name: "test_bucket",
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Bucket{
				containerURL: tt.fields.containerURL,
				config:       tt.fields.config,
				opsTotal:     tt.fields.opsTotal,
			}
			got, err := b.Exists(tt.args.ctx, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Bucket.Exists() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Bucket.Exists() = %v, want %v", got, tt.want)
			}
		})
	}
}
