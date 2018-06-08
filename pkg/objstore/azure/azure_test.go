package azure

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	blob "github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"github.com/prometheus/client_golang/prometheus"
)

func TestBucket_Exists(t *testing.T) {

	fmt.Printf("URL: %s\n", blobFormatString)

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
		reply   int
		want    bool
		wantErr bool
	}{
		{
			name: "testNonExistedBucket",
			fields: fields{
				containerURL: blob.ContainerURL{},
				config: &Config{
					"testing",
					base64.StdEncoding.EncodeToString([]byte("NoKey")),
					"testing",
				},
				opsTotal: &prometheus.CounterVec{},
			},
			args: args{
				ctx:  context.Background(),
				name: "no_bucket",
			},
			reply:   404,
			want:    false,
			wantErr: false,
		},
		{
			name: "ExistedBucket",
			fields: fields{
				containerURL: blob.ContainerURL{},
				config: &Config{
					"testing",
					base64.StdEncoding.EncodeToString([]byte("NoKey")),
					"testing",
				},
				opsTotal: &prometheus.CounterVec{},
			},
			args: args{
				ctx:  context.Background(),
				name: "bucket",
			},
			reply:   200,
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
			var apiStub = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.reply)
			}))

			blobFormatString = apiStub.URL + "/%s"

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
