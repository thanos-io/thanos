// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package azure

import (
	"context"
	"testing"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func Test_getContainerURL(t *testing.T) {
	type args struct {
		conf Config
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "default",
			args: args{
				conf: Config{
					StorageAccountName: "foo",
					StorageAccountKey:  "Zm9vCg==",
					ContainerName:      "roo",
					Endpoint:           azureDefaultEndpoint,
				},
			},
			want:    "https://foo.blob.core.windows.net/roo",
			wantErr: false,
		},
		{
			name: "azure china",
			args: args{
				conf: Config{
					StorageAccountName: "foo",
					StorageAccountKey:  "Zm9vCg==",
					ContainerName:      "roo",
					Endpoint:           "blob.core.chinacloudapi.cn",
				},
			},
			want:    "https://foo.blob.core.chinacloudapi.cn/roo",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			got, err := getContainerURL(ctx, tt.args.conf)
			if (err != nil) != tt.wantErr {
				t.Errorf("getContainerURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			testutil.Equals(t, tt.want, got.String())
		})
	}
}
