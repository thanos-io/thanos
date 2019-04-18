package azure

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestConfig_validate(t *testing.T) {
	type fields struct {
		StorageAccountName string
		StorageAccountKey  string
		ContainerName      string
		Endpoint           string
	}
	tests := []struct {
		name         string
		fields       fields
		wantErr      bool
		wantEndpoint string
	}{
		{
			name: "valid global configuration",
			fields: fields{
				StorageAccountName: "foo",
				StorageAccountKey:  "bar",
				ContainerName:      "roo",
			},
			wantErr:      false,
			wantEndpoint: azureDefaultEndpoint,
		},
		{
			name: "valid custom endpoint",
			fields: fields{
				StorageAccountName: "foo",
				StorageAccountKey:  "bar",
				ContainerName:      "roo",
				Endpoint:           "blob.core.chinacloudapi.cn",
			},
			wantErr:      false,
			wantEndpoint: "blob.core.chinacloudapi.cn",
		},
		{
			name: "no account key but account name",
			fields: fields{
				StorageAccountName: "foo",
				StorageAccountKey:  "",
				ContainerName:      "roo",
			},
			wantErr: true,
		},
		{
			name: "no account name but account key",
			fields: fields{
				StorageAccountName: "",
				StorageAccountKey:  "bar",
				ContainerName:      "roo",
			},
			wantErr: true,
		},
		{
			name: "no container name",
			fields: fields{
				StorageAccountName: "foo",
				StorageAccountKey:  "bar",
				ContainerName:      "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := &Config{
				StorageAccountName: tt.fields.StorageAccountName,
				StorageAccountKey:  tt.fields.StorageAccountKey,
				ContainerName:      tt.fields.ContainerName,
				Endpoint:           tt.fields.Endpoint,
			}
			err := conf.validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.validate() error = %v, wantErr %v", err, tt.wantErr)
			} else {
				testutil.Equals(t, tt.wantEndpoint, conf.Endpoint)
			}
		})
	}
}
