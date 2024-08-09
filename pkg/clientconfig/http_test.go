// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package clientconfig

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestNewHTTPClientConfigFromYAML(t *testing.T) {
	for _, tc := range []struct {
		desc string
		cfg  HTTPClientConfig
		err  bool
	}{
		{
			desc: "empty string",
			cfg:  HTTPClientConfig{},
			err:  false,
		},
		{
			desc: "missing CA file",
			cfg: HTTPClientConfig{
				TLSConfig: TLSConfig{
					CAFile: "xxx",
				},
			},
			err: true,
		},
		{
			desc: "invalid CA file",
			cfg: HTTPClientConfig{
				TLSConfig: TLSConfig{
					CAFile: "testdata/invalid.pem",
				},
			},
			err: true,
		},
		{
			desc: "valid CA file",
			cfg: HTTPClientConfig{
				TLSConfig: TLSConfig{
					CAFile: "testdata/tls-ca-chain.pem",
				},
			},
			err: false,
		},
		{
			desc: "invalid cert file",
			cfg: HTTPClientConfig{
				TLSConfig: TLSConfig{
					CAFile:   "testdata/tls-ca-chain.pem",
					CertFile: "testdata/invalid.pem",
					KeyFile:  "testdata/self-signed-client.key",
				},
			},
			err: true,
		},
		{
			desc: "invalid key file",
			cfg: HTTPClientConfig{
				TLSConfig: TLSConfig{
					CAFile:   "testdata/tls-ca-chain.pem",
					CertFile: "testdata/self-signed-client.crt",
					KeyFile:  "testdata/invalid.pem",
				},
			},
			err: true,
		},
		{
			desc: "valid CA, cert and key files",
			cfg: HTTPClientConfig{
				TLSConfig: TLSConfig{
					CAFile:   "testdata/tls-ca-chain.pem",
					CertFile: "testdata/self-signed-client.crt",
					KeyFile:  "testdata/self-signed-client.key",
				},
			},
			err: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := NewHTTPClient(tc.cfg, "")
			if tc.err {
				t.Logf("err: %v", err)
				testutil.NotOk(t, err)
				return
			}

			testutil.Ok(t, err)
		})
	}
}
