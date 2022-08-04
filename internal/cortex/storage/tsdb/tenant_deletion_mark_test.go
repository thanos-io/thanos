// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package tsdb

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestTenantDeletionMarkExists(t *testing.T) {
	const username = "user"

	for name, tc := range map[string]struct {
		objects map[string][]byte
		exists  bool
	}{
		"empty": {
			objects: nil,
			exists:  false,
		},

		"mark doesn't exist": {
			objects: map[string][]byte{
				"user/01EQK4QKFHVSZYVJ908Y7HH9E0/meta.json": []byte("data"),
			},
			exists: false,
		},

		"mark exists": {
			objects: map[string][]byte{
				"user/01EQK4QKFHVSZYVJ908Y7HH9E0/meta.json": []byte("data"),
				"user/" + TenantDeletionMarkPath:            []byte("data"),
			},
			exists: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			// "upload" objects
			for objName, data := range tc.objects {
				require.NoError(t, bkt.Upload(context.Background(), objName, bytes.NewReader(data)))
			}

			res, err := TenantDeletionMarkExists(context.Background(), bkt, username)
			require.NoError(t, err)
			require.Equal(t, tc.exists, res)
		})
	}
}
