// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package util

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestGetSourceFromOutgoingCtx(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{
			name:  "No value in key",
			key:   ipAddressesKey,
			value: "",
			want:  "",
		},
		{
			name:  "Value in key",
			key:   ipAddressesKey,
			value: "172.16.1.1",
			want:  "172.16.1.1",
		},
		{
			name:  "Stored under wrong key",
			key:   "wrongkey",
			value: "172.16.1.1",
			want:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test extracting from incoming context
			ctx := context.Background()
			if tt.value != "" {
				md := metadata.Pairs(tt.key, tt.value)
				ctx = metadata.NewIncomingContext(ctx, md)
			}
			got := GetSourceIPsFromIncomingCtx(ctx)
			assert.Equal(t, tt.want, got)

			// Test extracting from outgoing context
			ctx = context.Background()
			if tt.value != "" {
				md := metadata.Pairs(tt.key, tt.value)
				ctx = metadata.NewOutgoingContext(ctx, md)
			}
			got = GetSourceIPsFromOutgoingCtx(ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}
