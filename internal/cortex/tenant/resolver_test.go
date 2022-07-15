// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package tenant

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/user"
)

func strptr(s string) *string {
	return &s
}

type resolverTestCase struct {
	name         string
	headerValue  *string
	errTenantID  error
	errTenantIDs error
	tenantID     string
	tenantIDs    []string
}

func (tc *resolverTestCase) test(r Resolver) func(t *testing.T) {
	return func(t *testing.T) {

		ctx := context.Background()
		if tc.headerValue != nil {
			ctx = user.InjectOrgID(ctx, *tc.headerValue)
		}

		tenantID, err := r.TenantID(ctx)
		if tc.errTenantID != nil {
			assert.Equal(t, tc.errTenantID, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.tenantID, tenantID)
		}

		tenantIDs, err := r.TenantIDs(ctx)
		if tc.errTenantIDs != nil {
			assert.Equal(t, tc.errTenantIDs, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.tenantIDs, tenantIDs)
		}
	}
}

var commonResolverTestCases = []resolverTestCase{
	{
		name:         "no-header",
		errTenantID:  user.ErrNoOrgID,
		errTenantIDs: user.ErrNoOrgID,
	},
	{
		name:        "empty",
		headerValue: strptr(""),
		tenantIDs:   []string{""},
	},
	{
		name:        "single-tenant",
		headerValue: strptr("tenant-a"),
		tenantID:    "tenant-a",
		tenantIDs:   []string{"tenant-a"},
	},
	{
		name:         "parent-dir",
		headerValue:  strptr(".."),
		errTenantID:  errInvalidTenantID,
		errTenantIDs: errInvalidTenantID,
	},
	{
		name:         "current-dir",
		headerValue:  strptr("."),
		errTenantID:  errInvalidTenantID,
		errTenantIDs: errInvalidTenantID,
	},
}

func TestSingleResolver(t *testing.T) {
	r := NewSingleResolver()
	for _, tc := range append(commonResolverTestCases, []resolverTestCase{
		{
			name:        "multi-tenant",
			headerValue: strptr("tenant-a|tenant-b"),
			tenantID:    "tenant-a|tenant-b",
			tenantIDs:   []string{"tenant-a|tenant-b"},
		},
		{
			name:         "containing-forward-slash",
			headerValue:  strptr("forward/slash"),
			errTenantID:  errInvalidTenantID,
			errTenantIDs: errInvalidTenantID,
		},
		{
			name:         "containing-backward-slash",
			headerValue:  strptr(`backward\slash`),
			errTenantID:  errInvalidTenantID,
			errTenantIDs: errInvalidTenantID,
		},
	}...) {
		t.Run(tc.name, tc.test(r))
	}
}

func TestMultiResolver(t *testing.T) {
	r := NewMultiResolver()
	for _, tc := range append(commonResolverTestCases, []resolverTestCase{
		{
			name:        "multi-tenant",
			headerValue: strptr("tenant-a|tenant-b"),
			errTenantID: user.ErrTooManyOrgIDs,
			tenantIDs:   []string{"tenant-a", "tenant-b"},
		},
		{
			name:        "multi-tenant-wrong-order",
			headerValue: strptr("tenant-b|tenant-a"),
			errTenantID: user.ErrTooManyOrgIDs,
			tenantIDs:   []string{"tenant-a", "tenant-b"},
		},
		{
			name:        "multi-tenant-duplicate-order",
			headerValue: strptr("tenant-b|tenant-b|tenant-a"),
			errTenantID: user.ErrTooManyOrgIDs,
			tenantIDs:   []string{"tenant-a", "tenant-b"},
		},
		{
			name:         "multi-tenant-with-relative-path",
			headerValue:  strptr("tenant-a|tenant-b|.."),
			errTenantID:  errInvalidTenantID,
			errTenantIDs: errInvalidTenantID,
		},
		{
			name:         "containing-forward-slash",
			headerValue:  strptr("forward/slash"),
			errTenantID:  &errTenantIDUnsupportedCharacter{pos: 7, tenantID: "forward/slash"},
			errTenantIDs: &errTenantIDUnsupportedCharacter{pos: 7, tenantID: "forward/slash"},
		},
		{
			name:         "containing-backward-slash",
			headerValue:  strptr(`backward\slash`),
			errTenantID:  &errTenantIDUnsupportedCharacter{pos: 8, tenantID: "backward\\slash"},
			errTenantIDs: &errTenantIDUnsupportedCharacter{pos: 8, tenantID: "backward\\slash"},
		},
	}...) {
		t.Run(tc.name, tc.test(r))
	}
}
