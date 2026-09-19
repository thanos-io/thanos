// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package tenant

import (
	"strings"
)

const tenantIDsLabelSeparator = "|"

func JoinTenantIDs(tenantIDs []string) string {
	return strings.Join(tenantIDs, tenantIDsLabelSeparator)
}
