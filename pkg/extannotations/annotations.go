// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extannotations

import (
	"strings"

	"github.com/prometheus/prometheus/util/annotations"
)

func IsPromQLAnnotation(s string) bool {
	// We cannot use "errors.Is(w, annotations.PromQLInfo)" here because of gRPC so we use a string as argument
	return strings.HasPrefix(s, annotations.PromQLInfo.Error()) || strings.HasPrefix(s, annotations.PromQLWarning.Error())
}
