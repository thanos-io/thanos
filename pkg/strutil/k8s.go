// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package strutil

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Pre-compile the regex for efficiency if the function is called often.
// Kept the original regex for strict matching.
var podOrdinalRegex = regexp.MustCompile(`^.*\-(\d+)\..+\.svc\.cluster\.local$`)

// ExtractPodOrdinal extracts the ordinal number from a Kubernetes StatefulSet pod DNS name.
// The DNS name should be in the format: <pod-name>.<service-name>.<namespace>.svc.cluster.local[:port]
// where <pod-name> is typically <statefulset-name>-<ordinal> or a hyphenated name ending with -<ordinal>.
func ExtractPodOrdinal(dnsName string) (int, error) {
	dnsWithoutPort := strings.Split(dnsName, ":")[0]
	matches := podOrdinalRegex.FindStringSubmatch(dnsWithoutPort)
	if len(matches) != 2 {
		return -1, fmt.Errorf("invalid DNS name format: %s", dnsName)
	}
	ordinal, err := strconv.Atoi(matches[1])
	if err != nil {
		return -1, fmt.Errorf("failed to parse ordinal number: %v", err)
	}
	return ordinal, nil
}
