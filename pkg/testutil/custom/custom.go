// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package custom

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"go.uber.org/goleak"
)

// TolerantVerifyLeakMain verifies go leaks but excludes the go routines that are
// launched as side effects of some of our dependencies.
func TolerantVerifyLeakMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// https://github.com/census-instrumentation/opencensus-go/blob/d7677d6af5953e0506ac4c08f349c62b917a443a/stats/view/worker.go#L34
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		// https://github.com/kubernetes/klog/blob/c85d02d1c76a9ebafa81eb6d35c980734f2c4727/klog.go#L417
		goleak.IgnoreTopFunction("k8s.io/klog/v2.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("k8s.io/klog.(*loggingT).flushDaemon"),
		// https://github.com/baidubce/bce-sdk-go/blob/9a8c1139e6a3ad23080b9b8c51dec88df8ce3cda/util/log/logger.go#L359
		goleak.IgnoreTopFunction("github.com/baidubce/bce-sdk-go/util/log.NewLogger.func1"),
	)
}

// TolerantVerifyLeak verifies go leaks but excludes the go routines that are
// launched as side effects of some of our dependencies.
func TolerantVerifyLeak(t *testing.T) {
	goleak.VerifyNone(t,
		// https://github.com/census-instrumentation/opencensus-go/blob/d7677d6af5953e0506ac4c08f349c62b917a443a/stats/view/worker.go#L34
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		// https://github.com/kubernetes/klog/blob/c85d02d1c76a9ebafa81eb6d35c980734f2c4727/klog.go#L417
		goleak.IgnoreTopFunction("k8s.io/klog/v2.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("k8s.io/klog.(*loggingT).flushDaemon"),
		// https://github.com/baidubce/bce-sdk-go/blob/9a8c1139e6a3ad23080b9b8c51dec88df8ce3cda/util/log/logger.go#L359
		goleak.IgnoreTopFunction("github.com/baidubce/bce-sdk-go/util/log.NewLogger.func1"),
	)
}

// Contains fails the test if needle is not contained within haystack, if haystack or needle is
// an empty slice, or if needle is longer than haystack.
func Contains(tb testing.TB, haystack, needle []string) {
	_, file, line, _ := runtime.Caller(1)

	if !contains(haystack, needle) {
		tb.Fatalf(sprintfWithLimit("\033[31m%s:%d: %#v does not contain %#v\033[39m\n\n", filepath.Base(file), line, haystack, needle))
	}
}

func contains(haystack, needle []string) bool {
	if len(haystack) == 0 || len(needle) == 0 {
		return false
	}

	if len(haystack) < len(needle) {
		return false
	}

	for i := 0; i < len(haystack); i++ {
		outer := i

		for j := 0; j < len(needle); j++ {
			// End of the haystack but not the end of the needle, end
			if outer == len(haystack) {
				return false
			}

			// No match, try the next index of the haystack
			if haystack[outer] != needle[j] {
				break
			}

			// End of the needle and it still matches, end
			if j == len(needle)-1 {
				return true
			}

			// This element matches between the two slices, try the next one
			outer++
		}
	}

	return false
}

func sprintfWithLimit(act string, v ...interface{}) string {
	s := fmt.Sprintf(act, v...)
	if len(s) > 10000 {
		return s[:10000] + "...(output trimmed)"
	}
	return s
}
