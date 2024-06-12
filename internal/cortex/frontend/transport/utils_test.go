package transport

//
//import (
//	"net/url"
//	"testing"
//
//	"github.com/efficientgo/core/testutil"
//)
//
//func testGetQueryRangeSeconds(t *testing.T) {
//	type testCase struct {
//		values url.Values
//		expect int
//	}
//	var (
//		tcs = []struct {
//			tc testCase
//		}{
//			{
//				// Non-dynamic lookbackDelta should always return the same engine.
//				tc: {2, 2},
//			},
//			{
//				tc: {2, 2},
//			},
//		}
//	)
//	for _, tc := range tcs {
//		got := getQueryRangeSeconds(tc)
//		testutil.Equals(t, tc.expect, got)
//	}
//
//}
//
//func testUpdateFailedQueryCache(t *testing.T) {
//
//}
