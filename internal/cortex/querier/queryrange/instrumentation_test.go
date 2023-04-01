package queryrange

import (
	"github.com/opentracing/opentracing-go"
	"testing"
)

type mockRequest struct {
	start int64
	end   int64
}

func (m mockRequest) Reset() {
}

func (m mockRequest) String() string {
	return "mock"
}

func (m mockRequest) ProtoMessage() {
}

func (m mockRequest) GetStart() int64 {
	return m.start
}

func (m mockRequest) GetEnd() int64 {
	return m.end
}

func (m mockRequest) GetStep() int64 {
	return 0
}

func (m mockRequest) GetQuery() string {
	return ""
}

func (m mockRequest) GetCachingOptions() CachingOptions {
	return CachingOptions{}
}

func (m mockRequest) WithStartEnd(startTime int64, endTime int64) Request {
	return mockRequest{start: startTime, end: endTime}
}

func (m mockRequest) WithQuery(query string) Request {
	return mockRequest{}
}

func (m mockRequest) LogToSpan(span opentracing.Span) {}

func (m mockRequest) GetStats() string {
	return ""
}

func (m mockRequest) WithStats(stats string) Request {
	return mockRequest{}
}

func TestGetRangeBucket(t *testing.T) {
	cases := []struct {
		start    int64
		end      int64
		expected string
	}{
		{0, -1, "Invalid"},
		{1, 60 * 60 * 1000, "1h"},
		{1, 6 * 60 * 60 * 1000, "6h"},
		{1, 12 * 60 * 60 * 1000, "12h"},
		{1, 24 * 60 * 60 * 1000, "1d"},
		{1, 48 * 60 * 60 * 1000, "2d"},
		{1, 7 * 24 * 60 * 60 * 1000, "7d"},
		{1, 30 * 24 * 60 * 60 * 1000, "30d"},
		{1, 31 * 24 * 60 * 60 * 1000, "+INF"},
	}

	for _, c := range cases {
		req := mockRequest{start: c.start, end: c.end}
		bucket := getRangeBucket(req)
		if bucket != c.expected {
			t.Errorf("getRangeBucket(%v) returned %v, expected %v", req, bucket, c.expected)
		}
	}
}
