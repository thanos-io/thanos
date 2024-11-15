package queryfrontend

import (
	bytes "bytes"
	"context"
	"net/http"
	"net/http/httptest"

	spb "github.com/gogo/googleapis/google/rpc"
	"github.com/gogo/protobuf/types"
	"github.com/gogo/status"
	grpc "google.golang.org/grpc"
)

type HTTPGRPCServer struct {
	handler http.HandlerFunc
}

type nopCloser struct {
	*bytes.Buffer
}

func toHeader(hs []*Header, header http.Header) {
	for _, h := range hs {
		header[h.Key] = h.Values
	}
}

func fromHeader(hs http.Header) []*Header {
	result := make([]*Header, 0, len(hs))
	for k, vs := range hs {
		result = append(result, &Header{
			Key:    k,
			Values: vs,
		})
	}
	return result
}

func (nopCloser) Close() error { return nil }

// BytesBuffer returns the underlaying `bytes.buffer` used to build this io.ReadCloser.
func (n nopCloser) BytesBuffer() *bytes.Buffer { return n.Buffer }

var _ HTTPServer = &HTTPGRPCServer{}

func (s *HTTPGRPCServer) Handle(ctx context.Context, r *HTTPRequest) (*HTTPResponse, error) {
	req, err := http.NewRequest(r.Method, r.Url, nopCloser{Buffer: bytes.NewBuffer(r.Body)})
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	req.RequestURI = r.Url
	req.ContentLength = int64(len(r.Body))
	toHeader(r.Headers, req.Header)

	recorder := httptest.NewRecorder()
	s.handler.ServeHTTP(recorder, req)
	resp := &HTTPResponse{
		Code:    int32(recorder.Code),
		Headers: fromHeader(recorder.Header()),
		Body:    recorder.Body.Bytes(),
	}
	if recorder.Code/100 == 5 {
		return nil, ErrorFromHTTPResponse(resp)
	}
	return resp, nil
}

func ErrorFromHTTPResponse(resp *HTTPResponse) error {
	a, err := types.MarshalAny(resp)
	if err != nil {
		return err
	}

	return status.ErrorProto(&spb.Status{
		Code:    resp.Code,
		Message: string(resp.Body),
		Details: []*types.Any{a},
	})
}

func RegisterHTTPGRPCServer(httpgrpcServer HTTPServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		RegisterHTTPServer(s, httpgrpcServer)
	}
}

func NewHTTPGRPCServer(handler http.HandlerFunc) *HTTPGRPCServer {
	return &HTTPGRPCServer{handler: handler}
}
