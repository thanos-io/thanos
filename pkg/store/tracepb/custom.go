package tracepb

import (
	"io"
)

type spanNetRecvBytesReporter struct {
	io.ReadCloser

	span *Span
}

func (m *Span) AddNetRecvBytesSizer(rc io.ReadCloser) io.ReadCloser {
	return &spanNetRecvBytesReporter{ReadCloser: rc, span: m}
}

func (s *spanNetRecvBytesReporter) Read(p []byte) (n int, err error) {
	n, err = s.ReadCloser.Read(p)
	s.span.NetRecvBytes += int64(n)
	return n, err
}
