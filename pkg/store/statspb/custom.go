package statspb

import "io"

type SizedReadCloser struct {
	io.ReadCloser

	bytesRead int64
}

func (s *SizedReadCloser) BytesRead() int64 {
	return s.bytesRead
}

func NewSizedReadCloser(rc io.ReadCloser) *SizedReadCloser {
	return &SizedReadCloser{ReadCloser: rc}
}

func (s *SizedReadCloser) Read(p []byte) (n int, err error) {
	n, err = s.ReadCloser.Read(p)
	s.bytesRead += int64(n)
	return n, err
}
