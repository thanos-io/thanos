package compression

import (
	"compress/gzip"
	"compress/zlib"
	"io"
	"net/http"
	"strings"
)

const (
	acceptEncodingHeader  = "Accept-Encoding"
	contentEncodingHeader = "Content-Encoding"
	gzipEncoding          = "gzip"
	deflateEncoding       = "deflate"
)

// Wrapper around http.Handler which adds suitable response compression based
// on the client's Accept-Encoding headers.
type compressedResponseWriter struct {
	http.ResponseWriter
	writer io.Writer
}

// Writes HTTP response content data.
func (c *compressedResponseWriter) Write(p []byte) (int, error) {
	return c.writer.Write(p)
}

// Closes the compressedResponseWriter and ensures to flush all data before.
func (c *compressedResponseWriter) Close() {
	if zlibWriter, ok := c.writer.(*zlib.Writer); ok {
		zlibWriter.Flush()
	}
	if gzipWriter, ok := c.writer.(*gzip.Writer); ok {
		gzipWriter.Flush()
	}
	if closer, ok := c.writer.(io.Closer); ok {
		defer closer.Close()
	}
}

// Constructs a new compressedResponseWriter based on client request headers.
func newCompressedResponseWriter(writer http.ResponseWriter, req *http.Request) *compressedResponseWriter {
	encodings := strings.Split(req.Header.Get(acceptEncodingHeader), ",")
	for _, encoding := range encodings {
		switch strings.TrimSpace(encoding) {
		case gzipEncoding:
			writer.Header().Set(contentEncodingHeader, gzipEncoding)
			return &compressedResponseWriter{
				ResponseWriter: writer,
				writer:         gzip.NewWriter(writer),
			}
		case deflateEncoding:
			writer.Header().Set(contentEncodingHeader, deflateEncoding)
			return &compressedResponseWriter{
				ResponseWriter: writer,
				writer:         zlib.NewWriter(writer),
			}
		}
	}
	return &compressedResponseWriter{
		ResponseWriter: writer,
		writer:         writer,
	}
}

// Handler is a wrapper around http.Handler which adds suitable
// response compression based on the client's Accept-Encoding headers.
type Handler struct {
	Handler http.Handler
}

// ServeHTTP adds compression to the original http.Handler's ServeHTTP() method.
func (c Handler) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	compWriter := newCompressedResponseWriter(writer, req)
	c.Handler.ServeHTTP(compWriter, req)
	compWriter.Close()
}
