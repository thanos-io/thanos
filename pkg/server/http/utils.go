// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package http

import "net/http"

// ResponseWriterWithStatus wraps around http.ResponseWriter to capture the status code of the response.
type ResponseWriterWithStatus struct {
	http.ResponseWriter
	status          int
	isHeaderWritten bool
}

// WrapResponseWriterWithStatus wraps the http.ResponseWriter for extracting status.
func WrapResponseWriterWithStatus(w http.ResponseWriter) *ResponseWriterWithStatus {
	return &ResponseWriterWithStatus{ResponseWriter: w}
}

// Status returns http response status.
func (r *ResponseWriterWithStatus) Status() int {
	return r.status
}

// WriteHeader writes the header.
func (r *ResponseWriterWithStatus) WriteHeader(code int) {
	if !r.isHeaderWritten {
		r.status = code
		r.ResponseWriter.WriteHeader(code)
		r.isHeaderWritten = true
	}
}
