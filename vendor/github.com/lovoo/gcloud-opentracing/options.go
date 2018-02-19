package gcloudtracer

import "errors"

var (
	// ErrInvalidProjectID occurs if project identifier is invalid.
	ErrInvalidProjectID = errors.New("invalid project id")
)

// Options contains options for recorder.
type Options struct {
	log Logger
}

// Option defines an recorder option.
type Option func(o *Options)

// WithLogger returns an Option that specifies a logger of the Recorder.
func WithLogger(logger Logger) Option {
	return func(o *Options) {
		o.log = logger
	}
}
