package tracing

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/alecthomas/kingpin.v2"
)

// Factory defines an interface for a factory that can create implementations of different tracer types.
type Factory interface {
	Create(ctx context.Context, logger log.Logger, serviceName string) (opentracing.Tracer, io.Closer, error)
	RegisterKingpinFlags(app *kingpin.Application)
}
