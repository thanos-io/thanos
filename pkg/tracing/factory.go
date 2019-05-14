package tracing

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type Factory interface {
	Create(ctx context.Context, logger log.Logger, serviceName string) (opentracing.Tracer, io.Closer)
	RegisterKingpinFlags(app *kingpin.Application)
}
