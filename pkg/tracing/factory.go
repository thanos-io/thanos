package tracing

import (
	"context"
	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type Factory interface {
	Create(ctx context.Context, logger log.Logger, debugName string) (opentracing.Tracer, func() error)
	RegisterKingpinFlags(app *kingpin.Application)
}