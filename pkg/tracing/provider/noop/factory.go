package noop

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type Factory struct {
}

func NewFactory() *Factory {
	return &Factory{}
}

func (f *Factory) Create(ctx context.Context, logger log.Logger, serviceName string) (opentracing.Tracer, io.Closer) {
	t := &Tracer{}
	return t, t
}

func (f *Factory) RegisterKingpinFlags(app *kingpin.Application) {
}
