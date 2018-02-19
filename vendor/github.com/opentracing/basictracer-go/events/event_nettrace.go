package events

import (
	"bytes"
	"fmt"

	"golang.org/x/net/trace"

	basictracer "github.com/opentracing/basictracer-go"
)

// NetTraceIntegrator can be passed into a basictracer as NewSpanEventListener
// and causes all traces to be registered with the net/trace endpoint.
var NetTraceIntegrator = func() func(basictracer.SpanEvent) {
	var tr trace.Trace
	return func(e basictracer.SpanEvent) {
		switch t := e.(type) {
		case basictracer.EventCreate:
			tr = trace.New("tracing", t.OperationName)
			tr.SetMaxEvents(1000)
		case basictracer.EventFinish:
			tr.Finish()
		case basictracer.EventTag:
			tr.LazyPrintf("%s:%v", t.Key, t.Value)
		case basictracer.EventLogFields:
			var buf bytes.Buffer
			for i, f := range t.Fields {
				if i > 0 {
					buf.WriteByte(' ')
				}
				fmt.Fprintf(&buf, "%s:%v", f.Key(), f.Value())
			}

			tr.LazyPrintf("%s", buf.String())
		case basictracer.EventLog:
			if t.Payload != nil {
				tr.LazyPrintf("%s (payload %v)", t.Event, t.Payload)
			} else {
				tr.LazyPrintf("%s", t.Event)
			}
		}
	}
}
