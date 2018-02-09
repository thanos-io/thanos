[![GoDoc](https://godoc.org/github.com/lovoo/gcloud-opentracing?status.svg)](http://godoc.org/github.com/lovoo/gcloud-opentracing)
# gcloud-opentracing
 OpenTracing Tracer implementation for GCloud StackDriver in Go. Based on [basictracer](https://github.com/opentracing/basictracer-go) and implemented `Recorder` for this propose.

### Getting Started
-------------------
To install gcloud-opentracing, use `go get`:

```bash
go get github.com/lovoo/gcloud-opentracing
```
or `govendor`:

```bash
govendor fetch github.com/lovoo/gcloud-opentracing
```
or other tool for vendoring.

### Sample Usage
-------------------
First of all, you need to init Global Tracer with GCloud Tracer:
```go
package main

import (
    "log"

    trace "cloud.google.com/go/trace/apiv1"
    gcloudtracer "github.com/lovoo/gcloud-opentracing"
    opentracing "github.com/opentracing/opentracing-go"
    basictracer "github.com/opentracing/basictracer-go"
    "golang.org/x/net/context"
)

func main() {
    // ...
    client, err := trace.NewClient(context.Background() /*auth options here if necessary*/)
    if err != nil {
      log.Fatalf("error creating a tracing client: %v", err)
    }

    recorder, err := gcloudtracer.NewRecorder(context.Background(), "gcp-project-id", client)
    if err != nil {
      log.Fatalf("error creating a recorder: %v", err)
    }
    defer recorder.Close()

    opentracing.InitGlobalTracer(basictracer.New(recorder))
    // ...
}
```

Then you can create traces as decribed [here](https://github.com/opentracing/opentracing-go). More information you can find on [OpenTracing project](http://opentracing.io) website.
