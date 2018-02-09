package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	golog "log"
	"net/http"
	"os"
	"runtime"
	"strings"

	"golang.org/x/net/context"

	"github.com/opentracing/basictracer-go/examples/dapperish"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
)

func client() {
	reader := bufio.NewReader(os.Stdin)
	for {
		span := opentracing.StartSpan("getInput")
		ctx := opentracing.ContextWithSpan(context.Background(), span)
		// Make sure that global baggage propagation works.
		span.SetBaggageItem("User", os.Getenv("USER"))
		span.LogFields(log.Object("ctx", ctx))
		fmt.Print("\n\nEnter text (empty string to exit): ")
		text, _ := reader.ReadString('\n')
		text = strings.TrimSpace(text)
		if len(text) == 0 {
			fmt.Println("Exiting.")
			os.Exit(0)
		}

		span.LogFields(log.String("user text", text))

		httpClient := &http.Client{}
		httpReq, _ := http.NewRequest("POST", "http://localhost:8080/", bytes.NewReader([]byte(text)))
		textCarrier := opentracing.HTTPHeadersCarrier(httpReq.Header)
		err := span.Tracer().Inject(span.Context(), opentracing.TextMap, textCarrier)
		if err != nil {
			panic(err)
		}
		resp, err := httpClient.Do(httpReq)
		if err != nil {
			span.LogFields(log.Error(err))
		} else {
			span.LogFields(log.Object("response", resp))
		}

		span.Finish()
	}
}

func server() {
	http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		textCarrier := opentracing.HTTPHeadersCarrier(req.Header)
		wireSpanContext, err := opentracing.GlobalTracer().Extract(
			opentracing.TextMap, textCarrier)
		if err != nil {
			panic(err)
		}
		serverSpan := opentracing.GlobalTracer().StartSpan(
			"serverSpan",
			ext.RPCServerOption(wireSpanContext))
		serverSpan.SetTag("component", "server")
		defer serverSpan.Finish()

		fullBody, err := ioutil.ReadAll(req.Body)
		if err != nil {
			serverSpan.LogFields(log.Error(err))
		}
		serverSpan.LogFields(log.String("request body", string(fullBody)))
	})

	golog.Fatal(http.ListenAndServe(":8080", nil))
}

func main() {
	opentracing.InitGlobalTracer(dapperish.NewTracer("dapperish_tester"))

	go server()
	go client()

	runtime.Goexit()
}
