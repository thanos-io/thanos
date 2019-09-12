package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/fatih/structtag"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore/azure"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/cos"
	"github.com/thanos-io/thanos/pkg/objstore/gcs"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/objstore/swift"
	trclient "github.com/thanos-io/thanos/pkg/tracing/client"
	"github.com/thanos-io/thanos/pkg/tracing/elasticapm"
	"github.com/thanos-io/thanos/pkg/tracing/jaeger"
	"github.com/thanos-io/thanos/pkg/tracing/stackdriver"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
	yaml "gopkg.in/yaml.v2"
)

var (
	bucketConfigs = map[client.ObjProvider]interface{}{
		client.AZURE: azure.Config{},
		client.GCS:   gcs.Config{},
		client.S3:    s3.Config{},
		client.SWIFT: swift.SwiftConfig{},
		client.COS:   cos.Config{},
	}
	tracingConfigs = map[trclient.TracingProvider]interface{}{
		trclient.JAEGER:      jaeger.Config{},
		trclient.STACKDRIVER: stackdriver.Config{},
		trclient.ELASTIC_APM: elasticapm.Config{},
	}
)

func main() {
	app := kingpin.New(filepath.Base(os.Args[0]), "Thanos config examples generator.")
	app.HelpFlag.Short('h')
	outputDir := app.Flag("output-dir", "Output directory for generated examples.").String()

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	if _, err := app.Parse(os.Args[1:]); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	for typ, config := range bucketConfigs {
		if err := generate(client.BucketConfig{Type: typ, Config: config}, "bucket_"+strings.ToLower(string(typ)), *outputDir); err != nil {
			level.Error(logger).Log("msg", "failed to generate", "type", typ, "err", err)
			os.Exit(1)
		}
	}

	for typ, config := range tracingConfigs {
		if err := generate(trclient.TracingConfig{Type: typ, Config: config}, "tracing_"+strings.ToLower(string(typ)), *outputDir); err != nil {
			level.Error(logger).Log("msg", "failed to generate", "type", typ, "err", err)
			os.Exit(1)
		}
	}
	logger.Log("msg", "success")
}

func generate(obj interface{}, typ string, outputDir string) error {
	// We forbid omitempty option. This is for simplification for doc generation.
	if err := checkForOmitEmptyTagOption(obj); err != nil {
		return err
	}

	out, err := yaml.Marshal(obj)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(filepath.Join(outputDir, fmt.Sprintf("config_%s.txt", typ)), out, os.ModePerm)
}

func checkForOmitEmptyTagOption(obj interface{}) error {
	return checkForOmitEmptyTagOptionRec(reflect.ValueOf(obj))
}

func checkForOmitEmptyTagOptionRec(v reflect.Value) error {
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i += 1 {
			tags, err := structtag.Parse(string(v.Type().Field(i).Tag))
			if err != nil {
				return err
			}

			tag, err := tags.Get("yaml")
			if err != nil {
				return err
			}

			for _, opts := range tag.Options {
				if opts == "omitempty" {
					return errors.Errorf("omitempty is forbidden for config, but spotted on field '%s'", v.Type().Field(i).Name)
				}
			}

			if err := checkForOmitEmptyTagOptionRec(v.Field(i)); err != nil {
				return err
			}
		}

	case reflect.Ptr:
		if !v.IsValid() {
			return errors.New("nil pointers are not allowed in configuration.")
		}

		return errors.New("nil pointers are not allowed in configuration.")

	case reflect.Interface:
		return checkForOmitEmptyTagOptionRec(v.Elem())
	}

	return nil
}
