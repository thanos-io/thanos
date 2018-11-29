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
	"github.com/improbable-eng/thanos/pkg/objstore/azure"
	"github.com/improbable-eng/thanos/pkg/objstore/client"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v2"
)

var (
	configs = map[client.ObjProvider]interface{}{
		client.AZURE: azure.Config{},
		client.GCS:   gcs.Config{},
		client.S3:    s3.Config{},
		client.SWIFT: azure.Config{},
	}
)

func main() {
	app := kingpin.New(filepath.Base(os.Args[0]), "Thanos bucket configs examples generator.")
	app.HelpFlag.Short('h')
	outputDir := app.Flag("output-dir", "Output directory for generated examples.").String()

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	_, err := app.Parse(os.Args[1:])
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	for typ, config := range configs {
		fullCfg := client.BucketConfig{Type: typ, Config: config}

		// We forbid omitempty option. This is for simplification for doc generation.
		if err := checkForOmitEmptyTagOption(fullCfg); err != nil {
			level.Error(logger).Log("type", typ, "err", err)
			os.Exit(1)
		}

		out, err := yaml.Marshal(fullCfg)
		if err != nil {
			level.Error(logger).Log("msg", "failed to marshal config", "type", typ, "err", err)
			os.Exit(1)
		}

		if err := ioutil.WriteFile(filepath.Join(*outputDir, fmt.Sprintf("config_%s.txt", strings.ToLower(string(typ)))), out, os.ModePerm); err != nil {
			level.Error(logger).Log("msg", "failed to write", "type", typ, "err", err)
			os.Exit(1)
		}
	}
	logger.Log("msg", "success")
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
					return errors.Errorf("omitempty is forbidden for bucketConfig, but spotted on field '%s'", v.Type().Field(i).Name)
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
