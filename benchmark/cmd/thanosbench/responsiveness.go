package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/benchmark/pkg/tsdb"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// This test will:
// 1. Generate a large amount of historic metric data, and store it in GCS.
// 2. Bootstrap a cluster containing a thanos-store and a thanos-query.
// 3. Perform queries over the store & measure responsiveness.

func testStoreResponsiveness(logger log.Logger, opts *opts) error {
	tmpDir, err := ioutil.TempDir("", "thanos")
	if err != nil {
		return errors.Wrap(err, "failed to create temporary directory for holding tsdb data")
	}
	tsdbDir := filepath.Join(tmpDir, "tsdb")

	defer func() {
		if err := os.RemoveAll(tsdbDir); err != nil {
			level.Error(logger).Log("failed to remove tsdb dir", tsdbDir)
		}
	}()

	// Create local tsdb.
	level.Info(logger).Log("msg", "Writing historic timeseries", "num-timeseries", opts.numTimeseries, "output-dir", tsdbDir)
	tsdbEndTime := time.Now()
	tsdbStartTime := tsdbEndTime.Add(-*opts.tsdbLength)
	if err := tsdb.CreateThanosTSDB(tsdb.Opts{
		OutputDir:      tsdbDir,
		NumTimeseries:  *opts.numTimeseries,
		StartTime:      tsdbStartTime,
		EndTime:        tsdbEndTime,
		SampleInterval: time.Second * 15,
		BlockLength:    *opts.blockLength,
	}); err != nil {
		return errors.Wrap(err, "failed to generate tsdb")
	}

	// Create k8s client.
	k8sConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: *opts.kubeConfig},
		&clientcmd.ConfigOverrides{CurrentContext: *opts.cluster},
	).ClientConfig()
	if err != nil {
		return errors.Wrap(err, "failed to create client config for cluster")
	}
	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return errors.Wrap(err, "failed to create client set")
	}

	// Remove any resources in the cluster.
	if err := cleanCluster(logger, k8sClient); err != nil {
		return err
	}

	// Create resources for this cluster.
	if err := bootstrapStoreResponsivenessCluster(logger, opts, k8sClient); err != nil {
		return err
	}

	// Safety prompt.
	fmt.Printf("WARNING: this will delete all data in the bucket (%s). Do you want to continue? Y/n: ", *opts.bucket)
	var resp string
	if _, err := fmt.Scanln(&resp); err != nil {
		return errors.Wrap(err, "failed to confirm intput")
	}
	if resp != "Y" && resp != "y" {
		return nil
	}
	level.Info(logger).Log("msg", "Uploading timeseries to GCS")

	// Upload TSDB.
	if err := pushToGCS(logger, opts, tsdbDir); err != nil {
		return errors.Wrap(err, "failed to upload data to gcs")
	}

	// Collect query information.
	results, err := getQueryTimes(logger, opts, k8sClient, "thanos-query", thanosNamespace, "querier-thanos", thanosHTTPPort)
	if err != nil {
		return err
	}

	level.Info(logger).Log("results", string(results))

	return nil
}

func bootstrapStoreResponsivenessCluster(logger log.Logger, opts *opts, k8sClient *kubernetes.Clientset) error {
	// Create namespaces.
	if err := createNamespaces(logger, k8sClient); err != nil {
		return errors.Wrap(err, "failed to create namespaces")
	}

	// Create headless service for thanos gossip members.
	if _, err := k8sClient.CoreV1().Services(thanosNamespace).Create(createThanosGossipService(thanosNamespace)); err != nil {
		return errors.Wrap(err, "failed to create headless service for thanos gossip")
	}

	// Create thanos store.
	level.Info(logger).Log("msg", "Creating thanos store")
	if _, err := k8sClient.CoreV1().Pods(thanosNamespace).Create(createThanosStore(opts, "improbable-thanos-loadtest")); err != nil {
		return errors.Wrap(err, "failed to create thanos store pod")
	}

	// Create thanos query layer.
	level.Info(logger).Log("msg", "Creating thanos query layer")
	svc, pod := createThanosQuery(opts)
	if _, err := k8sClient.CoreV1().Services(thanosNamespace).Create(svc); err != nil {
		return errors.Wrap(err, "failed to create thanos query service")
	}
	if _, err := k8sClient.CoreV1().Pods(thanosNamespace).Create(pod); err != nil {
		return errors.Wrap(err, "failed to create thanos query pod")
	}

	return nil
}

func pushToGCS(logger log.Logger, opts *opts, uploadDir string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	bkt := client.Bucket(*opts.bucket)

	objIt := bkt.Objects(ctx, nil)
	for {
		obj, err := objIt.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}

		level.Info(logger).Log("Deleting file", obj.Name)
		if err := bkt.Object(obj.Name).Delete(ctx); err != nil {
			level.Warn(logger).Log("failed to delete file", obj.Name, "error", err)
			continue
		}
	}

	return filepath.Walk(uploadDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return errors.Wrapf(err, "failed to open file (%s)", path)
		}

		trimmedPath := strings.TrimPrefix(path, uploadDir+string(filepath.Separator))
		level.Info(logger).Log("Uploading file", trimmedPath)
		w := bkt.Object(trimmedPath).NewWriter(ctx)

		if _, err := io.Copy(w, f); err != nil {
			return errors.Wrapf(err, "failed to upload file (%s)", trimmedPath)
		}

		if err := w.Close(); err != nil {
			return errors.Wrapf(err, "failed to close file (%s)", trimmedPath)
		}

		return nil
	})
}
