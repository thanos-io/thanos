// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package block contains common functionality for interacting with TSDB blocks
// in the context of Thanos.
package block

import (
	"context"
	"path"
	"strings"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	"github.com/thanos-io/objstore/providers/azure"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/directory"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

const AzureBlobGen2Name = "AZURE_BLOB_GEN2-" // use the name to identify the bucket type

func WrapWithAzDataLakeSdk(logger log.Logger, confContentYaml []byte, bkt objstore.Bucket) (objstore.Bucket, error) {
	bucketConf := &client.BucketConfig{}
	if err := yaml.UnmarshalStrict(confContentYaml, bucketConf); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}
	if strings.ToUpper(string(bucketConf.Type)) != string(client.AZURE) {
		level.Info(logger).Log("msg", "folder deletion is only enabled with Azure HNS", "bucket_type", bucketConf.Type)
		return bkt, nil
	}
	level.Info(logger).Log("msg", "wrap bucket with Azure DataLake Gen2 SDK")
	config, err := yaml.Marshal(bucketConf.Config)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of bucket configuration")
	}
	conf := azure.DefaultConfig
	if err := yaml.UnmarshalStrict(config, &conf); err != nil {
		return nil, errors.Wrap(err, "unmarshal bucket configuration")
	}
	return &AzureBlobBucket{
		Bucket:           bkt,
		connectionString: conf.StorageConnectionString,
		fs:               path.Join(conf.ContainerName, bucketConf.Prefix),
	}, nil
}

type AzureBlobBucket struct {
	objstore.Bucket
	connectionString, fs string
}

func (b *AzureBlobBucket) Name() string {
	return AzureBlobGen2Name + b.Bucket.Name()
}

func (b *AzureBlobBucket) Delete(ctx context.Context, name string) error {
	cl, err := directory.NewClientFromConnectionString(b.connectionString, name, b.fs, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create Azure DataLake Gen2 client")
	}
	_, err = cl.Delete(ctx, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to delete object %s", name)
	}
	return nil
}
