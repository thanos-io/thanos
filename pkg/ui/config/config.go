package config

import (
	"fmt"
	"github.com/pkg/errors"
	"strings"

	"github.com/prometheus/common/config"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/s3"

	"gopkg.in/yaml.v2"
)

// Config stores the configuration for storing and accessing blobs in filesystem.
type fileConfig struct {
	Type client.ObjProvider
	Config struct {
		Directory string `yaml:"directory"`
	}
}

// Config stores the configuration for oss bucket.
type aliConfig struct {
	Type client.ObjProvider
	Config struct {
		Endpoint        string        `yaml:"endpoint"`
		Bucket          string        `yaml:"bucket"`
		AccessKeyID     string        `yaml:"access_key_id"`
		AccessKeySecret config.Secret `yaml:"access_key_secret"`
	}
}

// Config stores the configuration for gcs bucket.
type gcsConfig struct {
	Type client.ObjProvider
	Config struct {
		Bucket         string `yaml:"bucket"`
		ServiceAccount string `yaml:"service_account"`
	}
}

// Config encapsulates the necessary config values to instantiate an cos client.
type cosConfig struct {
	Type client.ObjProvider
	Config struct {
		Bucket    string        `yaml:"bucket"`
		Region    string        `yaml:"region"`
		AppId     string        `yaml:"app_id"`
		SecretKey config.Secret `yaml:"secret_key"`
		SecretId  string        `yaml:"secret_id"`
	}
}


// Config Azure storage configuration.
type azureConfig struct {
	Type client.ObjProvider
	Config struct {
		StorageAccountName string        `yaml:"storage_account"`
		StorageAccountKey  config.Secret `yaml:"storage_account_key"`
		ContainerName      string        `yaml:"container"`
		Endpoint           string        `yaml:"endpoint"`
		MaxRetries         int           `yaml:"max_retries"`
	}
}

// Config Swift storage configuration.
type swiftConfig struct {
	Type client.ObjProvider
	Config struct {
		AuthUrl           string        `yaml:"auth_url"`
		Username          string        `yaml:"username"`
		UserDomainName    string        `yaml:"user_domain_name"`
		UserDomainID      string        `yaml:"user_domain_id"`
		UserId            string        `yaml:"user_id"`
		Password          config.Secret `yaml:"password"`
		DomainId          string        `yaml:"domain_id"`
		DomainName        string        `yaml:"domain_name"`
		ProjectID         string        `yaml:"project_id"`
		ProjectName       string        `yaml:"project_name"`
		ProjectDomainID   string        `yaml:"project_domain_id"`
		ProjectDomainName string        `yaml:"project_domain_name"`
		RegionName        string        `yaml:"region_name"`
		ContainerName     string        `yaml:"container_name"`
	}
}

// Config Swift storage configuration.
type s3Config struct {
	Type client.ObjProvider
	Config struct {
		Bucket             string            `yaml:"bucket"`
		Endpoint           string            `yaml:"endpoint"`
		Region             string            `yaml:"region"`
		AccessKey          string            `yaml:"access_key"`
		Insecure           bool              `yaml:"insecure"`
		SignatureV2        bool              `yaml:"signature_version2"`
		SecretKey          config.Secret     `yaml:"secret_key"`
		PutUserMetadata    map[string]string `yaml:"put_user_metadata"`
		HTTPConfig         s3.HTTPConfig     `yaml:"http_config"`
		TraceConfig        s3.TraceConfig     `yaml:"trace"`
		ListObjectsVersion string            `yaml:"list_objects_version"`
		// PartSize used for multipart upload. Only used if uploaded object size is known and larger than configured PartSize.
		PartSize  uint64    `yaml:"part_size"`
		SSEConfig s3.SSEConfig `yaml:"sse_config"`
	}
}

func ReplaceSecret(confContentYaml []byte) ([]byte, error) {
	var str []byte
	conf := &client.BucketConfig{}

	err := yaml.UnmarshalStrict(confContentYaml, conf)
	if err != nil {
		fmt.Println("parsing config YAML file", err)
		return nil, err
	}

	switch strings.ToUpper(string(conf.Type)) {
		case string(client.GCS):
			gcsConf := &gcsConfig{}
			str, _ = mashedYaml(confContentYaml, gcsConf)
		case string(client.S3):
			s3Conf := &s3Config{}
			str, _ = mashedYaml(confContentYaml, s3Conf)
		case string(client.AZURE):
			azConf := &azureConfig{}
			str, _ = mashedYaml(confContentYaml, azConf)
		case string(client.SWIFT):
			swiftConf := &swiftConfig{}
			str, _ = mashedYaml(confContentYaml, swiftConf)
		case string(client.COS):
			cosConf := &cosConfig{}
			str, _ = mashedYaml(confContentYaml, cosConf)
		case string(client.ALIYUNOSS):
			aliConf := &aliConfig{}
			str, _ = mashedYaml(confContentYaml, aliConf)
		case string(client.FILESYSTEM):
			fileConf := &fileConfig{}
			str, _ = mashedYaml(confContentYaml, fileConf)
		default:
			return nil, errors.Errorf("bucket with type %s is not supported", conf.Type)
	}
	return str, err
}

func mashedYaml(confContent []byte, conf interface{}) ([]byte, error) {
	var mashedStr []byte
	err := yaml.UnmarshalStrict(confContent, conf)
	if err != nil {
		fmt.Println("parsing config YAML file", err)
		return nil, err
	}

	mashedStr, _= yaml.Marshal(conf)

	return mashedStr, err
}
