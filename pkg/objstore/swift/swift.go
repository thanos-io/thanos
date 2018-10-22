/*
Copyright 2018 eBay Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package swift implements common object storage abstractions against OpenStack swift APIs.
package swift

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/improbable-eng/thanos/pkg/objstore"

	"github.com/go-kit/kit/log"
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/openstack/objectstorage/v1/objects"
	"github.com/gophercloud/gophercloud/pagination"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"
)

const (
	// Class A operations.
	opObjectsList  = "objects.list"
	opObjectInsert = "object.insert"

	// Class B operation.
	opObjectGet = "object.get"

	// Free operations.
	opObjectDelete = "object.delete"
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

type swiftConfig struct {
	AuthUrl       string `yaml:"authUrl"`
	Username      string `yaml:"username,omitempty"`
	UserId        string `yaml:"userId,omitempty"`
	Password      string `yaml:"password"`
	DomainId      string `yaml:"domainId,omitempty"`
	DomainName    string `yaml:"domainName,omitempty"`
	TenantID      string `yaml:"tenantId,omitempty"`
	TenantName    string `yaml:"tenantName,omitempty"`
	RegionName    string `yaml:"regionName,omitempty"`
	ContainerName string `yaml:"containerName"`
}

type Container struct {
	logger   log.Logger
	client   *gophercloud.ServiceClient
	opsTotal *prometheus.CounterVec
	name     string
}

func NewContainer(logger log.Logger, conf []byte, reg prometheus.Registerer) (*Container, error) {
	var sc swiftConfig
	if err := yaml.Unmarshal(conf, &sc); err != nil {
		return nil, err
	}

	authOpts := gophercloud.AuthOptions{
		IdentityEndpoint: sc.AuthUrl,
		Username:         sc.Username,
		UserID:           sc.UserId,
		Password:         sc.Password,
		DomainID:         sc.DomainId,
		DomainName:       sc.DomainName,
		TenantID:         sc.TenantID,
		TenantName:       sc.TenantName,

		// Allow Gophercloud to re-authenticate automatically.
		AllowReauth: true,
	}

	provider, err := openstack.AuthenticatedClient(authOpts)
	if err != nil {
		return nil, err
	}

	client, err := openstack.NewObjectStorageV1(provider, gophercloud.EndpointOpts{
		Region: sc.RegionName,
	})
	if err != nil {
		return nil, err
	}

	c := &Container{
		logger: logger,
		client: client,
		opsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_objstore_swift_container_operations_total",
			Help:        "Total number of operations that were executed against a Swift Storage container.",
			ConstLabels: prometheus.Labels{"container": sc.ContainerName},
		}, []string{"operation"}),
		name: sc.ContainerName,
	}

	if reg != nil {
		reg.MustRegister()
	}

	return c, nil
}

// Name returns the container name for swift.
func (c *Container) Name() string {
	return c.name
}

// Iter calls f for each entry in the given directory. The argument to f is the full
// object name including the prefix of the inspected directory.
func (c *Container) Iter(ctx context.Context, dir string, f func(string) error) error {
	c.opsTotal.WithLabelValues(opObjectsList).Inc()
	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if dir != "" {
		dir = strings.TrimSuffix(dir, DirDelim) + DirDelim
	}

	options := &objects.ListOpts{Full: false, Prefix: dir, Delimiter: DirDelim}
	err := objects.List(c.client, c.name, options).EachPage(func(page pagination.Page) (bool, error) {
		objectNames, err := objects.ExtractNames(page)
		if err != nil {
			return false, err
		}
		for _, objectName := range objectNames {
			if err := f(objectName); err != nil {
				return false, err
			}
		}

		return true, nil
	})

	return err
}

// Get returns a reader for the given object name.
func (c *Container) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if name == "" {
		return nil, fmt.Errorf("error, empty container name passed")
	}
	c.opsTotal.WithLabelValues(opObjectGet).Inc()
	response := objects.Download(c.client, c.name, name, nil)
	return response.Body, response.Err
}

// GetRange returns a new range reader for the given object name and range.
func (c *Container) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	c.opsTotal.WithLabelValues(opObjectGet).Inc()
	options := objects.DownloadOpts{
		Newest: true,
		Range:  fmt.Sprintf("bytes=%d-%d", off, off+length-1),
	}
	response := objects.Download(c.client, c.name, name, options)
	return response.Body, response.Err
}

// Exists checks if the given object exists.
func (c *Container) Exists(ctx context.Context, name string) (bool, error) {
	c.opsTotal.WithLabelValues(opObjectGet).Inc()
	err := objects.Get(c.client, c.name, name, nil).Err
	if err == nil {
		return true, nil
	}

	if _, ok := err.(gophercloud.ErrDefault404); ok {
		return false, nil
	}

	return false, err
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (c *Container) IsObjNotFoundErr(err error) bool {
	_, ok := err.(gophercloud.ErrDefault404)
	return ok
}

// Upload writes the contents of the reader as an object into the container.
func (c *Container) Upload(ctx context.Context, name string, r io.Reader) error {
	c.opsTotal.WithLabelValues(opObjectInsert).Inc()

	options := &objects.CreateOpts{Content: r}
	res := objects.Create(c.client, c.name, name, options)
	return res.Err
}

// Delete removes the object with the given name.
func (c *Container) Delete(ctx context.Context, name string) error {
	c.opsTotal.WithLabelValues(opObjectDelete).Inc()
	return objects.Delete(c.client, c.name, name, nil).Err

}

func (*Container) Close() error {
	// nothing to close
	return nil
}

func (c *Container) CreateContainer(name string) error {
	return containers.Create(c.client, name, nil).Err
}

func (c *Container) DeleteContainer(name string) error {
	return containers.Delete(c.client, name).Err
}

func configFromEnv() swiftConfig {
	c := swiftConfig{
		AuthUrl:       os.Getenv("OS_AUTH_URL"),
		Username:      os.Getenv("OS_USERNAME"),
		Password:      os.Getenv("OS_PASSWORD"),
		TenantID:      os.Getenv("OS_TENANT_ID"),
		TenantName:    os.Getenv("OS_TENANT_NAME"),
		RegionName:    os.Getenv("OS_REGION_NAME"),
		ContainerName: os.Getenv("OS_CONTAINER_NAME"),
	}

	return c
}

// ValidateForTests checks to see the config options for tests are set.
func ValidateForTests(conf swiftConfig) error {
	if conf.AuthUrl == "" ||
		conf.Username == "" ||
		conf.Password == "" ||
		(conf.TenantName == "" && conf.TenantID == "") ||
		conf.RegionName == "" {
		return errors.New("insufficient swift test configuration information")
	}
	return nil
}

// NewTestContainer creates test bkt client that before returning creates temporary container.
// In a close function it empties and deletes the container.
func NewTestContainer(t testing.TB) (objstore.Bucket, func(), error) {
	config := configFromEnv()
	if err := ValidateForTests(config); err != nil {
		return nil, nil, err
	}
	containerConfig, err := yaml.Marshal(config)
	if err != nil {
		return nil, nil, err
	}

	c, err := NewContainer(log.NewNopLogger(), containerConfig, nil)
	if err != nil {
		return nil, nil, err
	}

	if config.ContainerName != "" {
		if os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "" {
			return nil, nil, errors.New("OS_CONTAINER_NAME is defined. Normally this tests will create temporary container " +
				"and delete it after test. Unset OS_CONTAINER_NAME env variable to use default logic. If you really want to run " +
				"tests against provided (NOT USED!) container, set THANOS_ALLOW_EXISTING_BUCKET_USE=true. WARNING: That container " +
				"needs to be manually cleared. This means that it is only useful to run one test in a time. This is due " +
				"to safety (accidentally pointing prod container for test) as well as swift not being fully strong consistent.")
		}

		if err := c.Iter(context.Background(), "", func(f string) error {
			return errors.Errorf("container %s is not empty", config.ContainerName)
		}); err != nil {
			return nil, nil, errors.Wrapf(err, "swift check container %s", config.ContainerName)
		}

		t.Log("WARNING. Reusing", config.ContainerName, "container for Swift tests. Manual cleanup afterwards is required")
		return c, func() {}, nil
	}

	src := rand.NewSource(time.Now().UnixNano())

	tmpContainerName := fmt.Sprintf("test_%s_%x", strings.ToLower(t.Name()), src.Int63())
	if len(tmpContainerName) >= 63 {
		tmpContainerName = tmpContainerName[:63]
	}

	if err := c.CreateContainer(tmpContainerName); err != nil {
		return nil, nil, err
	}

	c.name = tmpContainerName
	t.Log("created temporary container for swift tests with name", tmpContainerName)

	return c, func() {
		objstore.EmptyBucket(t, context.Background(), c)
		if err := c.DeleteContainer(tmpContainerName); err != nil {
			t.Logf("deleting container %s failed: %s", tmpContainerName, err)
		}
	}, nil
}
