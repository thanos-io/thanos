package hdfs

import (
	"regexp"

	"github.com/pkg/errors"
)

// Config stores the configuration for an HDFS Bucket.
type Config struct {
	// NamenodeAddresses specifies the namenode(s) to connect to.
	NamenodeAddresses []string `yaml:"namenode_addresses"`

	// Username specifies which HDFS user the client will act as.
	Username string `yaml:"username"`

	// UseDatanodeHostnames specifies whether to connect to the DataNodes via
	// hostname (which is useful in multi-homed setups) or IP address, which may
	// be required if DNS isn't available.
	UseDatanodeHostnames bool `yaml:"use_datanode_hostnames"`

	// BucketPath is the path of this bucket within HDFS.
	BucketPath string `yaml:"bucket_path"`
}

// HDFS usernames must comply to this pattern. Taken from the "CAVEATS" section
// of `man 8 useradd`.
var usernamePattern = regexp.MustCompilePOSIX("^[a-z_][a-z0-9_-]*[$]?$")

func (c *Config) validate() (hdfsPath, error) {
	if len(c.NamenodeAddresses) < 1 {
		return invalidHdfsPath, errors.New("no HDFS namenode addresses specified")
	}
	if !usernamePattern.MatchString(c.Username) {
		return invalidHdfsPath, errors.Errorf("HDFS username invalid: %q", c.Username)
	}
	bucketPath, err := buildPath(c.BucketPath)
	if err != nil {
		return invalidHdfsPath, errors.Wrapf(err, "HDFS bucket path is invalid")
	}
	return bucketPath, nil
}
