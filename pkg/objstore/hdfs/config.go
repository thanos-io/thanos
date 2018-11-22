package hdfs

// Config stores the configuration for an HDFS Bucket.
type Config struct {
	// NameNodeAddresses specifies the namenode(s) to connect to.
	NameNodeAddresses []string

	// UserName specifies which HDFS user the client will act as.
	UserName string

	// UseDataNodeHostnames specifies whether to connect to the DataNodes via
	// hostname (which is useful in multi-homed setups) or IP address, which may
	// be required if DNS isn't available.
	UseDataNodeHostnames bool

	// BucketPath is the path of this bucket within HDFS.
	BucketPath string
}
