package receive

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// Remote write header constants.
const (
	versionHeader       = "X-Prometheus-Remote-Write-Version"
	version1HeaderValue = "0.1.0"
	version2HeaderValue = "2.0.0"
	appProtoContentType = "application/x-protobuf"
)

type WriteProtoFullName protoreflect.FullName

const (
	// WriteProtoFullNameV1 represents the `prometheus.WriteRequest` protobuf
	// message introduced in the https://prometheus.io/docs/specs/remote_write_spec/.
	// To be DEPRECATED
	WriteProtoFullNameV1 WriteProtoFullName = "prometheus.WriteRequest"
	// WriteProtoFullNameV2 represents the `io.prometheus.write.v2.Request` protobuf
	// message introduced in https://prometheus.io/docs/specs/remote_write_spec_2_0/
	WriteProtoFullNameV2 WriteProtoFullName = "io.prometheus.write.v2.Request"
)

// Validate returns error if the given reference for the protobuf message is not supported.
func (n WriteProtoFullName) Validate() error {
	switch n {
	case WriteProtoFullNameV1, WriteProtoFullNameV2:
		return nil
	default:
		return fmt.Errorf("unknown remote write protobuf message %v, supported: %v", n, protoMsgs{WriteProtoFullNameV1, WriteProtoFullNameV2}.String())
	}
}

type protoMsgs []WriteProtoFullName

func (m protoMsgs) Strings() []string {
	ret := make([]string, 0, len(m))
	for _, typ := range m {
		ret = append(ret, string(typ))
	}
	return ret
}

func (m protoMsgs) String() string {
	return strings.Join(m.Strings(), ", ")
}

var contentTypeHeaders = map[WriteProtoFullName]string{
	WriteProtoFullNameV1: appProtoContentType, // Also application/x-protobuf;proto=prometheus.WriteRequest but simplified for compatibility with 1.x spec.
	WriteProtoFullNameV2: appProtoContentType + ";proto=io.prometheus.write.v2.Request",
}

// ContentTypeHeader returns content type header value for the given proto message
// or empty string for unknown proto message.
func contentTypeHeader(m WriteProtoFullName) string {
	return contentTypeHeaders[m]
}

const (
	writtenSamplesHeader    = "X-Prometheus-Remote-Write-Samples-Written"
	writtenHistogramsHeader = "X-Prometheus-Remote-Write-Histograms-Written"
	writtenExemplarsHeader  = "X-Prometheus-Remote-Write-Exemplars-Written"
)

// Compression represents the encoding. Currently remote storage supports only
// one, but we experiment with more, thus leaving the compression scaffolding
// for now.
type Compression string

const (
	// SnappyBlockCompression represents https://github.com/google/snappy/blob/2c94e11145f0b7b184b831577c93e5a41c4c0346/format_description.txt
	SnappyBlockCompression Compression = "snappy"
)
