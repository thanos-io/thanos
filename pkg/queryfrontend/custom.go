package queryfrontend

import (
	gogoproto "github.com/gogo/protobuf/proto"
)

func init() {
	// Hack: register these types for gogoproto. This is needed because Cortex
	// code calls anypb from gogoproto and in there you need to have these types
	// registered. Without this, gogoproto doesn't know what to do with these types
	// i.e. how to marshal/unmarshal them.
	// TODO(GiedriusS): Remove this after Cortex is migrated to protobuf v2 API.
	gogoproto.RegisterType((*ThanosLabelsResponse)(nil), "queryfrontend.ThanosLabelsResponse")
	gogoproto.RegisterType((*ThanosSeriesResponse)(nil), "queryfrontend.ThanosSeriesResponse")
	gogoproto.RegisterType((*ResponseHeader)(nil), "queryfrontend.ResponseHeader")

	gogoproto.RegisterFile("queryfrontend/response.proto", file_queryfrontend_response_proto_rawDesc)
}
