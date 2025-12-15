// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadatapb

func NewMetricMetadataResponse(metadata *MetricMetadata) *MetricMetadataResponse {
	return &MetricMetadataResponse{
		Result: &MetricMetadataResponse_Metadata{
			Metadata: metadata,
		},
	}
}

func NewWarningMetadataResponse(warning error) *MetricMetadataResponse {
	return &MetricMetadataResponse{
		Result: &MetricMetadataResponse_Warning{
			Warning: warning.Error(),
		},
	}
}

// FromMetadataMap converts a map of metadata to a MetricMetadata protobuf message.
func FromMetadataMap(m map[string][]Meta) *MetricMetadata {
	result := &MetricMetadata{
		Metadata: make(map[string]*MetricMetadataEntry, len(m)),
	}
	for k, v := range m {
		metas := make([]*Meta, len(v))
		for i := range v {
			metas[i] = &Meta{
				Type: v[i].Type,
				Help: v[i].Help,
				Unit: v[i].Unit,
			}
		}
		result.Metadata[k] = &MetricMetadataEntry{
			Metas: metas,
		}
	}
	return result
}
