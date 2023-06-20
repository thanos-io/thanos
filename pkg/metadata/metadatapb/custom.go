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

func FromMetadataMap(m map[string][]*Meta) *MetricMetadata {
	res := &MetricMetadata{Metadata: make(map[string]*MetricMetadataEntry, len(m))}
	for k, v := range m {
		res.Metadata[k] = &MetricMetadataEntry{Metas: v}
	}
	return res
}
