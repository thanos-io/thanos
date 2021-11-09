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
	ret := &MetricMetadata{
		Metadata: make(map[string]*MetricMetadataEntry, len(m)),
	}
	for k, metaInfo := range m {
		metaInfo := metaInfo

		ret.Metadata[k] = &MetricMetadataEntry{
			Metas: metaInfo,
		}
	}
	return ret
}
