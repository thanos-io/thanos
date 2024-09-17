// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadatapb

func (m *Meta) Equal(o *Meta) bool {
	if m == nil && o == nil {
		return true
	}
	if m == nil || o == nil {
		return false
	}
	if m.Type != o.Type {
		return false
	}
	if m.Help != o.Help {
		return false
	}
	if m.Unit != o.Unit {
		return false
	}

	return true
}

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
	mt := make(map[string]*MetricMetadataEntry, len(m))
	for k, v := range m {
		metas := make([]*Meta, len(v))
		for i, meta := range v {
			meta := meta

			metas[i] = meta
		}

		mt[k] = &MetricMetadataEntry{Metas: metas}
	}
	return &MetricMetadata{Metadata: mt}
}
