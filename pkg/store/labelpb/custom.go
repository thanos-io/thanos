package labelpb

import "github.com/prometheus/prometheus/model/labels"

func MarshalToSizedBufferPromLabels(lbls labels.Labels, dAtA []byte) (int, error) {
	i := len(dAtA)

	lbls.Range(func(m labels.Label) {
		if len(m.Value) > 0 {
			i -= len(m.Value)
			copy(dAtA[i:], m.Value)
			i = encodeVarintTypes(dAtA, i, uint64(len(m.Value)))
			i--
			dAtA[i] = 0x12
		}
		if len(m.Name) > 0 {
			i -= len(m.Name)
			copy(dAtA[i:], m.Name)
			i = encodeVarintTypes(dAtA, i, uint64(len(m.Name)))
			i--
			dAtA[i] = 0xa
		}
	})
	return len(dAtA) - i, nil
}
