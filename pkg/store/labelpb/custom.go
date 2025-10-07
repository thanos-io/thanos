package labelpb

import "github.com/prometheus/prometheus/model/labels"

func PromLabelsSize(lbls labels.Labels) int {
	n := 0
	lbls.Range(func(l labels.Label) {
		ln := l.Name
		lln := len(ln)
		n += 1 + lln + sovTypes(uint64(lln))
		lv := l.Value
		lln = len(lv)
		n += 1 + lln + sovTypes(uint64(lln))
	})
	n += 1 + sovTypes(uint64(n))
	return n
}

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
