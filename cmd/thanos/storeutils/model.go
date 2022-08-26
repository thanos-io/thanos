package storeutils

import (
	"fmt"
	"strings"
	"time"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type labelSet map[string][]string

func (l labelSet) String() string {
	var s string
	if len(l) == 0 {
		return "No labels"
	}

	elipsisCutOut := 5
	for k, v := range l {
		valStrBuilder := strings.Builder{}
		valStrBuilder.WriteString("[")
		for i, val := range v {
			valStrBuilder.WriteString(val)
			if i >= elipsisCutOut {
				valStrBuilder.WriteString("...")
				valStrBuilder.WriteString(fmt.Sprintf(" (%d more)", len(v)-elipsisCutOut))
				break
			}
			if i != len(v)-1 {
				valStrBuilder.WriteString(", ")
			}
		}
		valStrBuilder.WriteString("]")

		s += fmt.Sprintf("\t\t%s: %s\n", k, valStrBuilder.String())
	}

	return s
}

type BlockSummary struct {
	LabelSet     labelSet  `json:"labelSet"`
	TotalMinTime time.Time `json:"totalMinTime"`
	TotalMaxTime time.Time `json:"totalMaxTime"`
}

type BlockInfo struct {
	metadata.Meta
}

func (b BlockInfo) HasLabelPair(key, value string) bool {
	for l, v := range b.Thanos.Labels {
		if l == key && v == value {
			return true
		}
	}

	return false
}

type InstanceInfo struct {
	Name         string       `json:"name"`
	Blocks       []BlockInfo  `json:"blocks"`
	BlockSummary BlockSummary `json:"blockSummary"`
}

func (i InstanceInfo) HasLabelPair(key, value string) bool {
	for _, block := range i.Blocks {
		if block.HasLabelPair(key, value) {
			return true
		}
	}

	return false
}

// string method for BlockInfo
func (b BlockInfo) String() string {
	return fmt.Sprintf(
		"Block: %s\n\tMin time: %s\n\tMax time: %s\n\tLabelset: \n%s",
		b.Meta.ULID,
		time.Unix(b.Meta.MinTime/1000, 0).Format(time.RFC1123Z),
		time.Unix(b.Meta.MinTime/1000, 0).Format(time.RFC1123Z),
		b.Thanos.Labels,
	)
}
