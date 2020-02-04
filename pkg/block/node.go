// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// Node type represents a node of a tree.
type Node struct {
	metadata.Meta
	Children []*Node
}

// getNonRootIDs returns list of ids which are not on root level.
func getNonRootIDs(root *Node) []ulid.ULID {
	var ulids []ulid.ULID
	for _, node := range root.Children {
		ulids = append(ulids, childrenToULIDs(root, node)...)
		ulids = remove(ulids, root.ULID)
	}
	return ulids
}

func childrenToULIDs(a, b *Node) []ulid.ULID {
	var ulids = []ulid.ULID{b.ULID}
	for _, childNode := range a.Children {
		ulids = append(ulids, childrenToULIDs(a, childNode)...)
	}
	return ulids
}

func remove(items []ulid.ULID, item ulid.ULID) []ulid.ULID {
	newitems := []ulid.ULID{}

	for _, i := range items {
		if i.Compare(item) != 0 {
			newitems = append(newitems, i)
		}
	}

	return newitems
}
