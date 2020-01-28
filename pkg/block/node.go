// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type addNode func(root *Node, metas map[ulid.ULID]*metadata.Meta, node *Node) bool

// Node type implements a node of a tree.
type Node struct {
	ID       ulid.ULID
	Value    *metadata.Meta
	Children []*Node
}

// NewNode creates a new node.
func NewNode(id ulid.ULID, value *metadata.Meta) *Node {
	return &Node{
		ID:       id,
		Children: []*Node{},
		Value:    value,
	}
}

// Add Inserts node as children to root node.
func (n *Node) Add(metas map[ulid.ULID]*metadata.Meta, addNodeFn addNode, nodes []*Node) bool {
	for _, newNode := range nodes {
		addNodeResult := addNodeFn(n, metas, newNode)
		if !addNodeResult {
			return false
		}
	}
	return true
}

// GetNonRootIDs returns list of ids which are not on root level.
func (n *Node) GetNonRootIDs() []ulid.ULID {
	var ulids []ulid.ULID
	for _, node := range n.Children {
		ulids = append(ulids, n.iterate(node)...)
		ulids = remove(ulids, node.ID)
	}
	return ulids
}

func (n *Node) iterate(node *Node) []ulid.ULID {
	var ulids []ulid.ULID
	ulids = append(ulids, node.ID)
	for _, childNode := range node.Children {
		ulids = append(ulids, n.iterate(childNode)...)
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
