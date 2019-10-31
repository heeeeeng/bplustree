package bplustree

import (
	"bytes"
	"golang.org/x/crypto/sha3"
)

type BTree struct {
	db Database

	root    *InteriorNode
	first   *LeafNode
	dirties []*dirtyNode

	leaf     int
	interior int
	height   int
	keyLen   int
}

func NewBTree(db Database, keyLen int) *BTree {
	leaf := newLeafNode(nil, keyLen)
	r := newInteriorNode(nil, leaf, keyLen)
	leaf.p = r
	return &BTree{
		db:       db,
		root:     r,
		first:    leaf,
		leaf:     1,
		interior: 1,
		height:   2,
		keyLen:   keyLen,
	}
}

// first returns the first LeafNode
func (bt *BTree) First() *LeafNode {
	return bt.first
}

// insert inserts a (Key, Value) into the B+ tree
func (bt *BTree) Insert(key []byte, value []byte) {
	_, oldIndex, leaf := search(bt.root, key)
	p := leaf.parent()

	mid, bump := leaf.insert(key, value)
	if !bump {
		return
	}
	bt.leaf++

	var midNode Node
	midNode = leaf

	p.Kcs[oldIndex].Child = leaf.next
	leaf.next.setParent(p)

	interior, interiorP := p, p.parent()

	for {
		var oldIndex int
		var newNode *InteriorNode

		isRoot := interiorP == nil

		if !isRoot {
			oldIndex, _ = interiorP.find(key)
		}

		mid, newNode, bump = interior.insert(mid, midNode)
		if !bump {
			return
		}
		bt.interior++

		if !isRoot {
			interiorP.Kcs[oldIndex].Child = newNode
			newNode.setParent(interiorP)

			midNode = interior
		} else {
			bt.root = newInteriorNode(nil, newNode, bt.keyLen)
			newNode.setParent(bt.root)

			bt.root.insert(mid, interior)
			bt.interior++
			bt.height++
			return
		}

		interior, interiorP = interiorP, interiorP.parent()
	}
}

// Search searches the Key in B+ tree
// If the Key exists, it returns the Value of Key and true
// If the Key does not exist, it returns an empty string and false
func (bt *BTree) Search(key []byte) ([]byte, bool) {
	kv, _, _ := search(bt.root, key)
	if kv == nil {
		return nil, false
	}
	return kv.Value, true
}

func (bt *BTree) SearchRange(start, end []byte) [][]byte {
	return searchRange(bt.root, start, end)
}

// Commit flush all the dirty nodes to db.
func (bt *BTree) Commit() error {
	if !bt.root.isDirty() {
		return nil
	}

	bt.dirties = make([]*dirtyNode, 0)
	hashNode(bt.root, bt)

	//batch := bt.db.NewBatch()
	for _, dirty := range bt.dirties {
		//batch.Put(dirty.hash, dirty.data)
		if err := bt.db.Put(dirty.hash, dirty.data); err != nil {
			return err
		}
		dirty.origin.setDirty(false)
	}
	//return batch.Write()
	return nil
}

func (bt *BTree) appendDirty(key, data []byte, n Node) {
	bt.dirties = append(bt.dirties, newDirtyNode(key, data, n))
}

// hash the tree recursively
func (bt *BTree) hashRc() {
	if !bt.root.isDirty() {
		return
	}
	hashNode(bt.root, bt)
}

//hash the tree in a loop
func (bt *BTree) hashLoop() {
	// TODO
}

// String marshal the tree to a string
// This is for debug only.
func (bt *BTree) String() string {
	s := ""

	// TODO
	// not implemented yet.

	return s
}

func search(n Node, key []byte) (*KV, int, *LeafNode) {
	curr := n
	oldIndex := -1

	for {
		switch t := curr.(type) {
		case *LeafNode:
			i, ok := t.find(key)
			if !ok {
				return nil, oldIndex, t
			}
			return &t.Kvs[i], oldIndex, t
		case *InteriorNode:
			i, _ := t.find(key)
			curr = t.Kcs[i].Child
			oldIndex = i
		default:
			panic("")
		}
	}
}

func searchRange(n Node, start, end []byte) [][]byte {
	result := make([][]byte, 0)

	_, index, leaf := search(n, start)
	for {
		if index == leaf.count() {
			index = 0
			leaf = leaf.next
			continue
		}
		kv := leaf.Kvs[index]
		if bytes.Compare(kv.Key, end) > 0 {
			return result
		}
		result = append(result, kv.Value)
		index++
	}

}

type dirtyNode struct {
	hash   []byte
	data   []byte
	origin Node
}

func newDirtyNode(key, data []byte, n Node) *dirtyNode {
	return &dirtyNode{
		hash:   key,
		data:   data,
		origin: n,
	}
}

func hashNode(n Node, tree *BTree) []byte {
	if dirty, hash, _ := n.cache(); !dirty {
		return hash
	}

	switch node := n.(type) {
	case *InteriorNode:
		for i := 0; i < node.count(); i++ {
			kc := node.Kcs[i]
			hashNode(kc.Child, tree)
		}

		data := node.encode()
		hash := sha3.Sum256(data)

		node.cacheHash = hash[:]
		node.cacheData = data

		tree.appendDirty(hash[:], data, node)
		return hash[:]
	case *LeafNode:
		data := node.encode()
		hash := sha3.Sum256(data)

		node.cacheHash = hash[:]
		node.cacheData = data

		tree.appendDirty(hash[:], data, node)
		return hash[:]
	default:
		return nil
	}
}

//
//func hashChildren(n Node, tree *BTree) *dirtyNode {
//
//	switch node := n.(type) {
//	case *InteriorNode:
//		for i := 0; i < node.count(); i++ {
//			kc := node.Kcs[i]
//
//			hashNode(kc.Child, tree)
//
//		}
//	case *LeafNode:
//
//	default:
//
//	}
//
//	return nil
//}
