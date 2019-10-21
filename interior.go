package bplustree

import (
	"bytes"
	"sort"
)

type kc struct {
	key   []byte
	child node
}

// one empty slot for split
type kcs [MaxKC + 1]kc

func (a *kcs) Len() int { return len(a) }

func (a *kcs) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a *kcs) Less(i, j int) bool {
	compare := bytes.Compare(a[i].key, a[j].key)
	return compare < 0
}

type interiorNode struct {
	kcs    kcs
	count  int
	p      *interiorNode
	keyLen int
}

func newInteriorNode(p *interiorNode, largestChild node, keyLen int) *interiorNode {
	i := &interiorNode{
		p:      p,
		count:  1,
		keyLen: keyLen,
	}

	if largestChild != nil {
		i.kcs[0].child = largestChild
	}
	return i
}

func (in *interiorNode) find(key []byte) (int, bool) {
	c := func(i int) bool { return bytes.Compare(in.kcs[i].key, key) > 0 }

	i := sort.Search(in.count-1, c)

	return i, true
}

func (in *interiorNode) full() bool { return in.count == MaxKC }

func (in *interiorNode) parent() *interiorNode { return in.p }

func (in *interiorNode) setParent(p *interiorNode) { in.p = p }

func (in *interiorNode) insert(key []byte, child node) ([]byte, *interiorNode, bool) {
	i, _ := in.find(key)

	if !in.full() {
		copy(in.kcs[i+1:], in.kcs[i:in.count])

		in.kcs[i].key = key
		in.kcs[i].child = child
		child.setParent(in)

		in.count++
		return nil, nil, false
	}

	// insert the new node into the empty slot
	in.kcs[MaxKC].key = key
	in.kcs[MaxKC].child = child
	child.setParent(in)

	next, midKey := in.split()

	return midKey, next, true
}

func (in *interiorNode) split() (*interiorNode, []byte) {
	sort.Sort(&in.kcs)

	// get the mid info
	midIndex := MaxKC / 2
	midChild := in.kcs[midIndex].child
	midKey := in.kcs[midIndex].key

	// create the split node with out a parent
	next := newInteriorNode(nil, nil, in.keyLen)
	copy(next.kcs[0:], in.kcs[midIndex+1:])
	next.count = MaxKC - midIndex
	// update parent
	for i := 0; i < next.count; i++ {
		next.kcs[i].child.setParent(next)
	}

	// modify the original node
	in.count = midIndex + 1
	in.kcs[in.count-1].key = 0
	in.kcs[in.count-1].child = midChild
	midChild.setParent(in)

	return next, midKey
}
