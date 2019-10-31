package bplustree

import (
	"bytes"
	"fmt"
	"sort"
)

//go:generate msgp

type KC struct {
	Key   []byte
	Child Node
}

type KCs []KC

func (a KCs) Len() int { return len(a) }

func (a KCs) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a KCs) Less(i, j int) bool {
	compare := bytes.Compare(a[i].Key, a[j].Key)
	return compare < 0
}

func (a KCs) String() string {
	var s string
	for _, kc := range a {
		s += fmt.Sprintf("%x\t", kc.Key)
	}
	return s
}

//msgp: tuple InteriorNode
type InteriorNode struct {
	Kcs   KCs
	Count int

	p      *InteriorNode
	keyLen int

	cacheHash []byte
	cacheData []byte
	dirty     bool
}

func newInteriorNode(p *InteriorNode, largestChild Node, keyLen int) *InteriorNode {
	i := &InteriorNode{
		Kcs:    make(KCs, MaxKC+1),
		p:      p,
		Count:  1,
		keyLen: keyLen,
		dirty:  true,
	}

	if largestChild != nil {
		var key []byte
		if largestChild.count() > 0 {
			key = largestChild.largestKey()
		} else {
			key = make([]byte, keyLen)
			for i := 0; i < keyLen; i++ {
				key[i] = byte(255)
			}
		}
		i.Kcs[0].Key = key
		i.Kcs[0].Child = largestChild
	}
	return i
}

func (in *InteriorNode) find(key []byte) (int, bool) {
	c := func(i int) bool { return bytes.Compare(in.Kcs[i].Key, key) > 0 }

	i := sort.Search(in.Count-1, c)

	return i, true
}

func (in *InteriorNode) count() int { return in.Count }

func (in *InteriorNode) isDirty() bool { return in.dirty }

func (in *InteriorNode) setDirty(dirty bool) { in.dirty = dirty }

func (in *InteriorNode) cache() (bool, []byte, []byte) {
	return in.dirty, in.cacheHash, in.cacheData
}

func (in *InteriorNode) largestKey() []byte { return in.Kcs[in.count()-1].Key }

func (in *InteriorNode) full() bool { return in.Count == MaxKC }

func (in *InteriorNode) parent() *InteriorNode { return in.p }

func (in *InteriorNode) setParent(p *InteriorNode) { in.p = p }

func (in *InteriorNode) insert(key []byte, child Node) ([]byte, *InteriorNode, bool) {
	defer func(n *InteriorNode) {
		n.dirty = true
	}(in)

	i, _ := in.find(key)

	if !in.full() {
		copy(in.Kcs[i+1:], in.Kcs[i:in.Count])

		in.Kcs[i].Key = key
		in.Kcs[i].Child = child
		child.setParent(in)

		in.Count++
		return nil, nil, false
	}

	// insert the new Node into the empty slot
	in.Kcs[MaxKC].Key = key
	in.Kcs[MaxKC].Child = child
	child.setParent(in)

	next, midKey := in.split()

	return midKey, next, true
}

func (in *InteriorNode) split() (*InteriorNode, []byte) {
	sort.Sort(&in.Kcs)

	// get the mid info
	midIndex := MaxKC / 2
	midChild := in.Kcs[midIndex].Child
	midKey := in.Kcs[midIndex].Key

	// create the split Node with out a parent
	next := newInteriorNode(nil, nil, in.keyLen)
	copy(next.Kcs[0:], in.Kcs[midIndex+1:])
	next.Count = MaxKC - midIndex
	// update parent
	for i := 0; i < next.Count; i++ {
		next.Kcs[i].Child.setParent(next)
	}

	// modify the original Node
	in.Count = midIndex + 1
	midChild.setParent(in)

	return next, midKey
}

func (in *InteriorNode) String() string {
	s := "【 " + in.Kcs.String() + " 】\t"

	//TODO

	//for _, kc := range in.Kcs {
	//
	//}

	return s
}

func (in *InteriorNode) encode() (value []byte) {
	value = make([]byte, 0)
	value = append(value, prefixInterior)
	value = append(value, Int32ToBytes(int32(in.count()))...)

	for i := 0; i < in.count(); i++ {
		kc := in.Kcs[i]

		// key size
		value = append(value, Int32ToBytes(int32(len(kc.Key)))...)
		value = append(value, kc.Key...)

		// value size
		_, childHash, _ := kc.Child.cache()
		value = append(value, Int32ToBytes(int32(len(childHash)))...)
		value = append(value, childHash...)
	}

	return value
}

func (in *InteriorNode) decode(data []byte) {

	return
}
