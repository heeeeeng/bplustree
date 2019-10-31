package bplustree

import (
	//"log"
	"bytes"
	"fmt"
	"sort"
)

//go:generate msgp

type KV struct {
	Key   []byte
	Value []byte
}

type KVs struct {
	data    []KV
	cmpFunc func(key1, key2 []byte) int
}

func newKVs(maxKV int, cmpFunc func(key1, key2 []byte) int) *KVs {
	kvs := &KVs{}
	kvs.data = make([]KV, maxKV)
	kvs.cmpFunc = cmpFunc

	return kvs
}

func (a KVs) Len() int           { return len(a.data) }
func (a KVs) Swap(i, j int)      { a.data[i], a.data[j] = a.data[j], a.data[i] }
func (a KVs) Less(i, j int) bool { return a.cmpFunc(a.data[i].Key, a.data[j].Key) < 0 }

func (a KVs) String() string {
	var s string
	for _, kv := range a.data {
		s += fmt.Sprintf("%x\t", kv.Key)
	}
	return s
}

//msgp: tuple LeafNode
type LeafNode struct {
	Kvs   *KVs
	Count int

	p      *InteriorNode
	next   *LeafNode
	keyLen int

	cacheHash []byte
	cacheData []byte
	dirty     bool
}

func newLeafNode(p *InteriorNode, keyLen int, cmpFunc func(key1, key2 []byte) int) *LeafNode {
	return &LeafNode{
		Kvs:    newKVs(MaxKV, cmpFunc),
		p:      p,
		keyLen: keyLen,
		dirty:  true,
	}
}

// find finds the index of a Key in the leaf Node.
// If the Key exists in the Node, it returns the index and true.
// If the Key does not exist in the Node, it returns index to
// insert the Key (the index of the smallest Key in the Node that larger
// than the given Key) and false.
func (l *LeafNode) find(key []byte) (int, bool) {
	c := func(i int) bool {
		return bytes.Compare(l.Kvs.data[i].Key, key) >= 0
	}

	i := sort.Search(l.Count, c)

	if i < l.Count && bytes.Compare(l.Kvs.data[i].Key, key) == 0 {
		return i, true
	}

	return i, false
}

// insert
func (l *LeafNode) insert(key []byte, value []byte) ([]byte, bool) {
	defer func(n *LeafNode) {
		n.dirty = true
	}(l)

	i, ok := l.find(key)

	if ok {
		l.Kvs.data[i].Value = value
		return nil, false
	}

	if !l.full() {
		copy(l.Kvs.data[i+1:], l.Kvs.data[i:l.Count])
		l.Kvs.data[i].Key = key
		l.Kvs.data[i].Value = value
		l.Count++
		return nil, false
	}

	next := l.split()

	if bytes.Compare(key, next.Kvs.data[0].Key) < 0 {
		l.insert(key, value)
	} else {
		next.insert(key, value)
	}

	return next.Kvs.data[0].Key, true
}

func (l *LeafNode) split() *LeafNode {
	next := newLeafNode(nil, l.keyLen, l.Kvs.cmpFunc)

	copy(next.Kvs.data[0:], l.Kvs.data[l.Count/2+1:])

	next.Count = MaxKV - l.Count/2 - 1
	next.next = l.next

	l.Count = l.Count/2 + 1
	l.next = next

	return next
}

func (l *LeafNode) count() int { return l.Count }

func (l *LeafNode) isDirty() bool { return l.dirty }

func (l *LeafNode) setDirty(dirty bool) { l.dirty = dirty }

func (l *LeafNode) cache() (bool, []byte, []byte) {
	return l.dirty, l.cacheHash, l.cacheData
}

func (l *LeafNode) largestKey() []byte { return l.Kvs.data[l.count()-1].Key }

func (l *LeafNode) full() bool { return l.Count == MaxKV }

func (l *LeafNode) parent() *InteriorNode { return l.p }

func (l *LeafNode) setParent(p *InteriorNode) { l.p = p }

func (l *LeafNode) encode() (value []byte) {
	value = make([]byte, 0)
	value = append(value, prefixLeaf)
	value = append(value, Int32ToBytes(int32(l.count()))...)

	for i := 0; i < l.count(); i++ {
		kv := l.Kvs.data[i]

		// size for key
		value = append(value, Int32ToBytes(int32(len(kv.Key)))...)
		value = append(value, kv.Key...)

		// size for value
		value = append(value, Int32ToBytes(int32(len(kv.Value)))...)
		value = append(value, kv.Value...)
	}
	return value
}

func (l *LeafNode) decode(data []byte) {

	return
}

func (l *LeafNode) MsgSize() (s int) {
	// prefix (1) + count (4)
	s = 1 + 4
	for _, kv := range l.Kvs.data {
		// key length
		s += len(kv.Key)
		// value length
		s += len(kv.Value)
	}
	return s
}
