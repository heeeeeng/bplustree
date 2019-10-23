package bplustree

import (
	//"log"
	"bytes"
	"fmt"
	"sort"
)

//go:generate msgp

//msgp: tuple KV
type KV struct {
	Key   []byte
	Value []byte
}

//msgp: tuple KVs
type KVs []KV

func (a KVs) Len() int           { return len(a) }
func (a KVs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KVs) Less(i, j int) bool { return bytes.Compare(a[i].Key, a[j].Key) < 0 }

func (a KVs) String() string {
	var s string
	for _, kv := range a {
		s += fmt.Sprintf("%x\t", kv.Key)
	}
	return s
}

//msgp: tuple LeafNode
type LeafNode struct {
	Kvs    KVs
	Count  int
	P      *InteriorNode
	next   *LeafNode
	keyLen int
}

func newLeafNode(p *InteriorNode, keyLen int) *LeafNode {
	return &LeafNode{
		Kvs:    make(KVs, MaxKV),
		P:      p,
		keyLen: keyLen,
	}
}

// find finds the index of a Key in the leaf Node.
// If the Key exists in the Node, it returns the index and true.
// If the Key does not exist in the Node, it returns index to
// insert the Key (the index of the smallest Key in the Node that larger
// than the given Key) and false.
func (l *LeafNode) find(key []byte) (int, bool) {
	c := func(i int) bool {
		return bytes.Compare(l.Kvs[i].Key, key) >= 0
	}

	i := sort.Search(l.Count, c)

	if i < l.Count && bytes.Compare(l.Kvs[i].Key, key) == 0 {
		return i, true
	}

	return i, false
}

// insert
func (l *LeafNode) insert(key []byte, value []byte) ([]byte, bool) {
	i, ok := l.find(key)

	if ok {
		//log.Println("insert.replace", i)
		l.Kvs[i].Value = value
		return nil, false
	}

	if !l.full() {
		copy(l.Kvs[i+1:], l.Kvs[i:l.Count])
		l.Kvs[i].Key = key
		l.Kvs[i].Value = value
		l.Count++
		return nil, false
	}

	next := l.split()

	if bytes.Compare(key, next.Kvs[0].Key) < 0 {
		l.insert(key, value)
	} else {
		next.insert(key, value)
	}

	return next.Kvs[0].Key, true
}

func (l *LeafNode) split() *LeafNode {
	next := newLeafNode(nil, l.keyLen)

	copy(next.Kvs[0:], l.Kvs[l.Count/2+1:])

	next.Count = MaxKV - l.Count/2 - 1
	next.next = l.next

	l.Count = l.Count/2 + 1
	l.next = next

	return next
}

func (l *LeafNode) full() bool { return l.Count == MaxKV }

func (l *LeafNode) parent() *InteriorNode { return l.P }

func (l *LeafNode) setParent(p *InteriorNode) { l.P = p }

func (l *LeafNode) encode() (key []byte, value []byte) {

	return
}

func (l *LeafNode) decode(data []byte) {

	return
}
