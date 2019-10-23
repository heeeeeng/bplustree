package bplustree

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

var (
	defaultKeyLength = 8
)

func newMemDB() Database {
	return NewMemDatabase()
}

func TestInsert(t *testing.T) {
	testCount := 1000000
	bt := newBTree(newMemDB(), defaultKeyLength)

	start := time.Now()
	for i := testCount; i > 0; i-- {
		bt.Insert(Int64ToBytes(int64(i)), nil)
	}
	fmt.Println(time.Now().Sub(start))

	verifyTree(bt, testCount, t)
}

func TestSearch(t *testing.T) {
	testCount := 1000000
	bt := newBTree(newMemDB(), defaultKeyLength)

	for i := testCount; i > 0; i-- {
		bt.Insert(Int64ToBytes(int64(i)), []byte(fmt.Sprintf("%d", i)))
	}

	start := time.Now()
	for i := 1; i < testCount; i++ {
		v, ok := bt.Search(Int64ToBytes(int64(i)))
		if !ok {
			t.Errorf("search: want = true, got = false")
		}
		if string(v) != fmt.Sprintf("%d", i) {
			t.Errorf("search: want = %d, got = %s", i, v)
		}
	}
	fmt.Println(time.Now().Sub(start))
}

func verifyTree(b *BTree, count int, t *testing.T) {
	verifyRoot(b, t)

	for i := 0; i < b.root.Count; i++ {
		verifyNode(b.root.Kcs[i].Child, b.root, t)
	}

	leftMost := findLeftMost(b.root)

	if leftMost != b.first {
		t.Errorf("bt.first: want = %s, \ngot = %s", b.first.Kvs.String(), leftMost.Kvs.String())
	}

	verifyLeaf(leftMost, count, t)
}

// min Child: 1
// max Child: MaxKC
func verifyRoot(b *BTree, t *testing.T) {
	if b.root.parent() != nil {
		t.Errorf("root.parent: want = nil, got = %p", b.root.parent())
	}

	if b.root.Count < 1 {
		t.Errorf("root.min.Child: want >=1, got = %d", b.root.Count)
	}

	if b.root.Count > MaxKC {
		t.Errorf("root.max.Child: want <= %d, got = %d", MaxKC, b.root.Count)
	}
}

func verifyNode(n Node, parent *InteriorNode, t *testing.T) {
	switch nn := n.(type) {
	case *InteriorNode:
		if nn.Count < MaxKC/2 {
			t.Errorf("interior.min.Child: want >= %d, got = %d", MaxKC/2, nn.Count)
		}

		if nn.Count > MaxKC {
			t.Errorf("interior.max.Child: want <= %d, got = %d", MaxKC, nn.Count)
		}

		if nn.parent() != parent {
			t.Errorf("interior.parent: want = %p, got = %p", parent, nn.parent())
		}

		var last []byte
		for i := 0; i < nn.Count; i++ {
			key := nn.Kcs[i].Key
			if key != nil && bytes.Compare(key, last) < 0 {
				t.Errorf("interior.sort.Key: want > %x, got = %x", last, key)
			}
			last = key

			verifyNode(nn.Kcs[i].Child, nn, t)
		}

	case *LeafNode:
		if nn.parent() != parent {
			t.Errorf("leaf.parent: want = %p, got = %p", parent, nn.parent())
		}

		if nn.Count < MaxKV/2 {
			t.Errorf("leaf.min.Child: want >= %d, got = %d", MaxKV/2, nn.Count)
		}

		if nn.Count > MaxKV {
			t.Errorf("leaf.max.Child: want <= %d, got = %d", MaxKV, nn.Count)
		}
	}
}

func verifyLeaf(leftMost *LeafNode, count int, t *testing.T) {
	curr := leftMost
	var last []byte
	c := 0

	for curr != nil {
		for i := 0; i < curr.Count; i++ {
			key := curr.Kvs[i].Key

			if bytes.Compare(key, last) <= 0 {
				t.Errorf("leaf.sort.Key: want > %x, got = %x", last, key)
			}
			last = key
			c++
		}
		curr = curr.next
	}

	if c != count {
		t.Errorf("leaf.Count: want = %d, got = %d", count, c)
	}
}

func findLeftMost(n Node) *LeafNode {
	switch nn := n.(type) {
	case *InteriorNode:
		return findLeftMost(nn.Kcs[0].Child)
	case *LeafNode:
		return nn
	default:
		panic("")
	}
}
