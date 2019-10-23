package bplustree

type BTree struct {
	db Database

	root  *InteriorNode
	first *LeafNode

	leaf     int
	interior int
	height   int
	keyLen   int
}

func newBTree(db Database, keyLen int) *BTree {
	leaf := newLeafNode(nil, keyLen)
	r := newInteriorNode(nil, leaf, keyLen)
	leaf.P = r
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

		if !isRoot {
			interiorP.Kcs[oldIndex].Child = newNode
			newNode.setParent(interiorP)

			midNode = interior
		} else {
			bt.root = newInteriorNode(nil, newNode, bt.keyLen)
			newNode.setParent(bt.root)

			bt.root.insert(mid, interior)
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

// String marshal the tree to a string
// This is for debug only.
func (bt *BTree) String() string {
	s := ""

	s += bt.root.Kcs.String()
	for

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
