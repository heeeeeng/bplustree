package bplustree

const (
	MaxKV = 255
	MaxKC = 511
)

type node interface {
	find(key []byte) (int, bool)
	parent() *interiorNode
	setParent(*interiorNode)
	full() bool
}
