package bplustree

const (
	MaxKV = 255
	MaxKC = 511
)

type Node interface {
	count() int
	find(key []byte) (int, bool)
	parent() *InteriorNode
	setParent(*InteriorNode)
	full() bool
	isDirty() bool
	setDirty(bool)
	cache() (bool, []byte, []byte)
	largestKey() []byte
	encode() (value []byte)
	decode(data []byte)

	//DecodeMsg(dc *msgp.Reader) (err error)
	//EncodeMsg(en *msgp.Writer) (err error)
	//MarshalMsg(b []byte) (o []byte, err error)
	//UnmarshalMsg(bts []byte) (o []byte, err error)
	//Msgsize() (s int)
}

var (
	prefixLeaf     = byte(0)
	prefixInterior = byte(1)
	suffixLeaf     = byte(0)
	suffixInterior = byte(1)
)
