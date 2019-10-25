package bplustree

import "github.com/tinylib/msgp/msgp"

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
	largestKey() []byte
	encode() (key []byte, value []byte)
	decode(data []byte)

	DecodeMsg(dc *msgp.Reader) (err error)
	EncodeMsg(en *msgp.Writer) (err error)
	MarshalMsg(b []byte) (o []byte, err error)
	UnmarshalMsg(bts []byte) (o []byte, err error)
	Msgsize() (s int)
}
