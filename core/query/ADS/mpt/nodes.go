package mpt

import (
	"github.com/ethereum/go-ethereum/rlp"
)


type Node interface {
	Hash() []byte
	Raw() []interface{}
}

func IsEmptyNode(node Node) bool {
	return node == nil
}

type nodeFlag struct {
	/*
	hash: hash of the node
	dirty: whether the node has changes
	*/
	hash []byte
	dirty bool
}

func newFlag() nodeFlag {
	return nodeFlag{dirty: true}
}

func Hash(node Node) []byte {
	return node.Hash()
}

func Serialize(node Node) []byte {
	var raw interface{}
	raw = node.Raw()
	rlp, err := rlp.EncodeToBytes(raw)
	if err != nil {
		panic(err)
	}
	return rlp
}

type HashNode struct {
	hash []byte
}

func (n HashNode) Hash() []byte {
	return n.hash
}

func (n HashNode) Raw() []interface{} {
	raw := []interface{}{n.hash}
	return raw
}

func NewHashNode(hash []byte) HashNode {
	hashNode := HashNode{hash: hash}
	return hashNode
}