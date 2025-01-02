package mpt

import (
	"crypto/md5"
)

type ExtensionNode struct {
		Path []byte
		Next Node
		flags nodeFlag
}

func NewExtensionNode(path []byte, next Node) *ExtensionNode {
	return &ExtensionNode{
		Path: path,
		Next: next,
		flags: newFlag(),
	}
}

func (e *ExtensionNode) SetNext(node Node) {
	e.Next = node
}
func (e ExtensionNode) Hash() []byte {
	h := md5.New()
	h.Write(e.Serialize())
	return h.Sum(nil)
	//return crypto.Keccak256(e.Serialize())
}

func (e ExtensionNode) Raw() []interface{} {
	hashes := make([]interface{}, 2)
	hashes[0] = e.Path
	hashes[1] = e.Next.Hash()
	return hashes
}

func (e ExtensionNode) Serialize() []byte {
	return Serialize(e)
}