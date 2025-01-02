package mpt

import (
	"crypto/md5"
	"sort"
	"strconv"
)

const BranchSize = 44

type BranchNode struct {
	Branches [BranchSize]Node
	Value map[int][]byte
	Content map[int]string
	flags    nodeFlag
}

func NewBranchNode() *BranchNode {
	return &BranchNode{
		Branches: [BranchSize]Node{},
		flags: newFlag(),
	}
}

func (b *BranchNode) SetBranch(bit interface{}, node Node) {
	switch bit.(type) {
	case int:
		b.Branches[bit.(int)] = node
	case byte:
		if bit.(byte) > 'Z' {
			b.Branches[bit.(byte) -'a'+26] = node
		} else {
			b.Branches[bit.(byte) -'A'] = node
		}
	}
}

func (b *BranchNode) GetBranch(bit interface{}) Node {
	switch bit.(type) {
	case int:
		return b.Branches[bit.(int)]
	case byte:
		if bit.(byte) > 'Z' {
			return b.Branches[bit.(byte) -'a'+26]
		} else {
			return b.Branches[bit.(byte) -'A']
		}
	}
	return nil
}

func (b *BranchNode) RemoveBranch(bit byte) {
	if bit > 'Z' {
		b.Branches[bit-'a'+26] = nil
	} else {
		b.Branches[bit-'A'] = nil
	}
}

func (b *BranchNode) SetValue(value map[int][]byte, content map[int]string) {
	b.Value = value
	b.Content = content
}

func (b *BranchNode) RemoveValue() {
	b.Value = nil
}

func (b BranchNode) HasValue() bool {
	return b.Value != nil
}

func (b BranchNode) Hash() []byte {
	h := md5.New()
	h.Write(b.Serialize())
	return h.Sum(nil)
	//return crypto.Keccak256(b.Serialize())
}

func (b BranchNode) Raw() []interface{} {
	hashes := make([]interface{}, BranchSize+1)
	for i := 0; i < BranchSize; i++ {
		if b.Branches[i] == nil {
			hashes[i] = " "
		} else {
			node := b.Branches[i]
			hashes[i] = node.Hash()
		}
	}
	var valueStr []string
	var keys []int
	for k, _ := range b.Value {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, v := range keys {
		valueStr = append(valueStr, strconv.Itoa(v))
		valueStr = append(valueStr, string(b.Value[v]))
		valueStr = append(valueStr, b.Content[v])
	}
	hashes[BranchSize] = valueStr
	return hashes
}

func (b BranchNode) Serialize() []byte {
	return Serialize(b)
}

