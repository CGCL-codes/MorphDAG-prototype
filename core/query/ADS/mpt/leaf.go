package mpt

import (
	"crypto/md5"
	"sort"
	"strconv"
)


type LeafNode struct {
	Path []byte
	Value map[int][]byte
	Content map[int]string
	flags nodeFlag
}

func NewLeafNode(key []byte, value map[int][]byte, content map[int]string) *LeafNode {
	return &LeafNode{
		Path:  key,
		Value: value,
		Content: content,
		flags: newFlag(),
	}
}

func (l LeafNode) Hash() []byte {
	h := md5.New()
	h.Write(l.Serialize())
	return h.Sum(nil)
	//return crypto.Keccak256(l.Serialize())
}

func (l LeafNode) Raw() []interface{} {
	path := l.Path
	var valueStr []string
	var keys []int
	for k, _ := range l.Value {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, v := range keys {
		valueStr = append(valueStr, strconv.Itoa(v))
		valueStr = append(valueStr, string(l.Value[v]))
		valueStr = append(valueStr, l.Content[v])
	}
	raw := []interface{}{path, valueStr}
	return raw
}

func (l LeafNode) Serialize() []byte {
	return Serialize(l)
}
