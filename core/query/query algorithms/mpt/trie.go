package mpt

import (
	"errors"
	"fmt"
)

type potentialPath struct {
	key []byte
	node Node
}

type Trie struct {
	root Node
}

func (t *Trie) GetRoot() Node {
	return t.root
}

func NewTrie() *Trie {
	return &Trie{}
}

func (t *Trie) Insert(key []byte, id int, hs []byte, con string) error {
	/*
	Inserting (key, value) into trie
	key: the key to be inserted
	id: the id of the vertex to be inserted
	hs: the neighborhood hash of the vertex to be inserted
	con: the content of the vertex to be inserted
	*/

	if len(key) == 0 {
		return errors.New("the key is empty")
	}
	node := &t.root
	var pre = node
	var recordB byte
	value := make(map[int][]byte)
	content := make(map[int]string)
	value[id] = hs
	content[id] = con
	for {
		if IsEmptyNode(*node) {
			leaf := NewLeafNode(key, value, content)
			*node = leaf
			return nil
		}

		if leaf, ok := (*node).(*LeafNode); ok {
			matched := PrefixMatchedLen(leaf.Path, key)
			// first case: full matched
			if matched == len(key) && matched == len(leaf.Path) {
				leaf.Value[id] = hs
				leaf.Content[id] = con
				return nil
			}
			// second case: no matched
			branch := NewBranchNode()
			if matched == 0 {
				if preBranch, yes := (*pre).(*BranchNode); yes {
					preBranch.SetBranch(recordB, branch)
				}
				*node = branch
				if len(key) == 0 {
					branch.SetValue(value, content)
					oldLeaf := NewLeafNode(leaf.Path[1:], leaf.Value, leaf.Content)
					branch.SetBranch(leaf.Path[0], oldLeaf)
					return nil
				}
				if len(leaf.Path) == 0 {
					branch.SetValue(leaf.Value, leaf.Content)
					newLeaf := NewLeafNode(key[1:], value, content)
					branch.SetBranch(key[0], newLeaf)
					return nil
				}
				oldLeaf := NewLeafNode(leaf.Path[1:], leaf.Value, leaf.Content)
				branch.SetBranch(leaf.Path[0],oldLeaf)
				newLeaf := NewLeafNode(key[1:], value, content)
				branch.SetBranch(key[0], newLeaf)
				return nil
			}
			// third case: part matched
			ext := NewExtensionNode(leaf.Path[:matched], branch)
			*node = ext
			if preBranch, yes := (*pre).(*BranchNode); yes {
				preBranch.SetBranch(recordB, ext)
			}
			if matched == len(leaf.Path) {
				branch.SetValue(leaf.Value, leaf.Content)
				branchKey, leafKey := key[matched], key[matched+1:]
				newLeaf := NewLeafNode(leafKey, value, content)
				branch.SetBranch(branchKey, newLeaf)
			} else if matched == len(key) {
				branch.SetValue(value, content)
				oldBranchKey, oldLeafKey := leaf.Path[matched], leaf.Path[matched+1:]
				oldLeaf := NewLeafNode(oldLeafKey, leaf.Value, leaf.Content)
				branch.SetBranch(oldBranchKey, oldLeaf)
			} else {
				oldBranchKey, oldLeafKey := leaf.Path[matched], leaf.Path[matched+1:]
				oldLeaf := NewLeafNode(oldLeafKey, leaf.Value, leaf.Content)
				branch.SetBranch(oldBranchKey, oldLeaf)
				branchKey, leafKey := key[matched], key[matched+1:]
				newLeaf := NewLeafNode(leafKey, value, content)
				branch.SetBranch(branchKey, newLeaf)
			}
			return nil
		}

		if branch, ok := (*node).(*BranchNode); ok {
			if len(key) == 0 {
				if branch.Value != nil{
					branch.Value[id] = hs
					branch.Content[id] = con
				} else {
					branch.SetValue(value, content)
				}
				return nil
			}
			pre = node
			recordB = key[0]
			b, remaining := key[0], key[1:]
			key = remaining
			tmp := branch.GetBranch(b)
			if tmp == nil {
				leaf := NewLeafNode(key, value, content)
				branch.SetBranch(b, leaf)
				return nil
			} else {
				node = &tmp
				continue
			}
		}

		if ext, ok := (*node).(*ExtensionNode); ok {
			matched := PrefixMatchedLen(ext.Path, key)
			// first case: full matched
			if  matched == len(ext.Path) {
				key = key[matched:]
				node = &ext.Next
				continue
			}
			// second case: no matched
			branch := NewBranchNode()
			if matched == 0 {
				if preBranch, ok := (*pre).(*BranchNode); ok {
					preBranch.SetBranch(recordB, branch)
				}
				extBranchKey, extRemainingKey := ext.Path[0], ext.Path[1:]
				if len(extRemainingKey) == 0 {
					branch.SetBranch(extBranchKey, ext.Next)
				} else {
					newExt := NewExtensionNode(extRemainingKey, ext.Next)
					branch.SetBranch(extBranchKey, newExt)
				}
				if len(key) == 0 {
					branch.SetValue(value, content)
					*node = branch
				} else {
					leafBranchKey, leafRemainingKey := key[0], key[1:]
					newLeaf := NewLeafNode(leafRemainingKey, value, content)
					branch.SetBranch(leafBranchKey, newLeaf)
					*node = branch
				}
				return nil
			}
			// third case: part matched
			commonKey, branchKey, extRemainingKey := ext.Path[:matched], ext.Path[matched], ext.Path[matched+1:]
			oldExt := NewExtensionNode(commonKey, branch)
			if preBranch, ok := (*pre).(*BranchNode); ok {
				preBranch.SetBranch(recordB, oldExt)
			}
			if len(extRemainingKey) == 0 {
				branch.SetBranch(branchKey, ext.Next)
			} else {
				newExt := NewExtensionNode(extRemainingKey, ext.Next)
				branch.SetBranch(branchKey, newExt)
			}
			if len(commonKey) == len(key) {
				branch.SetValue(value, content)
			} else {
				leafBranchKey, leafRemainingKey := key[matched], key[matched+1:]
				newLeaf := NewLeafNode(leafRemainingKey, value, content)
				branch.SetBranch(leafBranchKey, newLeaf)
			}
			*node = oldExt
			return nil
		}
		panic("unknown type")
	}
}

func (t *Trie) GetExactOne(key []byte) (bool, []int) {
	/*
	Get the target node depends on the given key
	 */

	node := t.root
	result := []int{}
	for {
		if IsEmptyNode(node) {
			return false, nil
		}

		if leaf, ok := node.(*LeafNode); ok {
			fmt.Println("leaf node") // for test
			matched := PrefixMatchedLen(leaf.Path, key)
			if matched != len(leaf.Path) || matched != len(key) {
				return false, nil
			}
			for k, _ := range leaf.Value {
				result = append(result, k)
			}
			return true, result
		}

		if branch, ok := node.(*BranchNode); ok {
			fmt.Println("branch node") // for test
			if len(key) == 0 {
				for k, _ := range branch.Value {
					result = append(result, k)
				}
				return true, result
			}
			b, remaining := key[0], key[1:]
			key = remaining
			node = branch.GetBranch(b)
			continue
		}

		if ext, ok := node.(*ExtensionNode); ok {
			fmt.Println("extension node") // for test
			matched := PrefixMatchedLen(ext.Path, key)
			if matched < len(ext.Path) {
				return false, nil
			}
			key = key[matched:]
			node = ext.Next
			continue
		}
		panic("not found")
	}
}

func (t *Trie) GetCandidate(key []byte) []int{
	/*
	get results that include given key
	 */

	var result []int
	if len(key) == 0 {
		return result
	}
	if root, ok := t.root.(*BranchNode); ok {
		node := root.GetBranch(key[0])
		key = key[1:]
		var latence []potentialPath
		for {
			if IsEmptyNode(node) {
				if len(latence) == 0 {
					return result
				}
				key = latence[0].key
				node = latence[0].node
				latence = latence[1:]
			}

			if leaf, ok := node.(*LeafNode); ok {
				//fmt.Println("leaf node")
				matched := PrefixMatchedLen(leaf.Path, key)
				if matched == len(key) || IsContain(leaf.Path[matched:], key[matched:]){
					for k, _ := range leaf.Value {
						result = append(result, k)
					}
				}
				if len(latence) == 0 {
					return result
				}
				key = latence[0].key
				node = latence[0].node
				latence = latence[1:]
				continue
			}

			if branch, ok := node.(*BranchNode); ok {
				//fmt.Println("branch node")
				if len(key) == 0 {
					latence = append(latence, ToBeAdd(key, *branch)...)
					for k, _ := range branch.Value {
						result = append(result, k)
					}
					if len(latence) == 0 {
						return result
					}
					key = latence[0].key
					node = latence[0].node
					latence = latence[1:]
					continue
				} else {
					latence = append(latence, ToBeAdd(key, *branch)...)
					b, remaining := key[0], key[1:]
					key = remaining
					node = branch.GetBranch(b)
					continue
				}
			}

			if ext, ok := node.(*ExtensionNode); ok {
				//fmt.Println("extension node")
				matched := PrefixMatchedLen(ext.Path, key)
				if matched < len(ext.Path) && matched < len(key){
					if ext.Path[len(ext.Path)-1] < key[matched] {
						key = key[matched:]
						node = ext.Next
						continue
					} else {
						containAll, i := ContainJudge(ext.Path[matched:], key[matched:])
						if containAll{
							key = []byte{}
							node = ext.Next
							continue
						} else if ext.Path[len(ext.Path)-1] < key[i] {
							key = key[i:]
							node = ext.Next
							continue
						} else {
							if len(latence) == 0 {
								return result
							}
							key = latence[0].key
							node = latence[0].node
							latence = latence[1:]
							continue
						}
					}
				} else {
					key = key[matched:]
					node = ext.Next
					continue
				}
			}
		}
	}
	return result
}

func (t *Trie)PrintTrie() {
	if t.root == nil {
		return
	}
	PrintNode(t.root)
	return
}

func (t *Trie) HashRoot() []byte {
	/*
	computing the root hash
	*/
	if t.root == nil {
		return nil
	}
	hashed := hash(&t.root)
	return hashed
}

func hash(node *Node) []byte {
	/*
	computing root hash of the subtree corresponding to the given node
	 */
	switch (*node).(type) {
	case *LeafNode:
		leaf, _ := (*node).(*LeafNode)
		hashed := leaf.Hash()
		leaf.flags.hash = hashed
		return hashed
	case *ExtensionNode:
		ext, _ := (*node).(*ExtensionNode)
		hash(&ext.Next)
		ext.flags.hash = ext.Hash()
		return ext.flags.hash
	case *BranchNode:
		branch, _ := (*node).(*BranchNode)
		for i:=0; i < BranchSize; i++ {
			if child := branch.Branches[i]; child != nil {
				hash(&child)
			}
		}
		branch.flags.hash = branch.Hash()
		return branch.flags.hash
	}
	return nil
}

func PrintNode(node Node) {
	switch (node).(type) {
	case *LeafNode:
		leaf, _ := (node).(*LeafNode)
		fmt.Println("LeafNode hash: ", leaf.flags.hash)
		//if len(leaf.Value) > 100 {
		//	fmt.Println("the number of elements in leaf node: ", len(leaf.Value))
		//}
		return
	case *ExtensionNode:
		ext, _ := (node).(*ExtensionNode)
		fmt.Println("ExtensionNode hash: ", ext.flags.hash)
		PrintNode(ext.Next)
		return
	case *BranchNode:
		branch, _ := (node).(*BranchNode)
		fmt.Println("BranchNode hash: ", branch.flags.hash)
		//fmt.Println("the number of elements in branch node: ", len(branch.Value))
		for i:=0; i<BranchSize; i++ {
			if child := branch.Branches[i]; child != nil {
				PrintNode(child)
			}
		}
		return
	}
	return
}

func PrefixMatchedLen(node1, node2 []byte) int {
	matched := 0
	for i := 0; i < len(node1) && i < len(node2); i++ {
		n1, n2 := node1[i], node2[i]
		if n1 == n2 {
			matched++
		} else {
			break
		}
	}
	return matched
}

func IsContain(node1, node2 []byte) bool {
	/*
	Judge the key of node1 whether contain the key of the node2
	*/

	for _, v := range node1 {
		if len(node2) == 0 {
			return true
		} else {
			if v > node2[0] {
				return false
			} else if v == node2[0] {
				node2 = node2[1:]
				continue
			}
		}
	}
	if len(node2) != 0 {
		return false
	} else {
		return true
	}
}

func ContainJudge(node1, node2 []byte) (bool, int) {
	/*
	Judge the key of node1 whether contain the key of the node2, if not, return the position in node2
	 */

	i := 0
	for _, v := range node1 {
		if len(node2) == 0 {
			return true, -1
		} else {
			if v > node2[0] {
				return false, i
			} else if v == node2[0] {
				i = i + 1
				node2 = node2[1:]
				continue
			}
		}
	}
	if len(node2) != 0 {
		return false, i
	} else {
		return true, -1
	}
}

func ToBeAdd(key []byte, node BranchNode) []potentialPath {
	var subBranches []Node
	if len(key) == 0 {
		subBranches = node.Branches[:len(node.Branches)]
	} else {
		if key[0] > 'Z' {
			subBranches = node.Branches[:key[0]-'a'+26]
		} else {
			subBranches = node.Branches[:key[0]-'A']
		}
	}

	var result []potentialPath
	for _, v := range subBranches{
		if IsEmptyNode(v) {
			continue
		}
		p := potentialPath{key, v}
		result = append(result, p)
	}
	return result
}
