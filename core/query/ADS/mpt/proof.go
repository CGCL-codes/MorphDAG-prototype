package mpt

import (
	"Corgi/preprocess"
	"fmt"
	"time"
)

func (t *Trie) AuthFilter(q *matching.QueryGraph) ([]Node, map[Node]bool, int) {
	/*
		obtaining candidate vertex set and merkle proof for all query vertices
	*/
	nodeBs := make(map[Node]bool)
	var nodeList []Node
	nodeListB := make(map[Node]bool)
	q.CandidateSets = make(map[int][]int)
	q.CandidateSetsB = make(map[int]map[int]bool)
	startT1 := time.Now()
	for str, ul := range q.NeiStr {
		C, P, nodeB, _ := t.AuthSearch([]byte(str))
		for k, _ := range nodeB {
			nodeBs[k] = true
		}
		//fmt.Println("present vertex: ", ul, " key: ", str, " candidates: ", len(C))
		for _, u := range ul {
			q.CandidateSets[u] = C
			q.CandidateSetsB[u] = make(map[int]bool)
			for _, v := range C {
				q.CandidateSetsB[u][v] = true
			}
		}
		for _, node := range P {
			if !nodeListB[node] {
				nodeListB[node] = true
				nodeList = append(nodeList, node)
			}
		}
	}
	// reset the pointing relationship of each node in VO.NodeList
	for _, node := range nodeList {
		switch node.(type) {
		case *LeafNode:
			continue
		case *BranchNode:
			branch, _ := (node).(*BranchNode)
			for i, child := range branch.Branches {
				if IsEmptyNode(child) || nodeListB[child] {
					continue
				} else {
					var hashNode HashNode
					if leaf, ok := child.(*LeafNode); ok {
						hashNode = NewHashNode(leaf.flags.hash)
					} else if bran, y := child.(*BranchNode); y {
						hashNode = NewHashNode(bran.flags.hash)
					} else {
						hashNode = NewHashNode(child.(*ExtensionNode).flags.hash)
					}
					branch.SetBranch(i, hashNode)
					nodeList = append(nodeList, hashNode)
				}
			}
		case *ExtensionNode:
			ext, _ := (node).(*ExtensionNode)
			if !nodeListB[ext.Next] {
				hashNode := NewHashNode(ext.Next.(*BranchNode).flags.hash)
				ext.SetNext(hashNode)
				nodeList = append(nodeList, hashNode)
			}
		}
	}
	time1 := time.Since(startT1)
	fmt.Println("phase 1 SP CPU time is: ", time1)
	return nodeList, nodeListB, len(nodeBs)
}

func (t *Trie) AuthSearch(key []byte) ([]int, []Node, map[Node]bool, bool) {
	/*
		obtaining candidate vertex set and merkle proof for the given key (one query vertex)
	*/
	nodeB := make(map[Node]bool)
	var vNodes []Node
	if len(key) == 0 {
		return nil, vNodes, nodeB, false
	}
	var result []int
	if root, ok := t.root.(*BranchNode); ok {
		//fmt.Println("branch node")
		node := root.GetBranch(key[0])
		key = key[1:]
		vNodes = append(vNodes, root)
		var latence []potentialPath
		for {
			if IsEmptyNode(node) {
				if len(latence) == 0 {
					break
				}
				key = latence[0].key
				node = latence[0].node
				latence = latence[1:]
			}

			if leaf, ok := node.(*LeafNode); ok {
				//fmt.Println("leaf node")
				vNodes = append(vNodes, leaf)
				matched := PrefixMatchedLen(leaf.Path, key)
				if matched == len(key) || IsContain(leaf.Path[matched:], key[matched:]) {
					nodeB[leaf] = true
					for k, _ := range leaf.Value {
						result = append(result, k)
					}
				}
				if len(latence) == 0 {
					break
				}
				key = latence[0].key
				node = latence[0].node
				latence = latence[1:]
				continue
			}

			if branch, ok := node.(*BranchNode); ok {
				//fmt.Println("branch node")
				vNodes = append(vNodes, branch)
				if len(key) == 0 {
					nodeB[branch] = true
					latence = append(latence, ToBeAdd(key, *branch)...)
					for k, _ := range branch.Value {
						result = append(result, k)
					}
					if len(latence) == 0 {
						break
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
				vNodes = append(vNodes, ext)
				matched := PrefixMatchedLen(ext.Path, key)
				if matched < len(ext.Path) && matched < len(key) {
					if ext.Path[len(ext.Path)-1] < key[matched] {
						key = key[matched:]
						node = ext.Next
						continue
					} else {
						containAll, i := ContainJudge(ext.Path[matched:], key[matched:])
						if containAll {
							key = []byte{}
							node = ext.Next
							continue
						} else if ext.Path[len(ext.Path)-1] < key[i] {
							key = key[i:]
							node = ext.Next
							continue
						} else {
							if len(latence) == 0 {
								break
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

	return result, vNodes, nodeB, true
}
