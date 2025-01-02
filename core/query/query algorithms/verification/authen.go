package verification

import (
	"Corgi/matching"
	"Corgi/mpt"
	"crypto/md5"
	"fmt"
	"github.com/ethereum/go-ethereum/rlp"
)

type VO struct {
	/*
		NodeList: save the visited nodes of the MVPTree and the digest of the siblings of the visited nodes
		CSG: save the search space
		FP: save the false positive vertices during pruning, each []int has three elements, first is the belonging candidate set, the second is the unconnected candidate set, the third is the false positive vertex id
		UV: save the un-result vertices during matching
		RS: save all results
		ExpandID: the expanding query vertex id
	*/
	NodeList  []mpt.Node
	NodeListB map[mpt.Node]bool
	CSG       map[int][]int
	FP        [][]int
	UV        map[int][]int
	RS        []map[int]int
	CSGMatrix map[int]map[int]bool
	ExpandID  int
	TP        int
	Filtered  int
}

type potentialPath struct {
	key  []byte
	node mpt.Node
}

type OneProof struct {
	MS  []map[int]int
	CSG map[int][]int
}

func (vo *VO) Authentication(query matching.QueryGraph, RD []byte) bool {
	/*
		Verifying whether the matching result satisfy correctness and completeness
	*/
	// verifying first phase
	// search MVPTree and recompute the root digest based on VO.N and VO.CSG
	CS := make(map[int][]int)
	CSB := make(map[int]map[int]bool)
	totalCandi := 0
	for str, ul := range query.NeiStr {
		//fmt.Println("verify key: ", str)
		tempB := make(map[int]bool)
		C := reSearch([]byte(str), vo.NodeList, vo.NodeListB, vo.CSG)
		if len(C) == 0 {
			return false
		}
		for _, c := range C {
			tempB[c] = true
		}
		for _, u := range ul {
			CSB[u] = tempB
			CS[u] = C
			totalCandi = totalCandi + len(C)
		}
	}
	vo.Filtered = totalCandi - vo.Filtered
	RRD := vo.NodeList[0].Hash()
	if string(RRD) != string(RD) {
		return false
	}

	// verifying second phase
	vo.CSGMatrix = make(map[int]map[int]bool)
	for k, j := range vo.CSG {
		vo.CSGMatrix[k] = make(map[int]bool)
		for _, n := range j {
			vo.CSGMatrix[k][n] = true
		}
	}
	if len(vo.FP) > 0 {
		for _, pair := range vo.FP {
			for _, n := range vo.CSG[pair[2]] {
				if query.CandidateSetsB[pair[1]][n] {
					return false
				}
			}
		}
	}
	NumRS := 0
	for _, v := range query.CandidateSets[vo.ExpandID] {
		NumRS = NumRS + len(vo.ExEnum(v, vo.ExpandID, query))
	}

	if len(vo.RS) != NumRS {
		return false
	}
	return true
}

func reSearch(key []byte, nodeList []mpt.Node, nodeListB map[mpt.Node]bool, CSG map[int][]int) []int {
	/*
		Search the MVPTree to obtain the CS and check the completeness of the CS then recomputing the root digest
	*/

	rootNode := nodeList[0]
	var result []int
	if len(key) == 0 {
		return nil
	}
	if root, ok := rootNode.(*mpt.BranchNode); ok {
		//fmt.Println("branch node")
		node := root.GetBranch(key[0])
		key = key[1:]
		var latence []potentialPath
		for {
			if mpt.IsEmptyNode(node) {
				if len(latence) == 0 {
					return result
				}
				key = latence[0].key
				node = latence[0].node
				latence = latence[1:]
			}

			if leaf, ok := node.(*mpt.LeafNode); ok {
				//fmt.Println("leaf node")
				matched := mpt.PrefixMatchedLen(leaf.Path, key)
				if matched == len(key) || mpt.IsContain(leaf.Path[matched:], key[matched:]) {
					for k, _ := range leaf.Value {
						result = append(result, k)
						if _, yes := CSG[k]; yes {
							h := md5.New()
							h.Write(Serialize(CSG[k]))
							leaf.Value[k] = h.Sum(nil)
							//leaf.Value[k] = crypto.Keccak256(Serialize(CSG[k]))
						}
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

			if branch, ok := node.(*mpt.BranchNode); ok {
				//fmt.Println("branch node")
				// check the unsatisfied branches are indeed unsatisfied
				if len(key) == 0 {
					//_, pendPaths := checkBranch(key, node, nodeList, relation)
					right, pendPaths := checkBranch(key, node, nodeListB)
					if !right {
						return nil
					}
					latence = append(latence, pendPaths...)
					for k, _ := range branch.Value {
						result = append(result, k)
						if _, yes := CSG[k]; yes {
							h := md5.New()
							h.Write(Serialize(CSG[k]))
							branch.Value[k] = h.Sum(nil)
							//branch.Value[k] = crypto.Keccak256(Serialize(CSG[k]))
						}
					}
					if len(latence) == 0 {
						return result
					}
					key = latence[0].key
					node = latence[0].node
					latence = latence[1:]
					continue
				} else {
					right, pendPaths := checkBranch(key, node, nodeListB)
					if !right {
						return nil
					}
					latence = append(latence, pendPaths...)
					b, remaining := key[0], key[1:]
					key = remaining
					node = branch.GetBranch(b)
					continue
				}
			}

			if ext, ok := node.(*mpt.ExtensionNode); ok {
				//fmt.Println("extension node")
				matched := mpt.PrefixMatchedLen(ext.Path, key)
				if matched < len(ext.Path) && matched < len(key) {
					if ext.Path[len(ext.Path)-1] < key[matched] {
						key = key[matched:]
						node = ext.Next
						continue
					} else {
						containAll, i := mpt.ContainJudge(ext.Path[matched:], key[matched:])
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
	return nil
}

func checkBranch(key []byte, b mpt.Node, nodeListB map[mpt.Node]bool) (bool, []potentialPath) {
	/*
		Checking whether the branch that needs to be added is empty
	*/
	if branch, yes := b.(*mpt.BranchNode); yes {
		var subBranches int8
		if len(key) == 0 {
			subBranches = mpt.BranchSize
		} else {
			subBranches = int8(len(branch.Branches[:key[0]-'A']))
		}
		var result []potentialPath
		var i int8
		for i = 0; i < subBranches; i++ {
			if branch.Branches[i] != nil {
				if !nodeListB[branch.Branches[i]] {
					return false, nil
				}
				p := potentialPath{key, branch.Branches[i]}
				result = append(result, p)
			}
		}
		return true, result
	}
	return false, nil
}

func Serialize(nei []int) []byte {
	raw := []interface{}{}
	for _, n := range nei {
		raw = append(raw, byte(n))
	}
	rlp, err := rlp.EncodeToBytes(raw)
	if err != nil {
		fmt.Println(err)
	}
	return rlp
}

func (vo *VO) ExEnum(candidateId, expandQId int, query matching.QueryGraph) []map[int]int {
	/*
		Expanding the data graph from the given candidate vertex to enumerate matching results and collect verification objects
	*/
	var MS []map[int]int

	expL := 1
	preMatched := make(map[int]int)
	preMatched[expandQId] = candidateId
	vo.Match(expL, expandQId, query, preMatched, &MS)
	return MS
}

func (vo *VO) Match(expL int, expQId int, query matching.QueryGraph, preMatched map[int]int, MS *[]map[int]int) {
	/*
		Authenticated recursively enumerating each layer's matched results then generating final results
		expL: current expanding layer
		expQId: the starting expansion query vertex
		preMatched: already matched part
		MS: save the results
	*/
	if expL > len(query.QVList[expQId].ExpandLayer) {
		return
	}
	// 1. get the query vertices of the current layer as well as each vertex's candidate set
	qPresentVer := query.QVList[expQId].ExpandLayer[expL]

	// 2. get the graph vertices of the current layer and classify them (Exploration)
	visited := make(map[int]bool)
	for _, v := range preMatched {
		visited[v] = true
	}
	var gVer []int // the graph vertices need to be expanded in current layer
	var curRes []map[int]int
	classes := make(map[int][]int)
	if expL == 1 {
		cVer := preMatched[expQId]
		for _, n := range vo.CSG[cVer] {
			if !visited[n] { // get one unvisited graph vertex n of the current layer
				for _, c := range qPresentVer { // check current graph vertex n belong to which query vertex's candidate set
					if query.CandidateSetsB[c][n] { // graph vertex n may belong to the candidate set of query vertex c
						classes[c] = append(classes[c], n)
					}
				}
			}
		}
		// if one of query vertices' candidate set is empty then return
		if len(classes) < len(qPresentVer) {
			return
		}
		// 3. obtain current layer's matched results (Enumeration)
		curRes = vo.ObtainCurRes(classes, query, qPresentVer)
		// if present layer has no media result then return
		if len(curRes) == 0 {
			return
		}
	} else {
		for _, q := range query.QVList[expQId].PendingExpand[expL-1] {
			gVer = append(gVer, preMatched[q])
		}
		repeat := make(map[int]bool) // avoid visited repeat vertex in current layer
		for _, v := range gVer {     // expand each graph vertex of current layer
			for _, n := range vo.CSG[v] {
				if !visited[n] && !repeat[n] { // get one unvisited graph vertex n of the current layer
					repeat[n] = true
					for _, c := range qPresentVer { // check current graph vertex n belong to which query vertex's candidate set
						fg := true
						if query.CandidateSetsB[c][n] { // graph vertex n may belong to the candidate set of query vertex c
							for pre, _ := range preMatched { // check whether the connectivity of query vertex c with its pre vertices and the connectivity of graph vertex n with its correspond pre vertices are consistent
								if query.Matrix[c][pre] && !vo.CSGMatrix[n][preMatched[pre]] { // not consist
									fg = false
									break
								}
							}
							if fg { // graph vertex n indeed belong to the candidate set of query vertex c
								classes[c] = append(classes[c], n)
							}
						}
					}
				}
			}
		}
		// if one of query vertices' candidate set is empty then return
		if len(classes) < len(qPresentVer) {
			return
		}
		// 3. obtain current layer's matched results (Enumeration)
		curRes = vo.ObtainCurRes(classes, query, qPresentVer)
		// if present layer has no media result then return
		if len(curRes) == 0 {
			return
		}
	}

	// 4. combine current layer's result with pre result
	totalRes := curRes
	for _, cur := range totalRes {
		for k, v := range preMatched {
			cur[k] = v
		}
	}

	// 5. if present layer is the last layer then add the filterMedia into res
	if expL == len(query.QVList[expQId].ExpandLayer) {
		*MS = append(*MS, totalRes...)
		return
	} else {
		// else continue matching
		for _, eachM := range totalRes {
			vo.Match(expL+1, expQId, query, eachM, MS)
		}
	}
}

func (vo *VO) ExEnumEdge(candidateId, expandQId int, query matching.QueryGraph, num int, class []int) []map[int]int {
	/*
		Expanding the data graph from the given candidate vertex to enumerate matching results and collect verification objects
	*/
	var MS []map[int]int

	expL := 1
	preMatched := make(map[int]int)
	preMatched[expandQId] = candidateId
	vo.MatchEdge(expL, expandQId, query, preMatched, &MS, num, class)
	return MS
}

func (vo *VO) MatchEdge(expL int, expQId int, query matching.QueryGraph, preMatched map[int]int, MS *[]map[int]int, nu int, class []int) {
	/*
		Authenticated recursively enumerating each layer's matched results then generating final results
		expL: current expanding layer
		expQId: the starting expansion query vertex
		preMatched: already matched part
		MS: save the results
	*/
	if expL > len(query.QVList[expQId].ExpandLayer) {
		return
	}
	// 1. get the query vertices of the current layer as well as each vertex's candidate set
	qPresentVer := query.QVList[expQId].ExpandLayer[expL]

	// 2. get the graph vertices of the current layer and classify them (Exploration)
	visited := make(map[int]bool)
	for _, v := range preMatched {
		visited[v] = true
	}
	var gVer []int // the graph vertices need to be expanded in current layer
	var curRes []map[int]int
	classes := make(map[int][]int)
	if expL == 1 {
		cVer := preMatched[expQId]
		classes[nu] = class
		for _, n := range vo.CSG[cVer] {
			if !visited[n] { // get one unvisited graph vertex n of the current layer
				for _, c := range qPresentVer { // check current graph vertex n belong to which query vertex's candidate set
					if c != nu && query.CandidateSetsB[c][n] { // graph vertex n may belong to the candidate set of query vertex c
						classes[c] = append(classes[c], n)
					}
				}
			}
		}
		// if one of query vertices' candidate set is empty then return
		if len(classes) < len(qPresentVer) {
			return
		}

		// 3. obtain current layer's matched results (Enumeration)
		curRes = vo.ObtainCurRes(classes, query, qPresentVer)
		// if present layer has no media result then return
		if len(curRes) == 0 {
			return
		}
	} else {
		for _, q := range query.QVList[expQId].PendingExpand[expL-1] {
			gVer = append(gVer, preMatched[q])
		}
		repeat := make(map[int]bool) // avoid visited repeat vertex in current layer
		for _, v := range gVer {     // expand each graph vertex of current layer
			for _, n := range vo.CSG[v] {
				if !visited[n] && !repeat[n] { // get one unvisited graph vertex n of the current layer
					repeat[n] = true
					for _, c := range qPresentVer { // check current graph vertex n belong to which query vertex's candidate set
						fg := true
						if query.CandidateSetsB[c][n] { // graph vertex n may belong to the candidate set of query vertex c
							for pre, _ := range preMatched { // check whether the connectivity of query vertex c with its pre vertices and the connectivity of graph vertex n with its correspond pre vertices are consistent
								if query.Matrix[c][pre] && !vo.CSGMatrix[n][preMatched[pre]] { // not consist
									fg = false
									break
								}
							}
							if fg { // graph vertex n indeed belong to the candidate set of query vertex c
								classes[c] = append(classes[c], n)
							}
						}
					}
				}
			}
		}
		// if one of query vertices' candidate set is empty then return
		if len(classes) < len(qPresentVer) {
			return
		}
		// 3. obtain current layer's matched results (Enumeration)
		curRes = vo.ObtainCurRes(classes, query, qPresentVer)
		// if present layer has no media result then return
		if len(curRes) == 0 {
			return
		}
	}

	// 4. combine current layer's result with pre result
	totalRes := curRes
	for _, cur := range totalRes {
		for k, v := range preMatched {
			cur[k] = v
		}
	}

	// 5. if present layer is the last layer then add the filterMedia into res
	if expL == len(query.QVList[expQId].ExpandLayer) {
		*MS = append(*MS, totalRes...)
		return
	} else {
		// else continue matching
		for _, eachM := range totalRes {
			vo.MatchEdge(expL+1, expQId, query, eachM, MS, nu, class)
		}
	}
}

func (vo *VO) ObtainCurRes(classes map[int][]int, query matching.QueryGraph, qVer []int) []map[int]int {
	/*
		Obtain current layer's matched results
	*/

	var matchedRes []map[int]int

	// find all edges between query vertices in current layer
	qVerCurAdj := make(map[int][]int)
	for i := 0; i < len(qVer); i++ {
		qVerCurAdj[qVer[i]] = []int{}
		for j := 0; j < len(qVer); j++ {
			if query.Matrix[qVer[i]][qVer[j]] {
				qVerCurAdj[qVer[i]] = append(qVerCurAdj[qVer[i]], qVer[j])
			}
		}
	}

	// using BFS find all connected part, meanwhile generating part results
	visited := make(map[int]bool)
	var queue []int
	var partResults []map[int][]int
	for _, k := range qVer {
		if !visited[k] {
			visited[k] = true
			queue = append(queue, k)
			onePartRes := make(map[int][]int)
			onePartRes[k] = classes[k]
			//sort.Ints(onePartRes[k])
			for len(queue) != 0 {
				v := queue[0]
				queue = queue[1:]
				for _, n := range qVerCurAdj[v] {
					if !visited[n] {
						visited[n] = true
						queue = append(queue, n)
						onePartRes = vo.join(onePartRes, v, n, classes[n], qVerCurAdj[n])
					}
				}
			}
			if len(onePartRes) != 0 {
				partResults = append(partResults, onePartRes)
			} else {
				return matchedRes
			}
		}
	}
	if len(partResults) == 0 {
		return matchedRes
	}
	// combine all part results
	var agent []int
	for _, par := range partResults {
		for k, _ := range par {
			agent = append(agent, k)
			break
		}
	}
	oneRes := make(map[int]int)
	matching.ProductPlus(partResults, &matchedRes, agent, 0, oneRes)
	return matchedRes
}

func (vo *VO) join(curRes map[int][]int, v1, v2 int, v2Candi, v2Nei []int) map[int][]int {
	/*
		Join the vertex v2 to current results
	*/
	newCurRes := make(map[int][]int)
	for i, c1 := range curRes[v1] {
		for _, c2 := range v2Candi {
			fg := false
			if vo.CSGMatrix[c1][c2] {
				fg = true
				// judge the connectivity with other matching vertices
				for _, n := range v2Nei { // check each neighbor of v2 whether in matched res or not
					if _, ok := curRes[n]; ok { // neighbor belong to res
						if !vo.CSGMatrix[curRes[n][i]][c2] { // the connectivity is not satisfied
							fg = false
							break
						}
					}
				}
				// satisfy the demand so that produce a new match
				if fg {
					for k, _ := range curRes {
						newCurRes[k] = append(newCurRes[k], curRes[k][i])
					}
					newCurRes[v2] = append(newCurRes[v2], c2)
				}
			}
		}
	}
	return newCurRes
}

func (vo *VO) Size() {
	/*
		Counting the size of the Proof
	*/
	hashL := 16
	intL := 4
	NodeSize := 0
	if len(vo.NodeList) != 0 {
		// count VO.NodeList
		nodeMap := make(map[mpt.Node]bool)
		for _, node := range vo.NodeList {
			if hs, ok := node.(mpt.HashNode); ok {
				hashSize := len(hs.Hash())
				NodeSize = NodeSize + hashSize
			} else {
				if !nodeMap[node] {
					nodeMap[node] = true
					if leaf, ok := node.(*mpt.LeafNode); ok {
						leafSize := len(leaf.Path) + len(leaf.Value)*(intL+hashL)
						NodeSize = NodeSize + leafSize
					} else if branch, ok := node.(*mpt.BranchNode); ok {
						branchSize := mpt.BranchSize + len(branch.Value)*(intL+hashL)
						NodeSize = NodeSize + branchSize
					} else if ext, ok := node.(*mpt.ExtensionNode); ok {
						extSize := len(ext.Path)
						NodeSize = NodeSize + extSize
					}
				}
			}
		}
	} else {
		for _, s := range vo.CSG {
			NodeSize = NodeSize + (len(s)+1)*intL
		}
	}
	// count VO.CSG
	CSGSize := 0
	for _, s := range vo.CSG {
		CSGSize = CSGSize + (len(s)+1)*intL
	}
	FPSize := 0
	for _, f := range vo.FP {
		FPSize = FPSize + (len(f))*intL
	}
	UVSize := 0
	for _, fs := range vo.UV {
		UVSize = UVSize + len(fs)*intL
	}

	fmt.Println("the VO size is: ", (NodeSize+CSGSize+FPSize+UVSize)/1024, "KB")
}
