package matching

import (
	"fmt"
)



type Proof struct {
	/*
	RS: save the matching results
	CSG: save the search space
	FP: save the false positive candidate vertices during pruning
	UV: save the un-result vertices during matching
	CSGRe: save the search space that removed duplicate vertices
	*/
	RS []map[int]int
	CSG map[int][]int
	FP [][]int
	UV map[int][]int
	ExpandID int
	CSGRe map[int][]int
	Filtered int
}

func (g *Graph) RemoveDup(query QueryGraph) (map[int][]int, map[int]map[int]bool){
	/*
	Remove the duplicate vertices in each candidate set
	 */
	tempCS := make(map[int][]int)
	tempCSB := make(map[int]map[int]bool)
	categoryCS := make(map[int]map[int][]int)
	sum := 0
	for k, cu := range query.CandidateSets {
		visited := make(map[int]bool)
		sum = sum + len(cu)
		categoryCS[k] = make(map[int][]int)
		for _, c := range cu {
			if !visited[c] {
				visited[c] = true
				categoryCS[k][c] = append(categoryCS[k][c], c)
				for _, cc := range cu {
					if !visited[cc] {
						if c != cc && len(g.adj[c]) == len(g.adj[cc]) {
							if Compare(cc, g.adj[c], g.matrix[cc]) {
								visited[cc] = true
								categoryCS[k][c] = append(categoryCS[k][c], cc)
							}
						}
					}
				}
			}
		}
	}
	//fmt.Println("before removing: ", sum)
	sum1 := 0
	for _, bs := range categoryCS {
		sum1 = sum1 + len(bs)
	}
	//fmt.Println("after remove duplicate: ", sum1)

	categoryCSB := make(map[int]map[int]bool)
	expandId := g.GetExpandQueryVertex(query)
	for k, _ := range categoryCS {
		categoryCSB[k] = make(map[int]bool)
		tempCSB[k] = make(map[int]bool)
		var rc []int
		for v, _ := range categoryCS[k] {
			rc = append(rc, v)
			categoryCSB[k][v] = true
			tempCSB[k][v] = true
		}
		tempCS[k] = rc
		if k == expandId {
			query.CandidateSets[k] = rc
			query.CandidateSetsB[k] = categoryCSB[k]
		}
	}
	return tempCS, tempCSB
}

func Compare(v2 int, v1N []int, v2NB map[int]bool) bool{
	/*
	compare whether two vertices have the same adj
	 */
	flag := true
	for _, n := range v1N {
		if !v2NB[n] {
			if n != v2 {
				flag = false
				break
			}
		}
	}
	return flag
}

func (g *Graph) PruningCS(query QueryGraph, start int) [][]int {
	/*
		Pruning the candidate sets according to the connection between query vertices
	*/
	// obtain the base pruning order
	var order []int
	order = append(order, start)
	for _, ul := range query.QVList[start].ExpandLayer {
		order = append(order, ul...)
	}
	FN := make(map[int][]int)
	for i:=0; i<len(order); i++ {
		FN[order[i]] = []int{}
		for j:=i+1; j<len(order); j++ {
			if query.Matrix[order[i]][order[j]] {
				FN[order[i]] = append(FN[order[i]], order[j])
			}
		}
	}

	// obtain the reversed pruning order
	var revOrder []int
	for i:=len(order)-1; i>=0; i-- {
		revOrder = append(revOrder, order[i])
	}
	BN := make(map[int][]int)
	for i:=0; i<len(revOrder); i++ {
		BN[revOrder[i]] = []int{}
		for j:=i+1; j<len(revOrder); j++ {
			if query.Matrix[revOrder[i]][revOrder[j]] {
				BN[revOrder[i]] = append(BN[revOrder[i]], revOrder[j])
			}
		}
	}

	// pruning three times according to the above orders
	time := 3
	var FP [][]int
	pendingCS := make(map[int][]int)
	pendingCSB := make(map[int]map[int]bool)
	//newCS := make(map[int][]int)
	for k, cl := range query.CandidateSets {
		pendingCS[k] = cl
	}
	for {
		if time <= 0 {
			break
		}
		if time % 2 != 0 {
			for _, u := range order {
				var prunC []int
				prunCB := make(map[int]bool)
				for _, v := range pendingCS[u] {
					flag := make(map[int]bool)
					for _, nu := range FN[u] {
						for _, c := range pendingCS[nu] {
							if g.matrix[v][c] {
								flag[nu] = true
								break
							}
						}
						if !flag[nu] {
							pair := []int{u, nu, v}
							FP = append(FP, pair)
							break
						}
					}
					if len(flag) == len(FN[u]) {
						prunC = append(prunC, v)
						prunCB[v] = true
					}
				}
				pendingCS[u] = prunC
				pendingCSB[u] = prunCB
			}
		} else {
			for _, u := range revOrder {
				var prunC []int
				prunCB := make(map[int]bool)
				for _, v := range pendingCS[u] {
					flag := make(map[int]bool)
					for _, nu := range BN[u] {
						for _, c := range pendingCS[nu] {
							if g.matrix[v][c] {
								flag[nu] = true
								break
							}
						}
						if !flag[nu] {
							pair := []int{u, nu, v}
							FP = append(FP, pair)
							break
						}
					}
					if len(flag) == len(BN[u]) {
						prunC = append(prunC, v)
						prunCB[v] = true
					}
				}
				pendingCS[u] = prunC
				pendingCSB[u] = prunCB
			}
		}
		time--
	}
	// check the pruning power
	sum1 := 0
	for _, pc := range pendingCS {
		sum1 = sum1 + len(pc)
	}
	sum2 := 0
	for k, _ := range query.CandidateSets {
		sum2 = sum2 + len(query.CandidateSets[k])
		query.CandidateSets[k] = pendingCS[k]
		query.CandidateSetsB[k] = pendingCSB[k]
	}
	//fmt.Println("before pruning: ", sum2)
	//fmt.Println("after pruning: ", sum1)
	return FP
}

func (g *Graph) BuildPathFeatureTree(query QueryGraph) map[int]map[string]map[int]int {
	/*
	Simulating the embedding tree of MVPTree
	 */
	EmTree := make(map[int]map[string]map[int]int) // int: the query vertex ID; string: the path label; int: the candidate vertex ID; int: the number of the path
	for k, cv := range query.CandidateSets {
		EmTree[k] = make(map[string]map[int]int)
		for _, c := range cv {
			for pth, num := range g.PathFeature[c] {
				if _, ok := EmTree[k][pth]; ok {
					EmTree[k][pth][c] = num
				} else {
					EmTree[k][pth] = make(map[int]int)
					EmTree[k][pth][c] = num
				}
			}
		}
	}
	return EmTree
}

func (g *Graph) PathFeatureFilter(query QueryGraph, EmTree map[int]map[string]map[int]int) {
	/*
	Using path feature to filter the CS
	*/
	sum := 0
	sum1 := 0
	for u, pf := range query.PathFeature {
		sum = sum + len(query.CandidateSets[u])
		if len(pf) != 0 {
			var newC []int
			newB := make(map[int]bool)
			intersect := make(map[int]int)
			for pth, num := range pf {
				for c, count := range EmTree[u][pth] {
					if count >= num {
						if _, ok := intersect[c]; ok {
							intersect[c] = intersect[c] +1
						} else {
							intersect[c] = 1
						}
					}
				}
			}
			for c, n := range intersect {
				if n == len(pf) {
					newC = append(newC, c)
					newB[c] = true
				}
			}
			query.CandidateSets[u] = newC
			query.CandidateSetsB[u] = newB
		}
		sum1 = sum1 + len(query.CandidateSets[u])
	}
	fmt.Println("before path feature filter: ", sum)
	fmt.Println("after path feature filter: ", sum1)
}

func (g *Graph) PathFeatureFilter1(query QueryGraph) {
	/*
	Using path feature to filter the CS
	 */
	sum := 0
	sum1 := 0
	for k, cv := range query.CandidateSets {
		sum = sum + len(cv)
		var newC []int
		newB := make(map[int]bool)
		for _, v := range cv {
			flag := true
			for pa, num := range query.PathFeature[k] {
				if _, yes := g.PathFeature[v][pa]; !yes {
					flag = false
					break
				} else if g.PathFeature[v][pa] < num {
					flag = false
					break
				}
			}
			if flag {
				newC = append(newC, v)
				newB[v] = true
			}
		}
		sum1 = sum1 + len(newC)
		query.CandidateSets[k] = newC
		query.CandidateSetsB[k] = newB
	}
	fmt.Println("before path feature filter: ", sum)
	fmt.Println("after path feature filter: ", sum1)
}

func (g *Graph) AuthMatching(query QueryGraph, proof *Proof) {
	/*
	Obtaining all matching results and their verification objects
	*/
	// use path feature
	//EmTree := g.BuildPathFeatureTree(query)
	//startT1 := time.Now()
	//g.PathFeatureFilter(query, EmTree)
	//time1 := time.Since(startT1)
	//fmt.Println("path feature filter time is: ", time1)

	// obtain CSG
	proof.CSG = make(map[int][]int)
	for _, cl := range query.CandidateSets {
		proof.Filtered = proof.Filtered + len(cl)
		for _, c := range cl {
			proof.CSG[c] = g.adj[c]
		}
	}
	// obtain FP & RS
	//startT2 := time.Now()
	expandId := g.GetExpandQueryVertex(query)
	//fmt.Println("expand vertex: ", expandId)
	proof.ExpandID = expandId
	proof.FP = g.PruningCS(query, expandId)

	for _, candid := range query.CandidateSets[expandId] {
		g.authEE(candid, expandId, query, proof)
	}
	//time2 := time.Since(startT2)
	//fmt.Println("phase 2 SP CPU time is: ", time2)

	//obtain UV
	proof.UV = make(map[int][]int)
	ResMap := make(map[int]map[int]bool)
	for k, _ := range query.QVList {
		ResMap[k] = make(map[int]bool)
	}
	for _, m := range proof.RS {
		for k, v := range m {
			ResMap[k][v] = true
		}
	}
	var ks []int
	for u, cl := range query.CandidateSets {
		ks = append(ks, u)
		for _, c := range cl {
			if !ResMap[u][c] {
				proof.UV[u] = append(proof.UV[u], c)
			}
		}
	}
	//sort.Ints(ks)

	//strCS := ""
	//strRS := ""
	//for _, k := range ks {
	//	strCS = strCS + strconv.Itoa(len(query.CandidateSets[k])) + "/"
	//	strRS = strRS + strconv.Itoa(len(ResMap[k])) + "/"
	//}
	//fmt.Println("CS size: ", strCS)
	//fmt.Println("RS-CS size: ", strRS)

}

func (g *Graph) authEE(candidateId, expandQId int, query QueryGraph, proof *Proof) {
	/*
	Expanding the data graph from the given candidate vertex to enumerate matching results and collect verification objects
	*/
	expL := 1
	preMatched := make(map[int]int)
	preMatched[expandQId] = candidateId
	g.authMatch(expL, expandQId, query, preMatched, proof)
}

func (g *Graph) authMatch(expL int, expQId int, query QueryGraph, preMatched map[int]int, proof *Proof){
	/*
	Authenticated recursively enumerating each layer's matched results then generating final results
	expT: still need expanding times
	expQId: the starting expansion query vertex
	preMatched: already matched part
	oneVer: save the result and auxiliary information
	lastLayerR: the vertex list of result that exist in last layer
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
	classes := make(map[int][]int)
	if expL == 1 {
		cVer := preMatched[expQId]
		for _, n := range g.adj[cVer] {
			if !visited[n] { // get one unvisited graph vertex n of the current layer
				for _, c := range qPresentVer { // check current graph vertex n belong to which query vertex's candidate set
					if query.CandidateSetsB[c][n] { // graph vertex n may belong to the candidate set of query vertex c
						//oneVer.CSG[n] = g.adj[n]
						classes[c] = append(classes[c], n)
					}
				}
			}
		}
	} else {
		for _, q := range query.QVList[expQId].PendingExpand[expL-1] {
			gVer = append(gVer, preMatched[q])
		}
		repeat := make(map[int]bool)  // avoid visited repeat vertex in current layer
		for _, v := range gVer { // expand each graph vertex of current layer
			//oneVer.CSG[v] = g.adj[v]
			for _, n := range g.adj[v] {
				if !visited[n] && !repeat[n] { // get one unvisited graph vertex n of the current layer
					repeat[n] = true
					for _, c := range qPresentVer { // check current graph vertex n belong to which query vertex's candidate set
						fg := true
						if query.CandidateSetsB[c][n] { // graph vertex n may belong to the candidate set of query vertex c
							//oneVer.CSG[n] = g.adj[n]
							for pre, _ := range preMatched { // check whether the connectivity of query vertex c with its pre vertices and the connectivity of graph vertex n with its correspond pre vertices are consistent
								if query.Matrix[c][pre] && !g.matrix[n][preMatched[pre]] { // not consist
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
	}
	// if one of query vertices' candidate set is empty then return
	if len(classes) < len(qPresentVer) {
		return
	}

	// 3. obtain current layer's matched results (Enumeration)
	var curRes []map[int]int
	curRes = g.ObtainCurRes(classes, query, qPresentVer)

	// if present layer has no media result then return
	if len(curRes) == 0 {
		return
	}
	// 4. combine current layer's result with pre result
	for _, cur := range curRes {
		for k, v := range preMatched {
			cur[k] = v
		}
	}

	// 5. if present layer is the last layer then add the filterMedia into res
	if expL == len(query.QVList[expQId].ExpandLayer) {
		proof.RS = append(proof.RS, curRes...)
		return
	} else {
		// else continue matching
		for _, eachM := range curRes {
			g.authMatch(expL+1, expQId, query, eachM, proof)
		}
	}
}

func (g *Graph) BaseFiltering(query *QueryGraph) {
	/*
	Simulating the traversing of LDTree
	 */
	query.CandidateSets = make(map[int][]int)
	query.CandidateSetsB = make(map[int]map[int]bool)
	for k, _ := range query.QVList {
		query.CandidateSetsB[k] = make(map[int]bool)
	}
	for _, v := range g.Vertices {
		for _, u := range query.QVList {
			if v.label == u.Label && len(g.adj[v.id]) >= len(query.Adj[u.Id]) {
				query.CandidateSets[u.Id] = append(query.CandidateSets[u.Id], v.id)
				query.CandidateSetsB[u.Id][v.id] = true
			}
		}
	}
}