package matching

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/ethereum/go-ethereum/rlp"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Vertex struct {
	id int
	label byte
	Content string
}

type Graph struct {
	/*
	Vertices: vertex list
	adj: the adjacency list
	matrix: the map format of the adj
	NeiHashes: the list of hash (neiHash) of the concatenation of the ID of one-hop neighborhood of each vertex
	NeiStr: statistic the one-hop neighborhood string (neiStr) for each vertex
	PathFeature: save the path feature of each vertex
	 */
	Vertices map[int]Vertex
	adj map[int][]int
	matrix map[int]map[int]bool
	NeiHashes map[int][]byte
	NeiStr map[string][]int
	PathFeature map[int]map[string]int
}

const PathLen = 2
type NeighborhoodGraph struct {
	Vertices map[int]Vertex
	Adj map[int][]int
}

func (g *Graph) LoadUnDireGraphFromTxt(fileName string) error {
	/*
	loading the graph from txt file and saving it into an adjacency list adj, the subscripts start from 0
	 */
	g.adj = make(map[int][]int)
	g.matrix = make(map[int]map[int]bool)
	content, err := readTxtFile(fileName)
	if err != nil {
		fmt.Println("Read file error!", err)
		return err
	}
	splitStr := " "
	if find := strings.Contains(content[0], ","); find {
		splitStr = ","
	} else if find := strings.Contains(content[0], "	"); find {
		splitStr = "	"
	}
	// determine edge is one-way (fg = false) or two-way (fg = true)
	var target string
	fg := true
	for i, line := range content {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if i == 0 {
			edge := strings.Split(line, splitStr)
			target = edge[1] + splitStr + edge[0]
			continue
		}
		if line == target {
			fg = false
			break
		}
	}
	if fg { // case1: two-way
		for _, line := range content {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			edge := strings.Split(line, splitStr)
			fr, err := strconv.Atoi(edge[0])
			if err != nil {
				return err
			}
			en, err := strconv.Atoi(edge[1])
			if err!= nil {
				return err
			}
			g.adj[fr] = append(g.adj[fr], en)
			g.adj[en] = append(g.adj[en], fr)
			// build matrix
			if g.matrix[fr] == nil {
				g.matrix[fr] = make(map[int]bool)
			}
			if g.matrix[en] == nil {
				g.matrix[en] = make(map[int]bool)
			}
			g.matrix[fr][en] = true
			g.matrix[en][fr] = true
		}
	} else { // case2: one-way
		for _, line := range content {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			edge := strings.Split(line, splitStr)
			fr, err := strconv.Atoi(edge[0])
			if err != nil {
				return err
			}
			en, err := strconv.Atoi(edge[1])
			if err!= nil {
				return err
			}
			g.adj[fr] = append(g.adj[fr], en)
			// build matrix
			if g.matrix[fr] == nil {
				g.matrix[fr] = make(map[int]bool)
			}
			g.matrix[fr][en] = true
		}
	}
	return nil
}

func (g *Graph) GraphMaxDegree() int {
	maxD := 0
	for _, lis := range g.adj {
		if len(lis) > maxD {
			maxD = len(lis)
		}
	}
	return maxD
}

func (g *Graph) LoadDireGraphFromTxt(fileName string) error {
	/*
	loading the graph from txt file and saving it into an adjacency list adj, the subscripts start from 0
	*/

	g.adj = make(map[int][]int)
	content, err := readTxtFile(fileName)
	if err != nil {
		fmt.Println("Read file error!", err)
		return err
	}
	splitStr := " "
	if find := strings.Contains(content[0], ","); find {
		splitStr = ","
	} else if find := strings.Contains(content[0], "	"); find {
		splitStr = "	"
	}
	for _, line := range content {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		edge := strings.Split(line, splitStr)
		fr, err := strconv.Atoi(edge[0])
		if err != nil {
			return err
		}
		en, err := strconv.Atoi(edge[1])
		if err!= nil {
			return err
		}
		g.adj[fr] = append(g.adj[fr], en)
	}
	return nil
}

func (g *Graph) LoadGraphFromExcel(fileName string) error {
	/*
	loading the graph from Excel file and saving it into an adjacency list adj
	*/
	g.adj = make(map[int][]int)
	xlsx, err := excelize.OpenFile(fileName)
	if err != nil {
		fmt.Println("Open file error!", err)
		return err
	}
	rows := xlsx.GetRows("Sheet1")
	for _, row := range rows {
		fr, err := strconv.Atoi(row[0])
		if err != nil {
			return err
		}
		en, err := strconv.Atoi(row[1])
		if err!= nil {
			return err
		}
		fmt.Println(fr, en)
		g.adj[fr] = append(g.adj[fr], en)
	}
	return nil
}

func (g *Graph) AssignLabel(labelFile string) error {
	/*
	Assigning a label to each vertex
	 */
	g.Vertices = make(map[int]Vertex)
	labelSet := make(map[int]string)
	content, err := readTxtFile(labelFile)
	if err != nil {
		fmt.Println("Read file error!", err)
		return err
	}
	for _, line := range content {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		onePair := strings.Split(line, " ")
		key, _ := strconv.Atoi(onePair[0])
		labelSet[key] = onePair[1]
	}
	for k, _ := range g.adj {
		var v Vertex
		v.id = k
		v.label = []byte(labelSet[k])[0]
		v.Content = ""
		g.Vertices[k] = v
	}
	g.getNeiHashNeiStr()
	return nil
}

func (g *Graph) getNeiHashNeiStr() error {
	/*
	Generating the neiHash and neiStr for each vertex
	 */

	g.NeiHashes = make(map[int][]byte)
	g.NeiStr = make(map[string][]int)

	for k, v := range g.adj {
		sort.Ints(g.adj[k])
		h := md5.New()
		h.Write(Serialize(g.adj[k]))
		g.NeiHashes[k] = h.Sum(nil)
		//g.NeiHashes[k] = crypto.Keccak256(Serialize(g.adj[k]))

		str := string(g.Vertices[k].label)
		var nei []string
		for _, i := range v {
			nei = append(nei, string(g.Vertices[i].label))
		}
		sort.Strings(nei)
		for _, t := range nei {
			str = str + t
		}
		g.NeiStr[str] = append(g.NeiStr[str], k)
	}
	return nil
}

func (g *Graph) getNG(id int) NeighborhoodGraph {
	/*
	Getting the neighborhood graph for vertex id
	 */
	var NG NeighborhoodGraph
	NG.Vertices = make(map[int]Vertex)
	NG.Adj = make(map[int][]int)
	for _, n := range g.adj[id] {
		for _, nn := range g.adj[n] {
			if g.matrix[id][nn] {
				NG.Adj[n] = append(NG.Adj[n], nn)
			}
		}
		if len(NG.Adj[n]) != 0  {
			NG.Vertices[n] = g.Vertices[n]
		}
	}
	return NG
}

func (g *Graph) WritePathFeature(fileName string) error {
	/*
		Generating path feature for each vertex then save
	*/
	filePtr, err := os.Create(fileName)
	if err != nil {
		fmt.Println("create file failed", err.Error())
		return err
	}
	defer filePtr.Close()
	encoder := json.NewEncoder(filePtr)

	PathFeature := make(map[int]map[string]int)
	for v, _ := range g.Vertices {
		PathFeature[v] = make(map[string]int)
		NG := g.getNG(v)
		pf := enumeratePF(NG, PathLen)
		for p, r := range pf {
			PathFeature[v][p] = len(r)
		}
	}
	err = encoder.Encode(PathFeature)
	if err != nil {
		fmt.Println("coding error", err.Error())
	} else {
		fmt.Println("coding success")
	}
	return nil
}

func (g *Graph) ObtainPathFeature(fileName string) error {
	/*
	Generating path feature for each vertex
	 */

	g.PathFeature = make(map[int]map[string]int)
	if fileName == "" {
		for v, _ := range g.Vertices {
			g.PathFeature[v] = make(map[string]int)
			NG := g.getNG(v)
			pf := enumeratePF(NG, PathLen)
			for p, r := range pf {
				g.PathFeature[v][p] = len(r)
			}
		}
	} else {
		filePtr, err := os.Open(fileName)
		if err != nil {
			fmt.Println("open file failed", err.Error())
			return err
		}
		defer filePtr.Close()
		decoder := json.NewDecoder(filePtr)
		err = decoder.Decode(&g.PathFeature)
		if err != nil {
			fmt.Println("decoding failed", err.Error())
		} else {
			fmt.Println("decoding success")
		}
	}
	return nil
}

func enumeratePF(ng NeighborhoodGraph, l int) map[string][][]int {
	/*
	Enumerating all paths that the length no more than l in ng
	 */
	PF := make(map[string][][]int)
	visited := make(map[int]bool)
	IdRoute := make(map[string]bool)
	var path []Vertex
	for k, v := range ng.Vertices {
		if !visited[k] {
			visited[k] = true
			path = append(path, v)
			DFS(k, path, 0, l, IdRoute, PF, visited, ng)
			visited[k] = false
			path = []Vertex{}
		}
	}
	return PF
}

func DFS(id int, path []Vertex, l int, bound int, idRoute map[string]bool, pf map[string][][]int, visited map[int]bool, ng NeighborhoodGraph) {
	if l > 0 {
		var pathStr string
		var idStr string
		var idInt []int
		// AB = BA, ABC = CBA
		if path[0].label > path[len(path)-1].label {
			for i:=len(path)-1; i>=0; i-- {
				pathStr = pathStr + string(path[i].label)
				idStr = idStr + strconv.Itoa(path[i].id)
				idInt = append(idInt, path[i].id)
			}
		} else {
			for _, ver := range path {
				pathStr = pathStr + string(ver.label)
				idStr = idStr + strconv.Itoa(ver.id)
				idInt = append(idInt, ver.id)
			}
		}
		if !idRoute[idStr] {
			idRevers := []byte(idStr)
			Reverse(idRevers)
			if !idRoute[string(idRevers)] {
				idRoute[idStr] = true
				pf[pathStr] = append(pf[pathStr], idInt)
			}
		}
		if l == bound {
			return
		}
		//return
	}
	for _, n := range ng.Adj[id] {
		if !visited[n] {
			visited[n] = true
			path = append(path, ng.Vertices[n])
			DFS(n, path, l+1, bound, idRoute, pf, visited, ng)
			visited[n] = false
			path = path[:len(path)-1]
		}
	}
	return
}

func (g *Graph) Print() error {
	for k, v := range g.NeiStr {
		fmt.Println(k, v)
	}
	return nil
}

func (g *Graph) ObtainMatchedGraphs(query QueryGraph) []map[int]int {
	/*
	Obtaining results that matched the given query graph in the data graph
	 */
	var result []map[int]int

	expandId := g.GetExpandQueryVertex(query)
	sort.Ints(query.CandidateSets[expandId])
	fmt.Println("the number of candidates: ", len(query.CandidateSets[expandId]))
	zero := 0 // for test
	useL := []int{}
	stTotal := time.Now()
	for _, candid := range query.CandidateSets[expandId] {
		//fmt.Println("processing: ", candid, "degree is: ", len(g.adj[candid]))
		//res := g.expandOneVertexV1(candid, expandId, query)
		res := g.expandOneVertexV2(candid, expandId, query)
		// for test
		if len(res) == 0 {
			zero = zero + 1
		} else {
			result = append(result, res...)
			useL = append(useL, candid)
		}
	}
	enTotal := time.Since(stTotal)
	fmt.Println("the total time is: ", enTotal)
	//for test
	//fmt.Println("the number of false vertices: ", zero)
	st := time.Now()
	for _, candid := range useL {
		g.expandOneVertexV2(candid, expandId, query)
	}
	en := time.Since(st)
	fmt.Println("without false positive time is: ", en)

	return result
}

func (g *Graph) expandOneVertexV2(candidateId, expandQId int, query QueryGraph) []map[int]int {
	/*
	Expanding the data graph from the given candidate vertex to obtain matched results (join-based)
	*/
	var result []map[int]int
	expL := 1
	preMatched := make(map[int]int)
	preMatched[expandQId] = candidateId
	g.matchingV2(expL, expandQId, query, preMatched, &result)

	return result
}

func (g *Graph) ConObtainMatchedGraphs(query QueryGraph) []map[int]int {
	/*
	Concurrently obtaining results that matched the given query graph in the data graph
	*/
	var result []map[int]int
	expandId := g.GetExpandQueryVertex(query)
	lenT := len(query.CandidateSets[expandId])

	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)
	chs := make([]chan []map[int]int, cpus)
	start := 0
	interval := lenT / cpus
	Shuffle(query.CandidateSets[expandId]) // because the front part is easy and the back part is hard
	for i := 0; i < len(chs); i++ {
		chs[i] = make(chan []map[int]int, 1)
		task := query.CandidateSets[expandId][start:start+interval]
		start = start + interval
		qExpand := expandId
		qG := query
		//go g.conExpandSomeVerticesV1(task, qExpand, qG, chs[i])
		go g.conExpandSomeVerticesV2(task, qExpand, qG, chs[i])
	}
	for _, ch := range chs {
		res := <- ch
		result = append(result, res...)
	}
	return result
}

func (g *Graph) conExpandSomeVerticesV2(candidateIdList []int, expandQId int, query QueryGraph, res chan []map[int]int)  {
	/*
	Expanding the given candidate vertices to obtain matched results (join-based)
	*/
	startT1 := time.Now()
	var resultA []map[int]int
	for _, id := range candidateIdList {
		//fmt.Println(id)
		var resultP []map[int]int

		expL := 1
		preMatched := make(map[int]int)
		preMatched[expandQId] = id
		g.matchingV2(expL, expandQId, query, preMatched, &resultP)
		resultA = append(resultA, resultP...)
	}
	res <- resultA
	time1 := time.Since(startT1)
	fmt.Println("the time of phase2 is: ", time1)
}

func (g *Graph) matchingV2(expL int, expQId int, query QueryGraph, preMatched map[int]int, res *[]map[int]int){
	/*
	Recursively enumerating each layer's matched results then generating final results
	expT: still need expanding times
	expQId: the starting expansion query vertex
	preMatched: already matched part
	res: save the result
	*/
	if expL > len(query.QVList[expQId].ExpandLayer) {
		return
	}
	// 1. get the query vertices of the current layer as well as each vertex's candidate set
	qPresentVer := query.QVList[expQId].ExpandLayer[expL]

	// 2. get the graph vertices of the current layer and classify them
	classes := make(map[int][]int)
	visited := make(map[int]bool)
	for _, v := range preMatched {
		visited[v] = true
	}
	var gVer []int // the graph vertices need to be expanded in current layer
	if expL == 1 {
		gVer = append(gVer, preMatched[expQId])
	} else {
		for _, q := range query.QVList[expQId].ExpandLayer[expL-1] {
			gVer = append(gVer, preMatched[q])
		}
	}
	repeat := make(map[int]bool)  // avoid visited repeat vertex in current layer
	for _, v := range gVer { // expand each graph vertex of current layer
		for _, n := range g.adj[v] {
			if !visited[n] && !repeat[n] { // get one unvisited graph vertex n of the current layer
				repeat[n] = true
				for _, c := range qPresentVer { // check current graph vertex n belong to which query vertex's candidate set
					fg := true
					if query.CandidateSetsB[c][n] { // graph vertex n may belong to the candidate set of query vertex c
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
	// if one of query vertices' candidate set is empty then return
	if len(classes) < len(qPresentVer) {
		return
	}

	// 3. obtain current layer's matched results
	curRes := g.ObtainCurRes(classes, query, qPresentVer)
	// if present layer has no media result then return
	if len(curRes) == 0 {
		return
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
		*res = append(*res, totalRes...)
		return
	} else {
		// else continue matching
		for _, eachM := range totalRes {
			g.matchingV2(expL+1, expQId, query, eachM, res)
		}
	}
}

func (g *Graph) Filter(preMatched map[int]int, raw []map[int]int, fine *[]map[int]int, qAdj map[int][]int) [][]int{
	/*
	Filter the raw media results rely on the connectivity of query graph and the vertices of present layer
	 */
	var verList [][]int
	var fg = true
	for _, r := range raw {
		if checkDuplicateVal(r) {
			continue
		}
		presentMatched := append2IntMap(preMatched, r)
		fg = true
		var verL []int
		I:
		for k1, v1 := range presentMatched {
			k1Nei := make(map[int]bool)
			for _, kn := range qAdj[k1]{
				k1Nei[kn] = true
			}
			v1Nei := make(map[int]bool)
			for _, vn := range g.adj[v1]{
				v1Nei[vn] = true
			}
			for k2, v2 := range presentMatched {
				if k1 == k2 {
					continue
				} else if !connected(k1Nei, k2, v1Nei, v2){
					fg = false
					break I
				}
			}
		}
		if fg {
			for _, v := range r {
				verL = append(verL, v)
			}
			verList = append(verList, verL)
			*fine = append(*fine, presentMatched)
		}
	}
	return verList
}

func (g *Graph) setVisited(candidateId, layers int) []map[int]bool {
	/*
	Expanding 'layer' times from the given start vertex 'candidateID', and setting the visited status for the vertices of layer
	 */
	var res []map[int]bool
	visi := make(map[int]bool)
	visi[candidateId] = true
	res = append(res, visi)

	hopVertices := make(map[int][]int)
	hopVertices[0] = append(hopVertices[0], candidateId)
	for hop:=0; hop < layers; hop++ {
		visited := make(map[int]bool)
		for _, k := range hopVertices[hop]{
			for _, j := range g.adj[k] {
				if !res[hop][j] {
					visited[j] = true
					hopVertices[hop+1] = append(hopVertices[hop+1], j)
				}
			}
		}
		for k, v := range res[hop] {
			visited[k] = v
		}
		res = append(res, visited)
	}
	return res
}

func (g *Graph) ObtainCurRes(classes map[int][]int, query QueryGraph, qVer []int) []map[int]int {
	/*
	Obtain current layer's matched results based on LC
	*/

	var matchedRes []map[int]int
	// find all edges between query vertices in current layer (generate it in advance)
	qVerCurAdj := make(map[int][]int)
	for i:=0; i<len(qVer); i++ {
		qVerCurAdj[qVer[i]] = []int{}
		for j:=0; j<len(qVer); j++ {
			if query.Matrix[qVer[i]][qVer[j]] {
				qVerCurAdj[qVer[i]] = append(qVerCurAdj[qVer[i]], qVer[j])
			}
		}
	}

	// using BFS find all connected part, meanwhile generating part results
	visited := make(map[int]bool)
	var queue []int
	var partResults []map[int][]int
	for _, k:= range qVer {
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
						onePartRes = g.join(onePartRes, v, n, classes[n], qVerCurAdj[n])
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
	ProductPlus(partResults, &matchedRes, agent, 0, oneRes)
	return matchedRes
}

func (g *Graph) join(curRes map[int][]int, v1, v2 int, v2Candi, v2Nei []int) map[int][]int {
	/*
	Join the vertex v2 to current results
	 */
	newCurRes := make(map[int][]int)
	for i, c1 := range curRes[v1] {
		for _, c2 := range v2Candi {
			fg := false
			if g.matrix[c1][c2] {
				fg = true
				// judge the connectivity with other matching vertices
				for _, n := range v2Nei { // check each neighbor of v2 whether in matched res or not
					if _, ok := curRes[n]; ok { // neighbor belong to res
						if !g.matrix[curRes[n][i]][c2]{ // the connectivity is not satisfied
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

func (g *Graph) GetExpandQueryVertex(q QueryGraph) int {
	/*
	Computing the weights for each query vertex and choose the smallest
	*/
	index := 0
	coe := 10000000000.0000
	for i, c := range q.CandidateSets {
		sum := 0
		for _, v := range c {
			sum = sum + len(g.adj[v])
		}
		temp := float64(sum/len(c)+len(c)) * float64(len(q.QVList[i].ExpandLayer))
		if temp < coe {
			index = i
			coe = temp
		}
	}
	return index
}

func ProductPlus(partRes []map[int][]int, res *[]map[int]int, qV []int, level int, oneMap map[int]int) {
	/*
	Permutation and combination all part results
	*/
	if level < len(partRes) {
		for i:=0; i<len(partRes[level][qV[level]]); i++ {
			for k, _ := range partRes[level] {
				oneMap[k] = partRes[level][k][i]
			}
			ProductPlus(partRes, res, qV, level+1, oneMap)
		}
	} else {
		deduplicate := make(map[int]bool)
		fg := false
		// check whether exist repeat elements in oneMap
		for _, v := range oneMap{
			if deduplicate[v] {
				fg = true
				break
			} else {
				deduplicate[v] = true
			}
		}
		if !fg { // without repeat elements
			newMp := copyMap(oneMap)
			*res = append(*res, newMp)
		}
	}
}

func checkDuplicateVal(mp map[int]int) bool {
	/*
	Checking whether the given map has the same value
	 */
	vMp := make(map[int]int)
	for _, v := range mp {
		if _, ok := vMp[v]; ok {
			return true
		} else {
			vMp[v] = 1
		}
	}
	return false
}

func append2IntMap(mp1, mp2 map[int]int) map[int]int {
	res := make(map[int]int)
	for k, v := range mp2 {
		res[k] = v
	}
	for k, v := range mp1 {
		res[k] = v
	}
	return res
}

func copyMap(orig map[int]int) map[int]int {
	cp := make(map[int]int)
	for k, v := range orig {
		cp[k] = v
	}
	return cp
}

func connected(qNei map[int]bool, qV int, gNei map[int]bool, gV int) bool {
	/*
	Checking whether the connection relationship between the two graph vertices is the same as the two query vertices
	 */
	if qNei[qV] && gNei[gV] {
		return true
	} else if !qNei[qV] && !gNei[gV] {
		return true
	}
	return false
}

func Product(matchedMap map[int][]int, res *[]map[int]int, qV []int, level int, oneMap map[int]int) {
	/*
	Permutation and combination on multiple lists
	 */
	if level < len(matchedMap) {
		for i:=0; i<len(matchedMap[qV[level]]); i++ {
			oneMap[qV[level]] = matchedMap[qV[level]][i]
			Product(matchedMap, res, qV, level+1, oneMap)
		}
	} else {
		newMp := copyMap(oneMap)
		*res = append(*res, newMp)
	}
}

func readTxtFile(filePath string) ([]string, error) {
	fileSuffix :=  path.Ext(filePath)
	result := []string{}
	if fileSuffix == ".txt" {
		cont, err := ioutil.ReadFile(filePath)
		if err != nil {
			fmt.Println("Open file error!", err)
			return result, err
		}
		s := string(cont)
		result = strings.Split(s, "\n")
		return result, nil
	} else {
		return result, errors.New("file format error")
	}
}

func Serialize(nei []int) []byte {
	raw := []interface{}{}
	for _, n := range nei {
		raw = append(raw, byte(n))
	}
	rlp, err := rlp.EncodeToBytes(raw)
	if err != nil {
		return rlp
	}
	return rlp
}

func Shuffle(slice []int) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for len(slice) > 0 {
		n := len(slice)
		randIndex := r.Intn(n)
		slice[n-1], slice[randIndex] = slice[randIndex], slice[n-1]
		slice = slice[:n-1]
	}
}

func Reverse(s interface{})  {
	sort.SliceStable(s, func(i, j int) bool {
		return true
	})
}