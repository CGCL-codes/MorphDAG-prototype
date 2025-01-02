package matching

import (
	"fmt"
	"sort"
)

type QVertex struct {
	/*
	ExpandLayer: save each layer's vertices, the layer index start from 1
	*/
	Id int
	Label byte
	ExpandLayer map[int][]int
	PendingExpand map[int][]int
}

type QueryGraph struct {
	/*
	QVList: the list of query vertices
	Adj: the adjacency list
	Matrix: the 'map' format of Adj
	CandidateSets:  candidate vertex set of each query vertex
	CandidateSetsB: the 'map' format of candidate vertex set, used as bloom filter
	*/
	QVList map[int]QVertex
	Adj map[int][]int
	Matrix map[int]map[int]bool
	NeiStr map[string][]int
	PathFeature map[int]map[string]int
	CandidateSets map[int][]int
	CandidateSetsB map[int]map[int]bool
}

func LoadProcessing(queryFile, queryLabelFile string) QueryGraph {
	/*
	Loading and preprocessing the query graph
	*/
	var queryG QueryGraph
	var query Graph
	query.LoadUnDireGraphFromTxt(queryFile)
	query.AssignLabel(queryLabelFile)
	queryG.Adj = query.adj
	queryG.Matrix = query.matrix
	queryG.NeiStr = query.NeiStr
	queryG.QVList = make(map[int]QVertex)
	queryG.PathFeature = make(map[int]map[string]int)
	query.ObtainPathFeature("")

	// obtain path feature (only remain the longest paths)
	for u, pf := range query.PathFeature {
		pLen := 0
		queryG.PathFeature[u] = make(map[string]int)
		for str, _ := range pf {
			if pLen < len(str) {
				pLen = len(str)
			}
		}
		for str, num := range pf {
			if len(str) == pLen {
				queryG.PathFeature[u][str] = num
			}
		}
	}

	var temp []int
	for k, _ := range query.Vertices {
		temp = append(temp, k)
	}
	sort.Ints(temp)
	for _, i := range temp {
		v := query.Vertices[i]
		qV := QVertex{Id: v.id, Label: v.label}
		qV.ExpandLayer, qV.PendingExpand = expandGraph(v.id, query.adj)
		queryG.QVList[v.id] = qV
	}
	return queryG
}

func (q *QueryGraph) Print() {
	for k, v := range q.CandidateSets {
		fmt.Println(k, v)
	}
}

func expandGraph(v int, adj map[int][]int) (map[int][]int, map[int][]int) {
	/*
	Expanding the given graph one hop at a time and recoding each hop's vertices, the start hop is 1
	*/
	hopVertices := make(map[int][]int)
	expanVertices := make(map[int][]int)
	expanVertices[0] = append(expanVertices[0], v)
	hop := 1
	hopVertices[hop] = adj[v]
	visited := initialVisited(len(adj), adj[v])
	visited[v] = true
	for {
		if allVisited(visited) {
			break
		}
		for _, k := range hopVertices[hop]{
			flag := false
			for _, j := range adj[k] {
				if !visited[j] {
					flag = true
					visited[j] = true
					hopVertices[hop+1] = append(hopVertices[hop+1], j)
				}
			}
			if flag {
				expanVertices[hop] = append(expanVertices[hop], k)
			}
		}
		hop++
	}
	return hopVertices, expanVertices
}

func initialVisited(length int, ini []int) map[int]bool {
	visited := make(map[int]bool)
	for i:=0; i<length; i++ {
		visited[i] = false
	}
	for _, e := range ini {
		visited[e] = true
	}
	return visited
}

func allVisited(visi map[int]bool) bool {
	for _, f := range visi {
		if !f {
			return false
		}
	}
	return true
}