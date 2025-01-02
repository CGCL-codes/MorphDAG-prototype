package main

import (
	"Corgi/matching"
	"Corgi/mpt"
	"Corgi/verification"
	"fmt"
	"time"
)

func main() {

	workload := "7"
	sample := "./data/"
	dataset := "dblp" // dblp, youtube

	fmt.Println("----------------workload: ", workload, " dataset: ", dataset)
	fmt.Println("----------------Loading Graph----------------")
	g := new(matching.Graph)
	g.LoadUnDireGraphFromTxt(sample + dataset + ".txt")
	g.AssignLabel(sample + dataset + "_label.txt")
	trie := mpt.NewTrie()
	for k, v := range g.NeiStr {
		byteKey := []byte(k)
		for _, e := range v {
			trie.Insert(byteKey, e, g.NeiHashes[e], g.Vertices[e].Content)
		}
	}
	RD := trie.HashRoot()

	fmt.Println("----------------Loading Query----------------")
	var q matching.QueryGraph
	q = matching.LoadProcessing("./data/query/query"+workload+".txt", "./data/query/query"+workload+"_label.txt")

	fmt.Println("----------------Authenticated Processing----------------")
	VO := verification.VO{}
	t1 := time.Now()
	VO.NodeList, VO.NodeListB, VO.TP = trie.AuthFilter(&q)
	VO2 := matching.Proof{}
	g.AuthMatching(q, &VO2)
	t1E := time.Since(t1)

	VO.CSG = VO2.CSG
	VO.FP = VO2.FP
	VO.UV = VO2.UV
	VO.RS = VO2.RS
	VO.ExpandID = VO2.ExpandID
	VO.Filtered = VO2.Filtered
	fmt.Println("the time of authentication processing is: ", t1E)
	fmt.Println("the number of total results is: ", len(VO.RS))

	VO.Size()

	fmt.Println("----------------Verification Processing----------------")
	t2 := time.Now()
	fmt.Println("the verification result is: ", VO.Authentication(q, RD))
	t2E := time.Since(t2)
	fmt.Println("the time of verification processing is: ", t2E)
}
