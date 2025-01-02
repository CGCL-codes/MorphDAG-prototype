package main

import (
	"Corgi/mpt"
	"Corgi/preprocess"
	"fmt"
	"time"
)

func main() {
	sample := "./data/"
	dataset := "dblp" // email, wordnet, dblp, youtube, patents

	fmt.Println("----------------Dataset: ", dataset)
	fmt.Println("----------------Loading Graph----------------")
	g := new(matching.Graph)
	g.LoadUnDireGraphFromTxt(sample + dataset + ".txt")
	g.AssignLabel(sample + dataset + "_label.txt")

	fmt.Println("----------------Building MVPTree----------------")
	start := time.Now()
	trie := mpt.NewTrie()
	for k, v := range g.NeiStr {
		byteKey := []byte(k)
		for _, e := range v {
			trie.Insert(byteKey, e, g.NeiHashes[e], g.Vertices[e].Content)
		}
	}
	end := time.Since(start)
	fmt.Println("the time of building the MVPTree for data graph "+dataset+" is: ", end)
	fmt.Println("the root digest of the MVPTree is: ", trie.HashRoot())
}
