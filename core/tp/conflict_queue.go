package tp

import (
	"github.com/PlainDAG/go-PlainDAG/core/ttype"
	"reflect"
	"strings"
)

// QueueGraph used to sort transactions for scheduling txs to threads
type QueueGraph struct {
	HotQueues  map[string]*Queue
	ColdQueues map[string][]*RWNode
}

type Queue struct {
	rSlice  []*RWNode
	rwSlice []*RWNode
	wSlice  []*RWNode
}

type RWNode struct {
	tx         *ttype.Transaction
	blkID      string
	location   int64
	ops        int // number of read/write operations
	isHot      bool
	isExecuted bool
}

func (node *RWNode) Tx() *ttype.Transaction { return node.tx }
func (node *RWNode) BlkId() string          { return node.blkID }
func (node *RWNode) Location() int64        { return node.location }
func (node *RWNode) Ops() int               { return node.ops }
func (node *RWNode) SetOps(ops int)         { node.ops = ops }
func (node *RWNode) IsHot() bool            { return node.isHot }
func (node *RWNode) IsExecuted() bool       { return node.isExecuted }

// CreateGraph creates a new queue graph
func CreateGraph(deps []string, txs map[string][]*ttype.Transaction, hotAccounts map[string]struct{}) QueueGraph {
	var hotQueues = make(map[string]*Queue)
	graph := mapToGraph(deps, txs)
	hotGraph, coldGraph := divide(graph, hotAccounts)
	initialGraph(hotQueues, hotAccounts)

	// arrange nodes in hotGraph
	for addr := range hotGraph {
		for _, node := range hotGraph[addr] {
			rwSets := GenerateRWSet(node)
			isWrite := false
			isInWrite := false

			for _, rw := range rwSets[addr] {
				if strings.Compare(rw.Label, "w") == 0 {
					isWrite = true
				}
				if strings.Compare(rw.Label, "iw") == 0 {
					isInWrite = true
				}
			}

			if isInWrite {
				hotQueues[addr].wSlice = append(hotQueues[addr].wSlice, node)
			} else if isWrite {
				hotQueues[addr].rwSlice = append(hotQueues[addr].rwSlice, node)
			} else {
				hotQueues[addr].rSlice = append(hotQueues[addr].rSlice, node)
			}
		}
	}

	return QueueGraph{HotQueues: hotQueues, ColdQueues: coldGraph}
}

// CombineNodes combines all nodes in a given set
func (q QueueGraph) CombineNodes(rwType string, deps []string, addrs []string) []*RWNode {
	var sortedMap = make(map[string][]*RWNode)
	var combined []*RWNode

	for _, addr := range addrs {
		queue := q.HotQueues[addr]
		var slice []*RWNode

		if strings.Compare(rwType, "r") == 0 {
			slice = queue.rSlice
		} else {
			slice = queue.wSlice
		}

		for _, node := range slice {
			rank := node.blkID
			if !ExistInNodes(sortedMap[rank], node) {
				sortedMap[rank] = append(sortedMap[rank], node)
			}
		}
	}

	for _, id := range deps {
		if _, ok := sortedMap[id]; ok {
			combined = append(combined, sortedMap[id]...)
		}
	}

	return combined
}

// mapToGraph initializes rwNodes and maps them to the basic graph
func mapToGraph(deps []string, txs map[string][]*ttype.Transaction) map[string][]*RWNode {
	var graph = make(map[string][]*RWNode)
	rwNodes := createRWNodes(deps, txs)

	for _, node := range rwNodes {
		rwSets := GenerateRWSet(node)
		node.SetOps(len(rwSets))
		for addr := range rwSets {
			graph[addr] = append(graph[addr], node)
		}
	}

	return graph
}

// divide partitions the graph into two sub-graphs
func divide(graph map[string][]*RWNode, hotAccounts map[string]struct{}) (map[string][]*RWNode, map[string][]*RWNode) {
	var hotGraph = make(map[string][]*RWNode)
	var coldGraph = make(map[string][]*RWNode)

	for addr := range graph {
		if _, ok := hotAccounts[addr]; ok {
			// add to the hotGraph
			for _, node := range graph[addr] {
				if !node.isHot {
					node.isHot = true
				}
			}
			hotGraph[addr] = graph[addr]
		} else {
			// add to the coldGraph
			coldGraph[addr] = graph[addr]
		}
	}

	// remove some hot txs accessing cold accounts
	for addr := range coldGraph {
		var newSet []*RWNode
		for _, node := range coldGraph[addr] {
			if !node.isHot {
				newSet = append(newSet, node)
			}
		}
		coldGraph[addr] = newSet
	}

	return hotGraph, coldGraph
}

// initialGraph initializes each rw node set in queues
func initialGraph(queues map[string]*Queue, hotAccounts map[string]struct{}) {
	for acc := range hotAccounts {
		queues[acc] = &Queue{
			rSlice:  make([]*RWNode, 0, 10000),
			rwSlice: make([]*RWNode, 0, 10000),
			wSlice:  make([]*RWNode, 0, 10000),
		}
	}
}

// createRWNodes create rw nodes for a given transaction set
func createRWNodes(deps []string, txs map[string][]*ttype.Transaction) []*RWNode {
	var rwNodes []*RWNode

	for _, key := range deps {
		txList := txs[key]
		for i, tx := range txList {
			rwNode := &RWNode{tx: tx, blkID: key, location: int64(i)}
			rwNodes = append(rwNodes, rwNode)
		}
	}

	return rwNodes
}

func GenerateRWSet(node *RWNode) map[string][]*ttype.RWSet {
	var rwSet map[string][]*ttype.RWSet
	// judge if tx is a common transfer
	if node.tx.Len() == 0 {
		rwSet = node.tx.CreateRWSets()
	} else {
		rwSet = node.tx.Payload
	}
	return rwSet
}

func ExistInNodes(slice []*RWNode, node *RWNode) bool {
	for _, n := range slice {
		if reflect.DeepEqual(node.tx.ID, n.tx.ID) {
			return true
		}
	}
	return false
}
