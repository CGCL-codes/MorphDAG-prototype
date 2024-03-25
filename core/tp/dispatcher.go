package tp

import (
	"github.com/PlainDAG/go-PlainDAG/core/state"
	"github.com/PlainDAG/go-PlainDAG/core/ttype"
	"sort"
	"strings"
	"sync"
)

const MaxQueue = 100000

var jobQueue chan []*RWNode
var stopSignal chan struct{}
var pending *ttype.ConcurPending

func reset() {
	jobQueue = make(chan []*RWNode, MaxQueue)
	stopSignal = make(chan struct{})
	pending = ttype.NewPending()
}

// Dispatcher assign txs to threads
type Dispatcher struct {
	ProcessorPool chan chan *RWNode
	Load          int
}

// NewDispatcher initiates a new dispatcher
func NewDispatcher(maxProcessors, txNum int) *Dispatcher {
	reset()
	pool := make(chan chan *RWNode, maxProcessors)
	return &Dispatcher{ProcessorPool: pool, Load: txNum}
}

// Run starts the work thread of dispatcher
func (d *Dispatcher) Run(q QueueGraph, statedb *state.StateDB, deps []string) {
	var wg sync.WaitGroup
	tracker := NewTracker(q, deps)

	wg.Add(2)

	go func() {
		defer wg.Done()
		tracker.Run()
	}()
	go func() {
		defer wg.Done()
		d.dispatch(statedb)
	}()

	wg.Wait()
}

// dispatch starts new processors and look for free processors
func (d *Dispatcher) dispatch(statedb *state.StateDB) {
	var totalNum int
	// pick the first parallel group and determine the number of processors
	parallel := <-jobQueue
	totalNum += len(parallel)
	for i := 0; i < len(parallel); i++ {
		processor := NewProcessor(d.ProcessorPool)
		processor.Start(statedb)
	}
	record := recordID(parallel)
	pending.BatchAppend(record)
	d.transferNodes(parallel)

	for {
		for {
			// if and only if the previous dependent transactions are executed
			if pending.Len() == 0 {
				break
			}
		}

		if totalNum == d.Load {
			// kill all the processors if all tasks are completed
			close(stopSignal)
			return
		}

		select {
		case jobs := <-jobQueue:
			totalNum += len(jobs)
			poolSize := len(d.ProcessorPool)
			if poolSize < len(jobs) {
				// add the number of processors
				addNum := len(jobs) - poolSize
				for i := 0; i < addNum; i++ {
					processor := NewProcessor(d.ProcessorPool)
					processor.Start(statedb)
				}
			}
			record2 := recordID(jobs)
			pending.BatchAppend(record2)
			d.transferNodes(jobs)
		}
	}
}

// transferNodes fetches the available (free) thread and assign tx to it
func (d *Dispatcher) transferNodes(parallel []*RWNode) {
	for _, node := range parallel {
		go func(node *RWNode) {
			processor := <-d.ProcessorPool
			processor <- node
		}(node)
	}
}

// Tracker track dependency and put txs into the JobQueue
type Tracker struct {
	graph QueueGraph
	deps  []string
}

func NewTracker(cq QueueGraph, deps []string) *Tracker {
	return &Tracker{cq, deps}
}

func (t *Tracker) Run() {
	var addrs1 []string
	var addrs2 []string

	for key := range t.graph.HotQueues {
		addrs1 = append(addrs1, key)
	}
	sort.Strings(addrs1)

	t.arrangeRNodes(addrs1)
	t.arrangeRWNodes(addrs1)
	t.arrangeWNodes(addrs1)

	for key := range t.graph.ColdQueues {
		addrs2 = append(addrs2, key)
	}
	sort.Strings(addrs2)

	t.arrangeColdNodes(addrs2)
}

func (t *Tracker) arrangeRNodes(addrs []string) {
	rNodes := t.graph.CombineNodes("r", t.deps, addrs)
	t.loopSeekParallel(rNodes)
}

func (t *Tracker) arrangeRWNodes(addrs []string) {
	for {
		var col []*RWNode
		// fetch nodes
		for _, addr := range addrs {
			q := t.graph.HotQueues[addr]

			for i := 0; i < len(q.rwSlice); i++ {
				node := q.rwSlice[i]
				if !node.IsExecuted() && !ExistInNodes(col, node) {
					col = append(col, node)
					break
				}
			}
		}

		if len(col) == 0 {
			break
		}

		t.advancedSeekParallel(col)
	}
}

func (t *Tracker) arrangeWNodes(addrs []string) {
	wNodes := t.graph.CombineNodes("iw", t.deps, addrs)
	var col []*RWNode

	// delete executed txs
	for _, node := range wNodes {
		if !node.IsExecuted() {
			col = append(col, node)
		}
	}

	jobQueue <- col
	// no need to check for conflicts
	//t.loopSeekParallel(col)
}

func (t *Tracker) arrangeColdNodes(addrs []string) {
	for {
		// ID -> [RWNode1, RWNode1,...]
		var parallel = make(map[string][]*RWNode)
		var group []*RWNode
		var final []*RWNode

		for _, addr := range addrs {
			cq := t.graph.ColdQueues[addr]
			for i := 0; i < len(cq); i++ {
				if !cq[i].IsExecuted() {
					group = append(group, cq[i])
					break
				}
			}
		}

		if len(group) == 0 {
			break
		}

		for _, node := range group {
			id := string(node.tx.ID)
			parallel[id] = append(parallel[id], node)
		}

		for id := range parallel {
			node := parallel[id][0]
			// if all the operations can be obtained
			if node.ops == len(parallel[id]) {
				final = append(final, node)
				node.isExecuted = true
			}
		}

		jobQueue <- final
	}
}

//func (t *Tracker) insertRorWNodes(rwType string, nodes []*RWNode) {
//	for {
//		parallel, remaining := t.advancedSeekParallel(rwType, nodes)
//		jobQueue <- parallel
//		nodes = remaining
//		if len(nodes) == 0 {
//			break
//		}
//	}
//}
//
//func (t *Tracker) insertRWNodes(nodes []*RWNode) {
//	parallel, _ := t.advancedSeekParallel("rwNode", nodes)
//	jobQueue <- parallel
//}
//
//func (t *Tracker) advancedSeekParallel(rwType string, nodes []*RWNode) ([]*RWNode, []*RWNode) {
//	var parallel []*RWNode
//	var remaining []*RWNode
//	var rMap = make(map[string]int)
//
//	for _, node := range nodes {
//		rwSet := GenerateRWSet(node)
//		for addr := range rwSet {
//			judge := false
//			if strings.Compare(rwType, "wNode") == 0 {
//				// only check cold account addresses
//				if _, ok := t.Graph.HotQueues[addr]; !ok {
//					judge = true
//				}
//			} else {
//				judge = true
//			}
//			if judge {
//				for _, v := range rwSet[addr] {
//					if strings.Compare(v.Label, "r") == 0 {
//						rMap[addr] += 1
//						break
//					}
//				}
//			}
//		}
//	}
//
//	for _, node := range nodes {
//		isParallel := true
//		rwSet := GenerateRWSet(node)
//		for addr := range rwSet {
//			if _, ok := rMap[addr]; ok {
//				hasWrite := false
//				hasRead := false
//				for _, v := range rwSet[addr] {
//					if strings.Compare(v.Label, "w") == 0 {
//						hasWrite = true
//					} else {
//						hasRead = true
//					}
//				}
//
//				if hasWrite {
//					if hasRead && rMap[addr] == 1 {
//						continue
//					}
//					isParallel = false
//					break
//				}
//			}
//		}
//		if isParallel {
//			parallel = append(parallel, node)
//			node.isExecuted = true
//		} else {
//			remaining = append(remaining, node)
//		}
//	}
//
//	return parallel, remaining
//}

func (t *Tracker) seekParallel(nodes []*RWNode) {
	var parallel []*RWNode

	for i := 0; i < len(nodes); i++ {
		if i == 0 {
			parallel = append(parallel, nodes[i])
			nodes[i].isExecuted = true
			continue
		}

		isParallel := true
		for j := 0; j < len(parallel); j++ {
			if isConflict := isInConflict(nodes[i], parallel[j]); isConflict {
				isParallel = false
				break
			}
		}
		if isParallel {
			parallel = append(parallel, nodes[i])
			nodes[i].isExecuted = true
		}
	}
	// put a parallel tx set into the JobQueue
	jobQueue <- parallel
}

func (t *Tracker) loopSeekParallel(nodes []*RWNode) {
	for {
		var parallel []*RWNode
		var remaining []*RWNode
		tiny := initializeTinyQueue()

		for i := 0; i < len(nodes); i++ {
			if i == 0 {
				parallel = append(parallel, nodes[i])
				nodes[i].isExecuted = true
				tiny.insertNode(nodes[i])
				continue
			}
			// check if the current node is in conflict with the added nodes
			isConflict := tiny.isInConflict(nodes[i])
			if !isConflict {
				parallel = append(parallel, nodes[i])
				nodes[i].isExecuted = true
				tiny.insertNode(nodes[i])
			} else {
				remaining = append(remaining, nodes[i])
			}
		}
		// put a parallel tx set into the JobQueue
		jobQueue <- parallel
		nodes = remaining
		if len(nodes) == 0 {
			break
		}
	}
}

func (t *Tracker) advancedSeekParallel(nodes []*RWNode) {
	var parallel []*RWNode
	tiny := initializeTinyQueue()

	for i := 0; i < len(nodes); i++ {
		if i == 0 {
			parallel = append(parallel, nodes[i])
			nodes[i].isExecuted = true
			tiny.insertNode(nodes[i])
			continue
		}
		// check if the current node is in conflict with the added nodes
		isConflict := tiny.isInConflict(nodes[i])
		if !isConflict {
			parallel = append(parallel, nodes[i])
			nodes[i].isExecuted = true
			tiny.insertNode(nodes[i])
		}
	}
	// put a parallel tx set into the JobQueue
	jobQueue <- parallel
}

type tinyQueue struct {
	tiny map[string]*queue
}

type queue struct {
	rSlice  []*RWNode
	wSlice  []*RWNode
	iwSlice []*RWNode
}

func initializeTinyQueue() *tinyQueue {
	return &tinyQueue{tiny: make(map[string]*queue)}
}

func (tq *tinyQueue) insertNode(node *RWNode) {
	rwSet := GenerateRWSet(node)
	for addr := range rwSet {
		if _, ok := tq.tiny[addr]; !ok {
			newQueue := &queue{
				rSlice:  make([]*RWNode, 0, 5000),
				wSlice:  make([]*RWNode, 0, 5000),
				iwSlice: make([]*RWNode, 0, 5000),
			}
			tq.tiny[addr] = newQueue
		}
		hasRInserted := false
		hasWInserted := false
		hasIWInserted := false
		for _, rw := range rwSet[addr] {
			if strings.Compare(rw.Label, "r") == 0 && !hasRInserted {
				tq.tiny[addr].rSlice = append(tq.tiny[addr].rSlice, node)
				hasRInserted = true
			} else if strings.Compare(rw.Label, "w") == 0 && !hasWInserted {
				tq.tiny[addr].wSlice = append(tq.tiny[addr].wSlice, node)
				hasWInserted = true
			} else if strings.Compare(rw.Label, "iw") == 0 && !hasIWInserted {
				tq.tiny[addr].iwSlice = append(tq.tiny[addr].iwSlice, node)
				hasIWInserted = true
			}
		}
	}
}

func (tq *tinyQueue) isInConflict(node *RWNode) bool {
	rwSet := GenerateRWSet(node)

	for addr := range rwSet {
		if q, ok := tq.tiny[addr]; ok {
			for _, rw := range rwSet[addr] {
				if strings.Compare(rw.Label, "r") == 0 {
					if len(q.wSlice) > 0 || len(q.iwSlice) > 0 {
						return true
					}
				} else if strings.Compare(rw.Label, "w") == 0 {
					if len(q.rSlice) > 0 || len(q.wSlice) > 0 || len(q.iwSlice) > 0 {
						return true
					}
				} else {
					if len(q.rSlice) > 0 || len(q.wSlice) > 0 {
						return true
					}
				}
			}
		}
	}

	return false
}

func isInConflict(node1, node2 *RWNode) bool {
	rwSet1 := GenerateRWSet(node1)
	rwSet2 := GenerateRWSet(node2)

	for addr := range rwSet1 {
		if _, ok := rwSet2[addr]; ok {
			// conflict condition: (node1 r : node2 w), (node1 w : node2 r)
			read1, write1, read2, write2 := false, false, false, false

			for _, rw := range rwSet1[addr] {
				if strings.Compare(rw.Label, "r") == 0 {
					read1 = true
				}
				if strings.Compare(rw.Label, "w") == 0 {
					write1 = true
				}
			}

			for _, rw := range rwSet2[addr] {
				if strings.Compare(rw.Label, "r") == 0 {
					read2 = true
				}
				if strings.Compare(rw.Label, "w") == 0 {
					write2 = true
				}
			}

			if (read1 && write2) || (write1 && read2) {
				return true
			}
			return false
		}
	}
	return false
}

func recordID(nodes []*RWNode) []string {
	var record []string
	for _, node := range nodes {
		newID := string(node.tx.ID)
		record = append(record, newID)
	}
	return record
}
