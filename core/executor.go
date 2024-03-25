package core

import (
	"github.com/PlainDAG/go-PlainDAG/core/state"
	"github.com/PlainDAG/go-PlainDAG/core/tp"
	"github.com/PlainDAG/go-PlainDAG/core/ttype"
)

type Executor struct {
	dispatcher  *tp.Dispatcher
	state       *state.StateDB
	hotAccounts map[string]struct{}
	n           *Node
}

// NewExecutor creates a new executor
func NewExecutor(state *state.StateDB, maxProcessors, txNum int, n *Node) *Executor {
	return &Executor{
		dispatcher: tp.NewDispatcher(maxProcessors, txNum),
		state:      state,
		n:          n,
	}
}

// Processing processes given transaction sets
func (e *Executor) Processing(txs map[string][]*ttype.Transaction, output []string, frequency int) []byte {
	// initialize state DB
	//e.state.BatchCreateObjects(txs)

	// cross-shard: mark created tem accs
	e.n.shardM.MarkTemAccs(txs)
	e.UpdateHotAccounts(txs, frequency)
	// execute txs and commit state updates
	graph := tp.CreateGraph(output, txs, e.hotAccounts)
	e.dispatcher.Run(graph, e.state, output)

	// cross-shard: generate cross-shard account aggregation msg
	e.n.shardM.ClearTemAccs()

	stateRoot := e.state.Commit()
	// clear in-memory data (default interval: one epoch)
	e.state.Reset()
	return stateRoot
}

// SerialProcessing serially processes given transaction sets
func (e *Executor) SerialProcessing(txs map[string][]*ttype.Transaction) []byte {
	for blk := range txs {
		for _, tx := range txs[blk] {
			msg := tx.AsMessage()
			err := tp.ApplyMessageForSerial(e.state, msg)
			if err != nil {
				panic(err)
			}
		}
	}
	stateRoot := e.state.Commit()
	e.state.Reset()
	return stateRoot
}

func (e *Executor) UpdateHotAccounts(txs map[string][]*ttype.Transaction, frequency int) {
	hotAccounts := AnalyzeHotAccounts(txs, frequency)
	e.hotAccounts = hotAccounts
}

func (e *Executor) GetHotAccounts() map[string]struct{} { return e.hotAccounts }
