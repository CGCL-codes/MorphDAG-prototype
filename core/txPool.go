package core

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/PlainDAG/go-PlainDAG/core/ttype"
)

type TxPool struct {
	pending *pendingPool
	queue   *sync.Map
	n       int
}

func NewTxPool() *TxPool {
	pool := &TxPool{
		pending: newPending(),
		queue:   new(sync.Map),
	}

	return pool
}

// GetScale gets the current workload scale
func (tp *TxPool) GetScale() int {
	return tp.pending.len()
}

// RetrievePending retrieves txs from the pending pool and adds them into the queue pool
func (tp *TxPool) RetrievePending() {
	pendingTxs := tp.pending.empty()
	fmt.Printf("tx pool size: %d\n", len(pendingTxs))
	for _, t := range pendingTxs {
		id := string(t.ID)
		tp.queue.Store(id, t)
	}
}

// Pick randomly picks #size txs into a new block
func (tp *TxPool) Pick(batchSize int) []*ttype.Transaction {
	var selectedTxs = make(map[int]struct{})
	var ids []string
	var txs []*ttype.Transaction
	var pickSize int
	var poolSize int

	tp.queue.Range(func(key, value any) bool {
		id := key.(string)
		ids = append(ids, id)
		poolSize++
		return true
	})

	// if the current pool length is smaller than batchsize, then pick all the txs in the pool
	if poolSize < batchSize {
		pickSize = poolSize
	} else {
		pickSize = batchSize
	}
	if pickSize == 0 {
		log.Println("Tx pool is empty!")
	}

	for i := 0; i < pickSize; i++ {
		for {
			rand.Seed(time.Now().UnixNano())
			random := rand.Intn(len(ids))
			if _, ok := selectedTxs[random]; !ok {
				selectedTxs[random] = struct{}{}
				break
			}
		}
	}
	// log.Println("[TxPool.Pick] selectedTxs: ", selectedTxs)

	for key := range selectedTxs {
		id := ids[key]
		v, _ := tp.queue.Load(id)
		t, ok := v.(*ttype.Transaction)
		if ok {
			txs = append(txs, t)
		}
	}

	return txs
}

// // Pick randomly picks #size txs into a new block
// func (tp *TxPool) Pick(size int) []*ttype.Transaction {
// 	var selectedTxs = make(map[int]struct{})
// 	var ids []string
// 	var txs []*ttype.Transaction

// 	tp.queue.Range(func(key, value any) bool {
// 		id := key.(string)
// 		ids = append(ids, id)
// 		return true
// 	})

// 	for i := 0; i < size; i++ {
// 		for {
// 			rand.Seed(time.Now().UnixNano())
// 			random := rand.Intn(len(ids))
// 			if _, ok := selectedTxs[random]; !ok {
// 				selectedTxs[random] = struct{}{}
// 				break
// 			}
// 		}
// 	}

// 	for key := range selectedTxs {
// 		id := ids[key]
// 		v, _ := tp.queue.Load(id)
// 		t, ok := v.(*ttype.Transaction)
// 		if ok {
// 			txs = append(txs, t)
// 		}
// 	}

// 	return txs
// }

// DeleteTxs deletes txs in other concurrent blocks
func (tp *TxPool) DeleteTxs(txs []*ttype.Transaction) []*ttype.Transaction {
	var deleted []*ttype.Transaction

	for _, del := range txs {
		id := string(del.ID)
		if v, ok := tp.queue.Load(id); ok {
			t := v.(*ttype.Transaction)
			deleted = append(deleted, t)
			tp.queue.Delete(id)
		}
	}

	return deleted
}

type pendingPool struct {
	mu sync.RWMutex
	//txs map[string]*types.Transaction
	txs []*ttype.Transaction
}

func (pp *pendingPool) Append(tx *ttype.Transaction) {
	//id := string(tx.ID)
	//pp.txs[id] = tx
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if !pp.existInPending(tx) {
		pp.txs = append(pp.txs, tx)
	}
}

func (pp *pendingPool) BatchAppend(txs []*ttype.Transaction) {
	for _, t := range txs {
		pp.Append(t)
	}
}

func (pp *pendingPool) pop() *ttype.Transaction {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	last := pp.txs[len(pp.txs)-1]
	pp.txs = pp.txs[:len(pp.txs)-1]
	return last
}

func (pp *pendingPool) delete(tx *ttype.Transaction) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	for i := range pp.txs {
		if pp.txs[i] == tx {
			pp.txs = append(pp.txs[:i], pp.txs[i+1:]...)
			break
		}
	}
	//id := string(tx.ID)
	//delete(pp.txs, id)
}

func (pp *pendingPool) len() int {
	pp.mu.RLock()
	defer pp.mu.RUnlock()
	return len(pp.txs)
}

func (pp *pendingPool) swap(i, j int) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.txs[i], pp.txs[j] = pp.txs[j], pp.txs[i]
}

func (pp *pendingPool) isFull() bool {
	pp.mu.RLock()
	defer pp.mu.RUnlock()
	return len(pp.txs) == MaximumPoolSize
}

func (pp *pendingPool) isEmpty() bool {
	return len(pp.txs) == 0
}

func (pp *pendingPool) empty() []*ttype.Transaction {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	var txs []*ttype.Transaction
	if !pp.isEmpty() {
		//for _, t := range pp.txs {
		//	txs = append(txs, t)
		//}
		txs = pp.txs
		pp.txs = make([]*ttype.Transaction, 0, MaximumPoolSize)
	}

	return txs
}

func (pp *pendingPool) existInPending(tx *ttype.Transaction) bool {
	id := string(tx.ID)
	for _, t := range pp.txs {
		if strings.Compare(id, string(t.ID)) == 0 {
			return true
		}
	}
	return false
}

func newPending() *pendingPool {
	return &pendingPool{
		txs: make([]*ttype.Transaction, 0, MaximumPoolSize),
		//txs: make(map[string]*types.Transaction),
	}
}
