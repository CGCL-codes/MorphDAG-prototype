package tp

import (
	"github.com/PlainDAG/go-PlainDAG/core/state"
	"github.com/PlainDAG/go-PlainDAG/core/ttype"
	"log"
)

const CacheLimit = 10000

type HProcessor struct {
	WorkerPool chan chan *RWNode
	Channel    chan *RWNode
	cache      *Cache
}

// NewProcessor initiates a new processor (run with a goroutine)
func NewProcessor(workerPool chan chan *RWNode) *HProcessor {
	return &HProcessor{
		WorkerPool: workerPool,
		Channel:    make(chan *RWNode),
		cache:      initializeCache(),
	}
}

// Start starts a processor thread
func (hp *HProcessor) Start(statedb *state.StateDB) {
	go func() {
		for {
			// register channel to the worker pool
			hp.WorkerPool <- hp.Channel
			select {
			case job := <-hp.Channel:
				var err error
				if job.IsHot() {
					err = hp.ApplyTransaction(job.tx, statedb, true)
				} else {
					err = hp.ApplyTransaction(job.tx, statedb, false)
				}
				if err != nil {
					log.Println(err)
				}
				// delete the completed task from the pending queue
				deletedID := string(job.tx.ID)
				pending.Delete(deletedID)
			case <-stopSignal:
				return
			}
		}
	}()
}

// ApplyTransaction executes a transaction and updates the statedb
func (hp *HProcessor) ApplyTransaction(tx *ttype.Transaction, statedb *state.StateDB, isHot bool) error {
	// set the end time of transaction execution
	tx.SetEnd2()
	msg := tx.AsMessage()
	var err error
	if isHot {
		// process transactions accessing hot accounts
		err = ApplyMessage(statedb, msg)
	} else {
		// process transactions accessing dormant accounts
		err = ApplyMessageForSerial(statedb, msg)
	}
	if err != nil {
		return err
	}
	return nil
}

type Cache struct {
	state map[string][]*ttype.RWSet
	size  int64
}

func initializeCache() *Cache {
	return &Cache{
		state: make(map[string][]*ttype.RWSet),
		size:  CacheLimit,
	}
}

func (c *Cache) SetState(addr string, wSet *ttype.RWSet) {
	c.state[addr] = append(c.state[addr], wSet)
}

func (c *Cache) Clear() {
	c.state = make(map[string][]*ttype.RWSet)
}

func (c *Cache) Expand() {
	if len(c.state) == int(c.size) {
		c.size *= 2
	}
}

func (c *Cache) GetState() map[string][]*ttype.RWSet { return c.state }
