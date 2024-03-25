package tp

import (
	"errors"
	"strings"
	"time"

	"github.com/PlainDAG/go-PlainDAG/core/state"
	"github.com/PlainDAG/go-PlainDAG/core/ttype"
)

type StateTransition struct {
	msg   ttype.ExecuteMsg
	value int64
	data  ttype.Payload
	state *state.StateDB
}

// NewStateTransition creates a new instance for state transition object
func NewStateTransition(msg ttype.ExecuteMsg, db *state.StateDB) *StateTransition {
	return &StateTransition{
		msg:   msg,
		value: msg.Value(),
		data:  msg.Data(),
		state: db,
	}
}

// ApplyMessage executes a given message
func ApplyMessage(db *state.StateDB, msg ttype.ExecuteMsg) error {
	return NewStateTransition(msg, db).TransitionDb()
}

// ApplyMessageForSerial executes a given message (for serial execution)
func ApplyMessageForSerial(db *state.StateDB, msg ttype.ExecuteMsg) error {
	return NewStateTransition(msg, db).SerialTransitionDb()
}

// ApplyMessageForNezha executes a given message (for Nezha)
func ApplyMessageForNezha(db *state.StateDB, msg ttype.ExecuteMsg) error {
	return NewStateTransition(msg, db).nezhaCommit()
}

// MimicConcurrentExecution tests speculative execution of Nezha
func MimicConcurrentExecution(db *state.StateDB, msg ttype.ExecuteMsg) {
	NewStateTransition(msg, db).mimicConcurrentExecution()
}

// TransitionDb conducts state transition
func (st *StateTransition) TransitionDb() error {
	length := st.msg.Len()

	if length == 0 {
		err := st.commonTransfer()
		if err != nil {
			return err
		}
	} else {
		err := st.stateTransfer()
		if err != nil {
			// cache.Clear()
			return err
		}
		//if !noWrites {
		//	wSets := cache.GetState()
		//	st.commit(wSets)
		//	cache.Clear()
		//}
	}

	return nil
}

// SerialTransitionDb conducts serial state transition
func (st *StateTransition) SerialTransitionDb() error {
	length := st.msg.Len()

	if length == 0 {
		err := st.commonTransfer()
		if err != nil {
			return err
		}
	} else {
		err := st.serialStateTransfer()
		if err != nil {
			return err
		}
	}

	return nil
}

// commonTransfer executes the common transfer transaction
func (st *StateTransition) commonTransfer() error {
	addrFrom := st.msg.From()
	addrTo := st.msg.To()

	bal, err := st.state.GetBalance(addrFrom)
	if err != nil {
		return err
	}

	// the account does not have sufficient balance
	if bal < st.value {
		err2 := errors.New("tx execution failed: insufficient balance")
		return err2
	}

	st.state.UpdateBalance(addrFrom, -st.value)
	st.state.UpdateBalance(addrTo, st.value)
	//log.Println("Tx execution succeed")
	return nil
}

// stateTransfer executes the state transfer transaction
func (st *StateTransition) stateTransfer() error {
	payload := st.data
	// simulate smart contract execution time
	// mimicExecution(len(payload))
	for addr := range payload {
		var bal int64
		rwSets := payload[addr]
		for _, rw := range rwSets {
			if strings.Compare(rw.Label, "r") == 0 {
				v, err := st.state.GetState([]byte(addr), rw.Addr)
				if err != nil {
					return err
				}
				bal = v
			} else if strings.Compare(rw.Label, "iw") == 0 {
				// writes to the array in statedb
				st.state.UpdateIncrementalState([]byte(addr), rw.Addr, rw.Value)
			} else {
				// writes to the statedb
				var latest int64
				if rw.Value < 0 {
					latest = bal - rw.Value
				} else {
					latest = rw.Value
				}
				st.state.UpdateState([]byte(addr), rw.Addr, latest)
			}
		}
	}
	//log.Println("Tx execution succeeds")
	return nil
}

// batchStateTransfer executes the state transfer transaction (batch version)
func (st *StateTransition) batchStateTransfer(cache *Cache) (bool, error) {
	noIWrites := true
	payload := st.data
	// simulate smart contract execution time
	// mimicExecution(len(payload))
	for addr := range payload {
		var bal int64
		rwSets := payload[addr]
		for _, rw := range rwSets {
			if strings.Compare(rw.Label, "r") == 0 {
				v, err := st.state.GetState([]byte(addr), rw.Addr)
				if err != nil {
					return noIWrites, err
				}
				bal = v
			} else if strings.Compare(rw.Label, "iw") == 0 {
				// writes to the cache
				noIWrites = false
				cache.Expand()
				cache.SetState(addr, rw)
			} else {
				// writes to the statedb
				var latest int64
				if rw.Value < 0 {
					latest = bal - rw.Value
				} else {
					latest = rw.Value
				}
				st.state.UpdateState([]byte(addr), rw.Addr, latest)
			}
		}
	}
	//log.Println("Tx execution succeeds")
	return noIWrites, nil
}

// commit writes the content of cache into the statedb
func (st *StateTransition) commit(wSets map[string][]*ttype.RWSet) {
	for addr := range wSets {
		var updates = make(map[string][]int64)
		writes := wSets[addr]
		for _, w := range writes {
			updates[string(w.Addr)] = append(updates[string(w.Addr)], w.Value)
		}
		st.state.UpdateBatchIncrementalStates([]byte(addr), updates)
	}
}

// serialStateTransfer serially executes the state transfer transaction
func (st *StateTransition) serialStateTransfer() error {
	payload := st.data
	// simulate smart contract execution time
	// mimicExecution(len(payload))
	for addr := range payload {
		var bal int64
		rwSets := payload[addr]
		for _, rw := range rwSets {
			if strings.Compare(rw.Label, "r") == 0 {
				v, err := st.state.GetState([]byte(addr), rw.Addr)
				if err != nil {
					return err
				}
				bal = v
			} else if strings.Compare(rw.Label, "iw") == 0 {
				// read before write
				before, err := st.state.GetState([]byte(addr), rw.Addr)
				if err != nil {
					return err
				}
				latest := rw.Value + before
				st.state.UpdateState([]byte(addr), rw.Addr, latest)
			} else {
				var latest int64
				if rw.Value < 0 {
					latest = bal - rw.Value
				} else {
					latest = rw.Value
				}
				st.state.UpdateState([]byte(addr), rw.Addr, latest)
			}
		}
	}
	//log.Println("Tx execution succeeds")
	return nil
}

// nezhaCommit tests transaction commits for Nezha
func (st *StateTransition) nezhaCommit() error {
	payload := st.data
	for addr := range payload {
		rwSets := payload[addr]
		for _, rw := range rwSets {
			if strings.Compare(rw.Label, "iw") == 0 {
				// read before write
				before, err := st.state.GetState([]byte(addr), rw.Addr)
				if err != nil {
					return err
				}
				latest := rw.Value + before
				st.state.UpdateState([]byte(addr), rw.Addr, latest)
			} else {
				var latest int64
				// read before write
				before, err := st.state.GetState([]byte(addr), rw.Addr)
				if err != nil {
					return err
				}
				if rw.Value < 0 {
					latest = before - rw.Value
				} else {
					latest = rw.Value
				}
				st.state.UpdateState([]byte(addr), rw.Addr, latest)
			}
		}
	}
	return nil
}

// mimicConcurrentExecution tests speculative execution of Nezha (private version)
func (st *StateTransition) mimicConcurrentExecution() {
	payload := st.data
	mimicExecution(len(payload))
}

func mimicExecution(ops int) {
	var a, b, c int
	a, b, c = 1, 1, 1

	for i := 0; i < ops*10; i++ {
		a += b
		b += a
		c += a
		time.Sleep(time.Nanosecond)
	}
}
