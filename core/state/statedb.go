package state

import (
	"errors"
	"log"
	"sync"

	"github.com/PlainDAG/go-PlainDAG/core/ttype"
)

type StateDB struct {
	stateObjects      *sync.Map
	stateObjectsDirty *sync.Map
	trieDB            *TrieDB
}

// NewState creates a new state from a given state root
func NewState(blkFile string, rootHash []byte) (*StateDB, error) {
	trieDB, err := NewTrieDB(blkFile, rootHash)
	if err != nil {
		return nil, err
	}

	return &StateDB{
		stateObjects:      new(sync.Map),
		stateObjectsDirty: new(sync.Map),
		trieDB:            trieDB,
	}, nil
}

// Reset resets the statedb but keeps the underlying triedb (sweeps the memory)
func (s *StateDB) Reset() {
	s.stateObjects = new(sync.Map)
	s.stateObjectsDirty = new(sync.Map)
	s.trieDB.Clear()
}

// Exist reports whether the give account exists in the statedb
func (s *StateDB) Exist(addr []byte) bool {
	return s.getStateObject(addr) != nil
}

// GetBalance retrieves the balance of the given address
func (s *StateDB) GetBalance(addr []byte) (int64, error) {
	obj := s.getStateObject(addr)
	if obj != nil {
		return obj.GetBalance(), nil
	}
	return 0, errors.New("fail to retrieve balance")
}

// GetNonce retrieves the nonce of the given address
func (s *StateDB) GetNonce(addr []byte) (uint32, error) {
	obj := s.getStateObject(addr)
	if obj != nil {
		return obj.GetNonce(), nil
	}
	return 0, errors.New("fail to retrieve nonce")
}

// GetState retrieves a value from the given account storage
func (s *StateDB) GetState(addr []byte, hash []byte) (int64, error) {
	obj := s.getStateObject(addr)
	if obj != nil {
		return obj.GetState(hash), nil
	}
	return 0, errors.New("fail to retrieve state")
}

// getStateObject retrieves state object
func (s *StateDB) getStateObject(addr []byte) *stateObject {
	// if state object is available in statedb
	if obj, ok := s.stateObjects.Load(string(addr)); ok {
		return obj.(*stateObject)
	}

	// load the object from the triedb, if it does not exist, then creates a new state object
	acc, err := s.trieDB.FetchState(addr)
	if err != nil {
		//log.Println("Failed to get state object")
		//return nil
		obj := s.createObject(addr)
		return obj
	}

	obj := newObject(addr, *acc)
	s.setStateObject(obj)
	return obj
}

// setStateObject sets state object
func (s *StateDB) setStateObject(object *stateObject) {
	object.InitialStorage()
	object.InitialMargin()
	s.stateObjects.Store(string(object.Address()), object)
}

// UpdateBalance updates the balance for the given account
func (s *StateDB) UpdateBalance(addr []byte, increment int64) {
	obj := s.GetOrNewStateObject(addr)
	if obj != nil {
		obj.SetBalance(increment)
		if _, ok := s.stateObjectsDirty.Load(string(addr)); !ok {
			// this account state has not been modified before
			s.stateObjectsDirty.Store(string(addr), struct{}{})
		}
	}
}

// UpdateIncrementalState updates the incremental value for the given account storage
func (s *StateDB) UpdateIncrementalState(addr []byte, key []byte, increment int64) {
	obj := s.GetOrNewStateObject(addr)
	if obj != nil {
		obj.IncrementState(key, increment)
		if _, ok := s.stateObjectsDirty.Load(string(addr)); !ok {
			// this account state has not been modified before
			s.stateObjectsDirty.Store(string(addr), struct{}{})
		}
	}
}

// UpdateBatchIncrementalStates updates the incremental values for the given account storage (batch version)
func (s *StateDB) UpdateBatchIncrementalStates(addr []byte, updates map[string][]int64) {
	obj := s.GetOrNewStateObject(addr)
	if obj != nil {
		obj.IncrementBatchStates(updates)
		if _, ok := s.stateObjectsDirty.Load(string(addr)); !ok {
			// this account state has not been modified before
			s.stateObjectsDirty.Store(string(addr), struct{}{})
		}
	}
}

// UpdateState updates the value for the given account storage
func (s *StateDB) UpdateState(addr []byte, key []byte, update int64) {
	obj := s.GetOrNewStateObject(addr)
	if obj != nil {
		obj.SetState(key, update)
		if _, ok := s.stateObjectsDirty.Load(string(addr)); !ok {
			// this account state has not been modified before
			s.stateObjectsDirty.Store(string(addr), struct{}{})
		}
	}
}

// GetOrNewStateObject retrieves a state object or creates a new state object if nil
func (s *StateDB) GetOrNewStateObject(addr []byte) *stateObject {
	obj := s.getStateObject(addr)
	if obj == nil {
		obj = s.createObject(addr)
	}
	return obj
}

// createObject creates a new state object
func (s *StateDB) createObject(addr []byte) *stateObject {
	acc := Account{Balance: InitialBalance, Nonce: 0}
	newObj := newObject(addr, acc)
	s.setStateObject(newObj)
	return newObj
}

// createObject2 creates a new state object (does not store it in the statedb)
func (s *StateDB) createObject2(addr []byte) *stateObject {
	acc := Account{Balance: InitialBalance, Nonce: 0}
	newObj := newObject(addr, acc)
	return newObj
}

// Database retrieves the underlying triedb.
func (s *StateDB) Database() *TrieDB {
	return s.trieDB
}

// PreFetch pre-fetches the state of account into the statedb
func (s *StateDB) PreFetch(addr string) {
	if _, ok := s.stateObjects.Load(addr); !ok {
		acc, err := s.trieDB.FetchState([]byte(addr))
		if err != nil {
			s.createObject([]byte(addr))
		}
		obj := newObject([]byte(addr), *acc)
		s.setStateObject(obj)
	}
}

// Commit writes the updated account state into the underlying triedb
func (s *StateDB) Commit() []byte {
	// retrieve the dirty state
	length := 0
	s.stateObjectsDirty.Range(func(key, value interface{}) bool {
		length++
		addr := key.(string)
		obj, _ := s.stateObjects.Load(addr)
		stateObj := obj.(*stateObject)
		// first commit to account storage
		stateObj.Commit()
		// write updates into the trie
		_ = s.trieDB.StoreState(stateObj)
		return true
	})

	if length > 0 {
		s.stateObjectsDirty = new(sync.Map)
	}

	// commit to the underlying leveldb
	root := s.trieDB.Commit()
	return root
}

// createObjects creates account state objects that a transaction access
func (s *StateDB) createObjects(tx *ttype.Transaction) {
	payload := tx.Data()
	for addr := range payload {
		if _, err := s.trieDB.FetchState([]byte(addr)); err != nil {
			obj := s.createObject2([]byte(addr))
			obj.InitialStorage()
			obj.InitialMargin()
			obj.Commit()
			if err2 := s.trieDB.StoreState(obj); err2 != nil {
				log.Println(err2)
			}
		}
	}
}

// BatchCreateObjects creates a batch of state objects and stores them into the underlying triedb
func (s *StateDB) BatchCreateObjects(txs []*ttype.Transaction) {
	for _, t := range txs {
		s.createObjects(t)
	}
	s.trieDB.Commit()
}

func (s *StateDB) RemoveTemAcc(TemAccs *[]map[string]struct{}) int {
	// start := time.Now()
	total := 0
	for _, temAccs := range *TemAccs {
		// target shard: shardid
		if len(temAccs) == 0 {
			continue
		}
		total += len(temAccs)
		// log.Println("delete ", len(temAccs), " tem acc for shard: ", shardid)
		// clear tem acc
		for temAcc := range temAccs {
			//get state of tem acc
			// log.Println("delete tem acc: ", temAcc)
			s.stateObjects.Delete(temAcc)
			s.stateObjectsDirty.Delete(temAcc)
			s.trieDB.RemoveState([]byte(temAcc))
		}
	}
	// duration := time.Since(start)
	// log.Println("Time of delete ", total, "tem accs is ", duration)
	return total
}
