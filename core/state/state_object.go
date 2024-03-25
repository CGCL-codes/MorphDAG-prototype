package state

import (
	"bytes"
	"encoding/gob"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
)

const InitialBalance = 1000

type stateObject struct {
	addr         []byte
	data         Account
	dirtyBalance int64
	dirtyStorage *sync.Map
	dirtyMargin  *sync.Map
}

// newObject creates a state object
func newObject(addr []byte, data Account) *stateObject {
	if data.Storage == nil {
		data.Storage = make(map[string]int64)
	}
	return &stateObject{
		addr:         addr,
		data:         data,
		dirtyBalance: 0,
		dirtyStorage: new(sync.Map),
		dirtyMargin:  new(sync.Map),
	}
}

func (s *stateObject) InitialStorage() {
	for i := 0; i < 5; i++ {
		value, ok := s.data.Storage[strconv.Itoa(i)]
		if ok {
			s.dirtyStorage.Store(strconv.Itoa(i), value)
		} else {
			s.dirtyStorage.Store(strconv.Itoa(i), int64(100000))
		}
	}
}

func (s *stateObject) InitialMargin() {
	for i := 0; i < 5; i++ {
		margin := make([]int64, 0, 2000)
		s.dirtyMargin.Store(strconv.Itoa(i), margin)
	}
}

// GetState returns the latest value of a given key
func (s *stateObject) GetState(key []byte) int64 {
	return s.getState(key)
}

// getState the private version of fetching state
func (s *stateObject) getState(key []byte) int64 {
	// if we have a dirty value, first check the margin array
	// if the length is 0, then retrieves the latest value
	// otherwise, adds up all the incremental values in the array
	v1, _ := s.dirtyMargin.Load(string(key))
	margin := v1.([]int64)
	v2, _ := s.dirtyStorage.Load(string(key))
	if len(margin) == 0 {
		return v2.(int64)
	} else {
		var sum int64
		for _, m := range margin {
			sum += m
		}
		latest := sum + v2.(int64)
		s.dirtyStorage.Store(string(key), latest)
		s.dirtyMargin.Store(string(key), make([]int64, 0, 2000))
		return latest
	}
}

// SetState inserts an updated value into dirty storage
func (s *stateObject) SetState(key []byte, newValue int64) {
	s.dirtyStorage.Store(string(key), newValue)
}

// IncrementState updates an incremental value into dirty storage
func (s *stateObject) IncrementState(key []byte, increment int64) {
	v, _ := s.dirtyMargin.Load(string(key))
	margins := v.([]int64)
	s.dirtyMargin.Store(string(key), append(margins, increment))
}

// IncrementBatchStates inserts a batch of incremental values into dirty storage
func (s *stateObject) IncrementBatchStates(updates map[string][]int64) {
	for key := range updates {
		increments := updates[key]
		var sum int64
		for _, v := range increments {
			sum += v
		}
		s.IncrementState([]byte(key), sum)
	}
}

// GetBalance obtains the latest account balance
func (s *stateObject) GetBalance() int64 {
	return s.getBalance()
}

// getBalance the private version of fetching balance
func (s *stateObject) getBalance() int64 {
	if atomic.LoadInt64(&s.dirtyBalance) == 0 {
		return s.data.Balance
	}
	return atomic.LoadInt64(&s.dirtyBalance)
}

// SetBalance updates the account balance
func (s *stateObject) SetBalance(increment int64) {
	// avoid update loss
	for {
		oldBalance := s.getBalance()
		newBalance := oldBalance + increment
		cur := s.getBalance()
		if cur == oldBalance {
			atomic.StoreInt64(&s.dirtyBalance, newBalance)
			break
		}
	}
}

// Commit moves all updated values and balance to account storage
func (s *stateObject) Commit() {
	// first adds up all incremental values
	s.dirtyMargin.Range(func(key, value any) bool {
		addr := key.(string)
		margin := value.([]int64)
		if len(margin) > 0 {
			var sum int64
			for _, m := range margin {
				sum += m
			}
			v, _ := s.dirtyStorage.Load(addr)
			latest := sum + v.(int64)
			s.dirtyStorage.Store(addr, latest)
		}
		return true
	})
	s.dirtyMargin = new(sync.Map)

	s.dirtyStorage.Range(func(key, value interface{}) bool {
		addr := key.(string)
		vvalue := value.(int64)
		s.data.Storage[addr] = vvalue
		return true
	})
	s.dirtyStorage = new(sync.Map)

	if s.dirtyBalance != 0 {
		s.data.Balance = s.dirtyBalance
		s.dirtyBalance = 0
	}
}

func (s *stateObject) GetNonce() uint32 {
	return s.data.Nonce
}

func (s *stateObject) SetNonce(nonce uint32) {
	s.data.Nonce = nonce
}

func (s *stateObject) Address() []byte {
	return s.addr
}

func (s *stateObject) Account() Account {
	return s.data
}

type Account struct {
	Nonce   uint32
	Balance int64
	Storage map[string]int64
}

// Serialize returns a serialized account
// this method may lead to different contents of byte slice, thus we use json marshal to replace it
func (acc Account) Serialize() []byte {
	var encode bytes.Buffer

	enc := gob.NewEncoder(&encode)
	err := enc.Encode(acc)
	if err != nil {
		log.Panic("Account encode fail:", err)
	}

	return encode.Bytes()
}

// DeserializeAcc deserializes an account
func DeserializeAcc(data []byte) Account {
	var acc Account

	decode := gob.NewDecoder(bytes.NewReader(data))
	err := decode.Decode(&acc)
	if err != nil {
		log.Panic("Account decode fail:", err)
	}

	return acc
}
