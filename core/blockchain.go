package core

import "sync"

type Blockchain struct {
	vertices map[int]*Round
	bcLock   sync.RWMutex
}

func NewBlockchain() (*Blockchain, error) {
	vers := make(map[int]*Round)
	r, err := newStaticRound()
	if err != nil {
		return nil, err
	}
	vers[0] = r
	return &Blockchain{
		vertices: vers,
		bcLock:   sync.RWMutex{},
	}, nil

}

func (bc *Blockchain) GetRound(round int) *Round {
	bc.bcLock.RLock()
	defer bc.bcLock.RUnlock()
	return bc.vertices[round]

}

func (bc *Blockchain) AddRound(round *Round) {
	bc.bcLock.Lock()
	defer bc.bcLock.Unlock()
	bc.vertices[round.roundNumber] = round
}
