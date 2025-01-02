package core

import (
	"fmt"
	"sync"
)

type StaticCommitter struct {
	decidedWaveNumber int

	unCommittedMsgs map[int]map[int][]Message
	unCommittedlock sync.RWMutex

	// leaderSlot     map[int]int
	// leaderSlotLock sync.RWMutex
	hasTriggerMap  map[int]bool
	hasTriggerLock sync.RWMutex

	voteMap  map[int]map[string]int
	voteLock sync.RWMutex
	n        *Node

	commitSyncLock sync.RWMutex
}

// type slot struct {
// 	slotindex int
// 	msgs      []Message
// }

func NewStaticCommitter(n *Node) *StaticCommitter {
	return &StaticCommitter{
		decidedWaveNumber: 0,
		unCommittedMsgs:   make(map[int]map[int][]Message),

		hasTriggerMap: make(map[int]bool),

		voteMap: make(map[int]map[string]int),
		//leaderSlot:        make(map[int]int),
		n: n,
	}
}

func (c *StaticCommitter) addVote(waveNumber int, target string) {
	c.voteLock.Lock()
	defer c.voteLock.Unlock()
	if _, ok := c.voteMap[waveNumber]; !ok {
		c.voteMap[waveNumber] = make(map[string]int)
	}
	c.voteMap[waveNumber][target]++
	if c.voteMap[waveNumber][target] >= 4*f+1 {
		c.triggerLocalCommit(target, waveNumber)
	}
	//fmt.Println("The leader in wave  " + fmt.Sprint(waveNumber) + "   has got " + fmt.Sprint(c.voteMap[waveNumber][target]) + " votes")
}

func (c *StaticCommitter) triggerLocalCommit(target string, waveNumber int) {

	c.hasTriggerLock.Lock()

	if _, ok := c.hasTriggerMap[waveNumber]; !ok {
		fmt.Println("now is triggering local commit for wave " + fmt.Sprint(waveNumber))
		c.hasTriggerMap[waveNumber] = true
		c.hasTriggerLock.Unlock()
		c.LocalCommit(target, waveNumber)
	} else {
		c.hasTriggerLock.Unlock()
		//fmt.Println("the local commit phase for wave  ", waveNumber, "  has been triggered before")
		return
	}
}

func (c *StaticCommitter) LocalCommit(target string, waveNumber int) {
	fround := c.n.bc.GetRound(waveNumber * 3)
	//fmt.Println(fround)
	msg, err := fround.getMsgByRef([]byte(target))
	//fmt.Println(msg.GetRN())
	if err != nil {
		fmt.Println(err)
		return
	}
	go msg.Commit(c.n)
}

func (c *StaticCommitter) addToUnCommitted(msg Message, id int) {
	rn := msg.GetRN()

	c.unCommittedlock.Lock()
	if c.unCommittedMsgs[rn] == nil {
		c.unCommittedMsgs[rn] = make(map[int][]Message)
	}
	c.unCommittedMsgs[rn][id] = append(c.unCommittedMsgs[rn][id], msg)
	c.unCommittedlock.Unlock()
	//fmt.Println("msg at round ", rn, "from ", id, "  has been added to uncommitted")

}

// func (c *StaticCommitter) addToUnEmbeded(msg Message, id int) {
// 	rn := msg.GetRN()

// 	c.unEmbeddedlock.Lock()
// 	if c.unEmbeddedMsgs[rn] == nil {
// 		c.unEmbeddedMsgs[rn] = make(map[int][]Message)
// 	}
// 	c.unEmbeddedMsgs[rn][id] = append(c.unEmbeddedMsgs[rn][id], msg)
// 	c.unEmbeddedlock.Unlock()
// 	//fmt.Println("msg at round ", rn, "from ", id, "  has been added to unembeded")
// }

// func (c *StaticCommitter) removeUnembeded(rn int, id int) {

// 	c.unEmbeddedlock.Lock()
// 	delete(c.unEmbeddedMsgs[rn], id)
// 	c.unEmbeddedlock.Unlock()
// }

func (c *StaticCommitter) removeUncommitted(rn int, id int) {

	c.unCommittedlock.Lock()
	delete(c.unCommittedMsgs[rn], id)
	c.unCommittedlock.Unlock()
}
