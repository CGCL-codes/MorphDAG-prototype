package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/PlainDAG/go-PlainDAG/sign"
)

type LeaderSelector struct {
	sigsMap     map[int]*SigsPerWave
	sigsMapLock sync.Mutex

	slotMap     map[int]*int
	slotMapLock sync.Mutex

	n *Node

	leaderChosenChan map[int]chan bool
	leaderChosenLock sync.RWMutex
}

type SigsPerWave struct {
	sigsbytes [][]byte
	sigsLock  sync.Mutex
	wn        int
}

func NewLeaderSelector(n *Node) *LeaderSelector {
	return &LeaderSelector{
		sigsMap:          make(map[int]*SigsPerWave),
		slotMap:          make(map[int]*int),
		leaderChosenChan: make(map[int]chan bool),
		n:                n,
	}
}

func (ls *LeaderSelector) handleTsMsg(msg *ThresSigMsg) error {
	ls.slotMapLock.Lock()

	if _, ok := ls.slotMap[msg.Wn]; ok {
		//fmt.Println("what for ?", msg.Wn)
		ls.slotMapLock.Unlock()
		return nil
	}
	ls.slotMapLock.Unlock()

	ls.sigsMapLock.Lock()
	if _, ok := ls.sigsMap[msg.Wn]; !ok {
		ls.sigsMap[msg.Wn] = &SigsPerWave{
			sigsbytes: make([][]byte, 0),
			wn:        msg.Wn,
		}
	}
	ls.sigsMapLock.Unlock()

	m := ls.sigsMap[msg.Wn]
	m.sigsLock.Lock()
	defer m.sigsLock.Unlock()

	if len(m.sigsbytes) >= f+1 {

		ls.slotMapLock.Lock()
		if _, ok := ls.slotMap[msg.Wn]; ok {

			ls.slotMapLock.Unlock()
			return nil
		}

		slot, err := ls.ChooseLeader(m)
		if err != nil {
			ls.slotMapLock.Unlock()
			return err
		}

		ls.slotMap[msg.Wn] = &slot
		ls.leaderChosenLock.Lock()
		if _, ok := ls.leaderChosenChan[msg.Wn]; !ok {
			ls.leaderChosenChan[msg.Wn] = make(chan bool, 1)
		}

		ls.leaderChosenChan[msg.Wn] <- true
		//fmt.Println("has sent chan")
		ls.leaderChosenLock.Unlock()
		ls.slotMapLock.Unlock()

		go func() {
			time.Sleep(2 * time.Second)
			ls.sigsMapLock.Lock()
			delete(ls.sigsMap, msg.Wn)

			ls.sigsMapLock.Unlock()
			// ls.leaderChosenLock.Lock()
			// close(ls.leaderChosenChan[msg.Wn])
			// delete(ls.leaderChosenChan, msg.Wn)
			// ls.leaderChosenLock.Unlock()
		}()
		fmt.Println("the leader slot for wave ", msg.Wn, "  is  ", slot)

	} else {
		m.sigsbytes = append(m.sigsbytes, msg.Sig)
	}
	//m.sigsLock.Unlock()
	return nil
	//fmt.
}

func (ls *LeaderSelector) ChooseLeader(sigs *SigsPerWave) (int, error) {
	bytes := make([]byte, binary.MaxVarintLen64)
	_ = binary.PutVarint(bytes, int64(sigs.wn*3+2))
	//fmt.Println(bytes)
	//fmt.Println(sigs.sigsbytes)
	intactSig := sign.AssembleIntactTSPartial(sigs.sigsbytes, ls.n.cfg.TSPubKey, bytes, f+1, 5*f+1)
	ok, err := sign.VerifyTS(ls.n.cfg.TSPubKey, bytes, intactSig)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("verify threshold signature failed")
	}
	lastByte := intactSig[len(intactSig)-1]
	lastInt := int(lastByte)
	return lastInt % (5*f + 1), nil

}
