package core

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"time"
)

type Stack []*FRoundMsg

func (s *Stack) Push(value *FRoundMsg) {
	*s = append(*s, value)
}

func (s *Stack) Pop() (*FRoundMsg, bool) {
	if len(*s) == 0 {
		return nil, false
	}
	index := len(*s) - 1
	value := (*s)[index]
	*s = (*s)[:index]
	return value, true
}

func (fm *FRoundMsg) AfterAttach(n *Node) error {

	//fmt.Println("received f-round message at wave", fm.GetRN()/3, "from node", n.cfg.StringIdMap[string(fm.GetSource())])
	//fmt.Println("waiting for the commit phase")
	return nil
}

func (fm *FRoundMsg) Commit(n *Node) error {
	n.committer.commitSyncLock.Lock()
	defer n.committer.commitSyncLock.Unlock()
	// log.Println("now f-round message at wave", fm.GetRN()/3, "from node", n.cfg.StringIdMap[string(fm.GetSource())], "is being locally committed")

	if fm.GetRN() == 3 || fm.EmbededLeader == 0 {
		n.committer.decidedWaveNumber = fm.GetRN() / 3
		fm.commitWithEmbeded(n)
		return nil
	}
	// if fm.EmbededLeader == 0 {
	// 	fm.commitWithEmbeded(n)
	// 	return nil
	// }

	leaderStack := Stack{}
	msg := fm
	for i := fm.GetRN(); i > n.committer.decidedWaveNumber*3; {
		leaderStack.Push(msg)
		fmt.Println("now pushing leader in round", msg.GetRN(), "   from node   ", n.cfg.StringIdMap[string(msg.GetSource())])
		if msg.EmbededLeader == 0 {
			break
		}
		lastLeaderRound := n.bc.GetRound(msg.EmbededLeader)
		lastLeaderMsgs := lastLeaderRound.msgs[*n.ls.slotMap[msg.EmbededLeader/3]]
		msg = lastLeaderMsgs[0].(*FRoundMsg)
		i = msg.GetRN()
		//n.committer.decidedWaveNumber = fm.GetRN() / 3

	}
	n.committer.decidedWaveNumber = fm.GetRN() / 3
	for {
		msg, ok := leaderStack.Pop()

		if !ok {
			break
		}
		fmt.Println("now commit leader in round", msg.GetRN(), "   from node   ", n.cfg.StringIdMap[string(msg.GetSource())])
		msg.commitWithEmbeded(n)
	}
	return nil
}
func (fm *FRoundMsg) commitWithEmbeded(n *Node) error {
	var commitedMsgs []Message

	go n.prefetchStates([]Message{fm})

	for i := fm.LeastEmbededRn; i < fm.GetRN(); i++ {
		hashes := fm.EmbededRefs[i]
		//fmt.Println(fm.LeastEmbededRn)
		//fmt.Println(hashes)
		round := n.bc.GetRound(i)
		msgs, _ := round.getMsgByRefsBatch(hashes)
		go n.prefetchStates(msgs)
		//n.committer.unCommittedlock.Lock()
		for _, msg := range msgs {
			id := n.cfg.StringIdMap[string(msg.GetSource())]
			n.committer.removeUncommitted(i, id)

			if i > n.latestRound { // for measuring tps
				if i == 1 {
					n.start = time.Now()
				}
				if i%100 == 0 {
					duration := time.Since(n.start)
					log.Println("commit round: ", i, " duration: ", duration.Milliseconds(), " ms")
				}
				n.latestRound = i
			}
			if id == n.cfg.Id { // for measuring block confirmation latency
				log.Println("commit message at round", i, "from node", id, "commit latency: ", time.Now().UnixMilli()-msg.GetTimestamp(), " ms")
			}
		}
		commitedMsgs = append(commitedMsgs, msgs...)
		//n.committer.unCommittedlock.Unlock()
	}
	n.committer.removeUncommitted(fm.GetRN(), n.cfg.StringIdMap[string(fm.GetSource())])
	log.Println("commit leader message at round", fm.GetRN(), "from node", n.cfg.StringIdMap[string(fm.GetSource())])
	commitedMsgs = append(commitedMsgs, fm)
	go n.processTxs(commitedMsgs)
	return nil
}

func (fm *FRoundMsg) GetTimestamp() int64 {
	return fm.Timestamp
}

func (fm *FRoundMsg) Serialize() ([]byte, error) {
	var encoded bytes.Buffer

	enc := gob.NewEncoder(&encoded)
	err := enc.Encode(*fm)
	if err != nil {
		return nil, err
	}

	return encoded.Bytes(), nil
}

func (fm *FRoundMsg) Deserialize(data []byte) {
	decoder := gob.NewDecoder(bytes.NewReader(data))
	err := decoder.Decode(fm)
	if err != nil {
		log.Panic(err)
	}
}

func (fm *FRoundMsg) GetPlainMsgs() [][]byte {
	return fm.BasicMsg.Plainmsg
}

func NewFRoundMsg(b *BasicMsg, leaderrn int, leaderref []byte, leastrn int, embeded map[int][][]byte) *FRoundMsg {
	return &FRoundMsg{
		BasicMsg:       b,
		EmbededLeader:  leaderrn,
		LeaderRef:      leaderref,
		LeastEmbededRn: leastrn,
		EmbededRefs:    embeded,
	}
}

// n.ls.slotMapLock.Lock()
// if _, ok := n.ls.slotMap[fm.GetRN()/3]; ok {
// 	slot := n.ls.slotMap[fm.GetRN()/3]
// 	if *slot != n.cfg.StringIdMap[string(fm.GetSource())] {
// 		n.ls.slotMapLock.Unlock()
// 		return
// 	} else {
// 		fm.waitTillCommit(n)
// 	}
// } else {
// 	n.ls.slotMapLock.Unlock()
// 	n.ls.leaderChosenLock.Lock()
// 	if _, ok := n.ls.leaderChosenChan[fm.GetRN()/3]; !ok {
// 		n.ls.leaderChosenChan[fm.GetRN()/3] = make(chan bool, 1)
// 	}
// 	ch := n.ls.leaderChosenChan[fm.GetRN()/3]
// 	n.ls.leaderChosenLock.Unlock()
// 	<-ch
// 	ch <- true
// 	//fmt.Println("has sent chan")

// 	n.ls.slotMapLock.Lock()
// 	slot := n.ls.slotMap[fm.GetRN()/3]
// 	if *slot != n.cfg.StringIdMap[string(fm.GetSource())] {
// 		n.ls.slotMapLock.Unlock()
// 		return
// 	} else {
// 		fm.waitTillCommit(n)
// 	}
// }

// func (fm *FRoundMsg) waitTillCommit(n *Node) {
// 	n.committer.hasTriggerLock.Lock()
// 	if _, ok := n.committer.hasTriggerMap[fm.GetRN()/3]; !ok {
// 		n.committer.hasTriggerLock.Unlock()
// 		n.committer.hasCommittedMapLock.Lock()
// 		if _, ok := n.committer.hasCommittedMap[fm.GetRN()/3]; !ok {
// 			n.committer.hasCommittedMap[fm.GetRN()/3] = make(chan bool, 1)
// 		}
// 		ch := n.committer.hasCommittedMap[fm.GetRN()/3]
// 		n.committer.hasCommittedMapLock.Unlock()
// 		<-ch
// 		ch <- true
// 		n.committer.voteLock.Lock()
// 		if n.committer.voteMap[fm.GetRN()/3][string(fm.GetHash())] >= 4*f+1 {
// 			n.committer.voteLock.Unlock()
// 			fmt.Println("now start to commit f-round message at wave", fm.GetRN()/3, "with its index at", n.cfg.StringIdMap[string(fm.GetSource())])
// 		}
// 	} else {
// 		n.committer.hasTriggerLock.Unlock()
// 		n.committer.voteLock.Lock()
// 		if n.committer.voteMap[fm.GetRN()/3][string(fm.GetHash())] >= 4*f+1 {
// 			n.committer.voteLock.Unlock()
// 			fmt.Println("now start to commit f-round message at wave", fm.GetRN()/3, "with its index at", n.cfg.StringIdMap[string(fm.GetSource())])
// 		}
// 	}

// }
