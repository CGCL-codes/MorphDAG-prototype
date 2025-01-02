package core

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
)

func NewLroundMsg(arefs [][]byte, b *BasicMsg) (*LRoundMsg, error) {

	return &LRoundMsg{
		BasicMsg: b,
		ARefs:    arefs,
	}, nil
}

func (lm *LRoundMsg) AfterAttach(n *Node) error {
	lround := n.bc.GetRound(lm.BasicMsg.GetRN() - 2)

	indexes, error := lround.getIndexByRefsBatch(lm.ARefs)
	//fmt.Println(len(lm.ARefs))
	if error != nil {
		return error
	}

	fmt.Println("received l-round message at round ", lm.GetRN(), "from node", n.cfg.StringIdMap[string(lm.BasicMsg.GetSource())], " and it A-References ", indexes)
	n.ls.slotMapLock.Lock()
	if _, ok := n.ls.slotMap[lm.BasicMsg.GetRN()/3]; ok {
		//fmt.Println("wavenumber ", lm.GetRN()/3)
		round := n.bc.GetRound(lm.BasicMsg.GetRN() - 2)
		lm.checkAddToVote(round, *n.ls.slotMap[lm.GetRN()/3], n)
		n.ls.slotMapLock.Unlock()
	} else {
		n.ls.slotMapLock.Unlock()
		n.ls.leaderChosenLock.Lock()

		if _, ok := n.ls.leaderChosenChan[lm.GetRN()/3]; !ok {
			n.ls.leaderChosenChan[lm.GetRN()/3] = make(chan bool, 1)
		}
		ch := n.ls.leaderChosenChan[lm.GetRN()/3]
		n.ls.leaderChosenLock.Unlock()
		<-ch
		ch <- true
		//fmt.Println("wavenumber locked", lm.GetRN()/3)
		n.ls.slotMapLock.Lock()
		slotnumber := n.ls.slotMap[lm.BasicMsg.GetRN()/3]
		n.ls.slotMapLock.Unlock()
		round := n.bc.GetRound(lm.BasicMsg.GetRN() - 2)
		lm.checkAddToVote(round, *slotnumber, n)
	}
	return nil
}

func (lm *LRoundMsg) GetARefs() [][]byte {
	return lm.ARefs
}

func (lm *LRoundMsg) checkAddToVote(r *Round, slotnumber int, n *Node) {
	r.messageLock.Lock()

	arefs := lm.GetARefs()
	msgs := r.msgs[slotnumber]
	messagemap := make(map[string]bool)
	for _, msg := range msgs {
		messagemap[string(msg.GetHash())] = true
	}
	r.messageLock.Unlock()
	//fmt.Println("hello here?")
	for _, aref := range arefs {
		if _, ok := messagemap[string(aref)]; ok {
			n.committer.addVote(lm.GetRN()/3, string(aref))
			return
		}
	}

}

func (lm *LRoundMsg) Commit(n *Node) error {
	return nil
}

func (lm *LRoundMsg) Serialize() ([]byte, error) {
	var encoded bytes.Buffer

	enc := gob.NewEncoder(&encoded)
	err := enc.Encode(*lm)
	if err != nil {
		return nil, err
	}

	return encoded.Bytes(), nil
}

func (lm *LRoundMsg) Deserialize(data []byte) {
	decoder := gob.NewDecoder(bytes.NewReader(data))
	err := decoder.Decode(lm)
	if err != nil {
		log.Panic(err)
	}
}

func (lm *LRoundMsg) GetPlainMsgs() [][]byte {
	return lm.BasicMsg.Plainmsg
}

func (lm *LRoundMsg) GetTimestamp() int64 {
	return lm.Timestamp
}
