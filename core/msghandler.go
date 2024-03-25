package core

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/PlainDAG/go-PlainDAG/core/ttype"
	"github.com/PlainDAG/go-PlainDAG/sign"
	"github.com/PlainDAG/go-PlainDAG/utils"
)

type Messagehandler interface {
	handleMsg(msg Message, sig []byte, msgbytes []byte) error
	handleThresMsg(msg *ThresSigMsg, sig []byte, msgbytes []byte) error
	getFutureMsgByRound(rn int) []Message
	tryHandle(msg Message) error
	handleFutureVers(rn int) error

	buildContextForRound(rn int)
	signalFutureVersHandled(rn int)
	readyForRound(rn int)

	handleCShardMsg(msg *CShardMsg, sig []byte, msgbytes []byte) error
}

type Statichandler struct {
	n              *Node
	futureVers     map[int][]Message
	futureVerslock sync.RWMutex

	waitingChanMap     map[int]chan bool
	waitingChanMaplock sync.RWMutex

	readyToSendMap     map[int]chan bool
	readyToSendMapLock sync.RWMutex

	isDoneWithFutureVers map[int]chan bool
	isDoneWithFuturelock sync.RWMutex

	isSent     map[int]bool
	isSentLock sync.RWMutex

	recvCSMsgMap    map[string]bool
	waitingCSMsgMap map[string][][]byte // CSMsgIdString -> a list of TSPartial
	CSMsgLock       sync.RWMutex
}

func (sh *Statichandler) signalFutureVersHandled(rn int) {
	sh.isDoneWithFuturelock.Lock()

	ch := sh.isDoneWithFutureVers[rn]
	ch <- true
	sh.isDoneWithFuturelock.Unlock()

}
func (sh *Statichandler) readyForRound(rn int) {
	if rn == 1 {
		return
	}

	// sh.waitingChanMaplock.Lock()
	// chwaiting := sh.waitingChanMap[rn-1]
	// sh.waitingChanMaplock.Unlock()
	//fmt.Println("are you stuck here in waiting?")

	// var wg sync.WaitGroup
	// for i := 0; i < 4*f; i++ {
	// 	wg.Add(1)
	// 	go func() {
	// 		<-chwaiting
	// 		fmt.Println("done")
	// 		wg.Done()
	// 	}()
	// }

	// wg.Wait()
	sh.readyToSendMapLock.Lock()
	chready := sh.readyToSendMap[rn-1]
	sh.readyToSendMapLock.Unlock()

	<-chready

	//fmt.Println("are you stuck here in waiting?")
	//fmt.Println("done waiting for round " + strconv.Itoa(rn-1))
	sh.isDoneWithFuturelock.RLock()
	chfuturedone := sh.isDoneWithFutureVers[rn-1]

	sh.isDoneWithFuturelock.RUnlock()
	//fmt.Println("are you stuck here in ready for round?")
	<-chfuturedone
	//fmt.Println("are you stuck here in ready for round?")

	close(chfuturedone)

	//fmt.Println("pace to round ", rn)
	if rn%3 == 0 && rn > 3 {
		sh.n.ls.leaderChosenLock.Lock()
		if _, ok := sh.n.ls.leaderChosenChan[rn/3-1]; !ok {
			//fmt.Println("generate?")
			sh.n.ls.leaderChosenChan[rn/3-1] = make(chan bool, 1)
		}
		ch := sh.n.ls.leaderChosenChan[rn/3-1]
		sh.n.ls.leaderChosenLock.Unlock()
		<-ch
		ch <- true
		//fmt.Println("has chosen leader for wave  ", rn/3-1, "now step to round", rn)
		go func() {
			time.Sleep(2 * time.Second)
			sh.n.ls.leaderChosenLock.Lock()
			close(ch)
			delete(sh.n.ls.leaderChosenChan, rn/3-1)
			sh.n.ls.leaderChosenLock.Unlock()
		}()
	}

}
func (sh *Statichandler) buildContextForRound(rn int) {
	sh.addWaitingChan(rn)
	sh.addIsDoneChan(rn)
	sh.addReadyToSendChan(rn)

}

func (sh *Statichandler) addIsDoneChan(rn int) {
	sh.isDoneWithFuturelock.Lock()
	sh.isDoneWithFutureVers[rn] = make(chan bool, 1)
	sh.isDoneWithFuturelock.Unlock()
}

func (sh *Statichandler) addWaitingChan(rn int) {
	sh.waitingChanMaplock.Lock()
	sh.waitingChanMap[rn] = make(chan bool, 4*f-1)
	sh.waitingChanMaplock.Unlock()
}

func (sh *Statichandler) addReadyToSendChan(rn int) {
	sh.readyToSendMapLock.Lock()
	sh.readyToSendMap[rn] = make(chan bool, 1)
	sh.readyToSendMapLock.Unlock()
}

func (sh *Statichandler) handleMsg(msg Message, sig []byte, msgbytes []byte) error {
	err := sh.VerifyandCheckMsg(msg, sig, msgbytes)
	if err != nil {
		return err
	}

	isFuture := sh.storeFutureVers(msg)
	if isFuture {
		return nil
	}

	//msg.DisplayinJson()

	err = sh.tryHandle(msg)
	if err != nil {
		return err
	}
	return sh.workAfterAttach(msg)
}

func (sh *Statichandler) tryHandle(msg Message) error {
	id := sh.n.cfg.StringIdMap[string(msg.GetSource())]

	rn := msg.GetRN()
	lastRound := sh.n.bc.GetRound(rn - 1)
	thisRound := sh.n.bc.GetRound(rn)
	lastRound.tryAttach(msg, thisRound, id)

	sh.n.committer.addToUnCommitted(msg, id)
	//sh.n.committer.addToUnEmbeded(msg, id)

	//fmt.Println("ends here tryhandle1?")
	sh.waitingChanMaplock.Lock()
	ch := sh.waitingChanMap[rn]

	sh.waitingChanMaplock.Unlock()
	//fmt.Println("ends here tryhandle1?")
	if rn%3 != 2 || rn == 2 {
		indexes, error := lastRound.getIndexByRefsBatch(msg.GetRefs())
		//fmt.Println(len(lm.ARefs))
		if error != nil {
			return error

		}
		fmt.Println("received message at round ", msg.GetRN(), "from node", sh.n.cfg.StringIdMap[string(msg.GetSource())], " and it References ", indexes)
	}

	//fmt.Println(msg.GetPlainMsgs())
	// } //fmt.Println("ends here tryhandle2?")
	sh.readyToSendMapLock.Lock()
	if len(ch) < 4*f-1 {
		if !thisRound.isReceivedMap[string(msg.GetSource())] {
			//fmt.Println("received from  " + strconv.Itoa(sh.n.cfg.StringIdMap[string(msg.GetSource())]))
			ch <- true
			thisRound.isReceivedMap[string(msg.GetSource())] = true
		}
	} else {
		//fmt.Println("now 4f for " + strconv.Itoa(rn))
		chready := sh.readyToSendMap[rn]
		thisRound.isReceivedMap[string(msg.GetSource())] = true
		sh.isSentLock.Lock()
		//fmt.Println("here out?")
		if !sh.isSent[rn] {
			//fmt.Println("here in?")
			chready <- true

			sh.isSent[rn] = true
			sh.isSentLock.Unlock()
			close(chready)
			close(ch)
			sh.readyToSendMapLock.Unlock()
			//fmt.Println("handle msg success from    " + strconv.Itoa(id) + "round number: " + strconv.Itoa(rn))
			return nil
		}
		sh.isSentLock.Unlock()
		//chready <- true

	}
	//fmt.Println("handle msg success from    " + strconv.Itoa(id) + "round number: " + strconv.Itoa(rn))

	sh.readyToSendMapLock.Unlock()
	return nil
}

func (sh *Statichandler) storeFutureVers(msg Message) bool {
	sh.futureVerslock.Lock()
	//fmt.Println("stuck here?")
	if msg.GetRN() > int(sh.n.currentround.Load()) {
		//sh.futureVerslock.Lock()
		sh.futureVers[msg.GetRN()] = append(sh.futureVers[msg.GetRN()], msg)
		sh.futureVerslock.Unlock()
		return true
	}
	sh.futureVerslock.Unlock()
	return false
}

func (sh *Statichandler) workAfterAttach(msg Message) error {
	return msg.AfterAttach(sh.n)
}

func (sh *Statichandler) handleFutureVers(rn int) error {

	msgsNextRound := sh.getFutureMsgByRound(rn)
	if msgsNextRound == nil {
		sh.signalFutureVersHandled(rn)
		//fmt.Println("signaled")
		return nil
	}
	sh.futureVerslock.Lock()
	var err error
	//fmt.Println(len(msgsNextRound))

	for _, msg := range msgsNextRound {
		//fmt.Println("handle")
		m := msg
		go func() {
			sh.tryHandle(m)
			sh.workAfterAttach(m)
		}()
	}
	sh.futureVerslock.Unlock()
	//fmt.Println("are you stuck here?")
	sh.signalFutureVersHandled(rn)
	go func() {
		time.Sleep(2 * time.Second)
		sh.futureVerslock.Lock()
		delete(sh.futureVers, rn)
		sh.futureVerslock.Unlock()
	}()
	return err
}
func (sh *Statichandler) VerifyandCheckMsg(msg Message, sig []byte, msgbytes []byte) error {
	b, err := utils.VerifySig(sh.n.cfg.StringpubkeyMap, sig, msgbytes, msg.GetSource())
	// b, err := msg.VerifySig(sh.n, sig, msgbytes)
	if err != nil {
		return err
	}
	if !b {
		return errors.New("signature verification failed")
	}

	if err := msg.VerifyFields(sh.n); err != nil {
		return err
	}
	return nil
}

func (sh *Statichandler) getFutureMsgByRound(rn int) []Message {
	sh.futureVerslock.RLock()
	defer sh.futureVerslock.RUnlock()
	msgs := sh.futureVers[rn]
	return msgs
}

func (sh *Statichandler) handleThresMsg(msg *ThresSigMsg, sig []byte, msgbytes []byte) error {
	b, err := utils.VerifySig(sh.n.cfg.StringpubkeyMap, sig, msgbytes, msg.Source)
	if err != nil {
		return err
	}
	if !b {
		return errors.New("signature verification failed")
	}

	return sh.n.ls.handleTsMsg(msg)
}

/*
handle cross-shard msg
*/
func (sh *Statichandler) handleCShardMsg(msg *CShardMsg, sig []byte, msgbytes []byte) error {
	// verify
	// log.Println("[handleCShardMsg] handle cross-shard msg: ", msg.GetId(), len(msg.AccStateMap))
	err := sh.VerifyandCheckCShardMsg(msg, sig, msgbytes)
	if err != nil {
		return err
	}

	// handle cross-shard msg
	return sh.tryHandleCShardMsg(msg, sig)
}

func (sh *Statichandler) VerifyandCheckCShardMsg(msg *CShardMsg, sig []byte, msgbytes []byte) error {
	// verify sig
	b, err := utils.VerifySig(sh.n.scfg.AllStringpubkeyMap, sig, msgbytes, msg.GetSource())
	if err != nil {
		return err
	}
	if !b {
		return errors.New("signature verification failed")
	}
	// log.Println("[VerifyandCheckCShardMsg] pass sig verification")
	// TODO: verify msg

	return nil
}

/*
try handle cross-shard msg
wait Threshold valid CSMsg, then send it to tx pool
*/
func (sh *Statichandler) tryHandleCShardMsg(msg *CShardMsg, sig []byte) error {

	msgId := fmt.Sprintf("(%d, %d)", msg.GetSourceShard(), msg.GetId())
	// log.Println("[tryHandleCShardMsg] handle cross-shard msg: ", msgId)
	// log.Println("[tryHandleCShardMsg] handle cross-shard msg: ", msg, " tSSig: ", msg.ThresSig)

	sh.CSMsgLock.Lock()
	_, ok := sh.recvCSMsgMap[msgId]
	if !ok {
		sh.recvCSMsgMap[msgId] = false
		sh.waitingCSMsgMap[msgId] = make([][]byte, 0)
		// log.Println("[init sig] id: , ", id, "map: ", sh.waitingCSMsgMap, "isRecv: ", sh.recvCSMsgMap)
	}

	if !sh.recvCSMsgMap[msgId] {

		sh.waitingCSMsgMap[msgId] = append(sh.waitingCSMsgMap[msgId], msg.ThresSig)
		// log.Println("[acc sig]  id: , ", msgId, " sig num: ", len(sh.waitingCSMsgMap[msgId]), " map: ", sh.waitingCSMsgMap, "isRecv: ", sh.recvCSMsgMap)

		if len(sh.waitingCSMsgMap[msgId]) == ConThreshold {

			tSSigList := sh.waitingCSMsgMap[msgId]
			delete(sh.waitingCSMsgMap, msgId)
			sh.recvCSMsgMap[msgId] = true
			sh.CSMsgLock.Unlock()

			// AssembleIntactTSPartial and vefiryTS
			intactSig := sign.AssembleIntactTSPartial(tSSigList, sh.n.cfg.TSPubKey, msg.GetHash(), ConThreshold, 5*f+1)
			ok, err := sign.VerifyTS(sh.n.cfg.TSPubKey, msg.GetHash(), intactSig)
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("verify threshold signature failed")
			}

			accAggTx := ttype.NewAccAggTransaction(
				msg.SourceShard, msg.TargetShard, msg.AccStateMap, msg.CShardMsgId, msg.MsgHash, intactSig,
			)

			// log.Println("[enough sig]  id: , ", msgId, " sig num: ", len(sh.waitingCSMsgMap[msgId]), " map: ", sh.waitingCSMsgMap, "isRecv: ", sh.recvCSMsgMap)
			// log.Println("AccAggMsg: ", accAggMsg)

			// send accAggMsg to tx pool
			candiNodeId := utils.ShuffleNodeIdList(sh.n.scfg.ShardSize, msg.GetHash())
			// TODO a single packager, cannot work under faults
			packager := candiNodeId[0]
			// log.Println("Collect enough sig for cross-shard msg: ", msgId, "nodeid: ", sh.n.cfg.Id, "after shuffle: ", candiNodeId, "number of packagers: ", PackagerNums)
			if sh.n.cfg.Id == packager {
				// send cross-shard msg to tx pool
				// log.Println("Send accAggTx: ", msgId, " ", accAggTx.AAggInfo.CShardMsgId, " to tx pool")
				sh.n.txPool.pending.Append(accAggTx)
			}
		} else {
			sh.CSMsgLock.Unlock()
		}
	} else {
		// ignore the msg
		// log.Println("[ignore sig]  id: , ", id, " map: ", sh.waitingCSMsgMap, "isRecv: ", sh.recvCSMsgMap)
		sh.CSMsgLock.Unlock()
	}
	return nil
}

func NewStatichandler(n *Node) *Statichandler {
	return &Statichandler{
		n:                    n,
		futureVers:           make(map[int][]Message),
		waitingChanMap:       make(map[int]chan bool),
		isDoneWithFutureVers: make(map[int]chan bool),
		readyToSendMap:       make(map[int]chan bool),
		isSent:               make(map[int]bool),

		recvCSMsgMap:    make(map[string]bool),
		waitingCSMsgMap: make(map[string][][]byte),
	}
}
