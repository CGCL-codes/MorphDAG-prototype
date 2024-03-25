package core

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

type Round struct {
	// msgs        [5*f + 1][]Message
	msgs [][]Message

	roundNumber int
	messageLock sync.RWMutex

	//checkmap is used to check whether a message has been received
	//1. upon a message is received, we first check if these's a bool channel for this messgage.hash in the checkmap
	// if not, it means there's no messages in higher round waiting for this message
	// if so, we put a a bool value into the channel
	// 2.  when we receive a message, we first check if all the messages it references have been stored in the DAG
	// if true, we simply store it in the DAG.
	// if false, we find the hashes that are not in the DAG, and use these hashes to construct the map and let the channel block this message
	// until all messages it references are received
	checkMap     map[string]chan bool
	checkMapLock sync.RWMutex

	// this map below checks if a message from a certain node has been received
	// if true, tag this node as true to indicate that this node has sent a message in this round
	// this is relevant to the tryhandle function in the msghandler.go when counting the 4f+1 threshold
	isReceivedMap map[string]bool

	isFround bool
}

func (r *Round) getMsgByRef(hash []byte) (Message, error) {
	r.messageLock.Lock()
	//fmt.Println("hello?")
	defer r.messageLock.Unlock()
	for _, msg := range r.msgs {
		for _, m := range msg {
			if bytes.Equal(m.GetHash(), hash) {
				return m, nil
			}
		}
	}
	return nil, errors.New("no such message for" + string(hash) + "in round" + strconv.Itoa(r.roundNumber))

}

func (r *Round) getMsgByRefsBatch(hashes [][]byte) ([]Message, error) {
	r.messageLock.RLock()
	defer r.messageLock.RUnlock()
	msgs := make([]Message, 0)
	searchMap := make(map[string]bool)
	for _, hash := range hashes {
		searchMap[string(hash)] = true
	}

	for _, msg := range r.msgs {
		for _, m := range msg {
			if searchMap[string(m.GetHash())] {
				msgs = append(msgs, m)
			}
		}
	}

	return msgs, nil
}

func getRefsByMsgsBatch(msgs []Message) ([][]byte, error) {

	uniqueRefs := make(map[string]bool)
	for _, m := range msgs {
		refs := m.GetRefs()
		for _, ref := range refs {
			uniqueRefs[string(ref)] = true
		}
	}

	trueRefs := make([][]byte, 0)

	for k, v := range uniqueRefs {
		if v {

			trueRefs = append(trueRefs, []byte(k))
		}
	}
	refs := trueRefs
	return refs, nil
}

func (r *Round) checkCanAttach(m Message) ([][]byte, bool) {
	r.messageLock.RLock()
	defer r.messageLock.RUnlock()
	refs := m.GetRefs()
	searchMap := make(map[string]bool)
	for _, ref := range refs {
		searchMap[string(ref)] = true
	}

	for _, msg := range r.msgs {
		for _, m := range msg {
			if searchMap[string(m.GetHash())] {
				delete(searchMap, string(m.GetHash()))
			}
		}
	}

	if len(searchMap) == 0 {
		return nil, true
	}
	missingRefs := make([][]byte, 0)
	for k, _ := range searchMap {
		missingRefs = append(missingRefs, []byte(k))
	}

	return missingRefs, false
}

func (r *Round) tryAttach(m Message, currentRound *Round, id int) {
	// CheckCanAttach checks whether all the msgs message m references are stored in the last round

	//currentRound.checkMapLock.Lock()
	missingrefs, canattach := r.checkCanAttach(m)
	//refs := m.GetRefs()
	//for i := 0; i < len(r.msgs); i++ {
	//	fmt.Println(r.msgs[i][0].GetHash())
	//	//fmt.Println(refs[i])
	//}
	//fmt.Println(len(missingrefs))
	//r.checkMapLock.Lock()
	// if true, attach msg m to the current round
	if canattach {
		currentRound.attachMsg(m, id)

		//fmt.Println("can attach   " + strconv.Itoa(id))
		currentRound.checkMapLock.Lock()
		if _, ok := currentRound.checkMap[string(m.GetHash())]; !ok {
			currentRound.checkMap[string(m.GetHash())] = make(chan bool, 1)

			//r.checkMapLock.Unlock()
		}
		currentRound.checkMap[string(m.GetHash())] <- true
		currentRound.checkMapLock.Unlock()
		//r.checkMapLock.Unlock()
		go func() {
			time.Sleep(2 * time.Second)
			currentRound.checkMapLock.Lock()

			close(currentRound.checkMap[string(m.GetHash())])
			currentRound.checkMapLock.Unlock()
		}()
		return
	}
	r.checkMapLock.Lock()
	//fmt.Println("cant attach" + strconv.Itoa(id))
	// r.checkMapLock.Lock()
	//fmt.Println(1)
	for _, ref := range missingrefs {
		if _, ok := r.checkMap[string(ref)]; !ok {
			r.checkMap[string(ref)] = make(chan bool, 1)
		}
	}
	r.checkMapLock.Unlock()
	//currentRound.checkMapLock.Unlock()
	var wg sync.WaitGroup
	//fmt.Println(m.GetRN())
	for _, ref := range missingrefs {
		wg.Add(1)
		rcopy := make([]byte, len(ref))
		copy(rcopy, ref)

		r.checkMapLock.Lock()
		c := r.checkMap[string(rcopy)]
		r.checkMapLock.Unlock()

		go func() {
			b := <-c
			c <- b
			wg.Done()
		}()
	}
	wg.Wait()
	//fmt.Println("still there?")
	currentRound.checkMapLock.Lock()

	if _, ok := currentRound.checkMap[string(m.GetHash())]; !ok {
		currentRound.checkMap[string(m.GetHash())] = make(chan bool, 1)

	}
	currentRound.checkMap[string(m.GetHash())] <- true

	go func() {
		time.Sleep(2 * time.Second)
		currentRound.checkMapLock.Lock()

		close(currentRound.checkMap[string(m.GetHash())])
		currentRound.checkMapLock.Unlock()
	}()
	currentRound.attachMsg(m, id)
	currentRound.checkMapLock.Unlock()
	return
}

func (r *Round) replicate(m Message, currentRound *Round, id int) {

	currentRound.checkMapLock.Lock()
	missingrefs, canattach := r.checkCanAttach(m)
	r.checkMapLock.Lock()
	// if true, attach msg m to the current round
	if canattach {
		currentRound.attachMsg(m, id)

		fmt.Println("can attach   " + strconv.Itoa(id))
		if _, ok := currentRound.checkMap[string(m.GetHash())]; !ok {
			//currentRound.checkMap[string(m.GetHash())] = make(chan bool)
			currentRound.checkMapLock.Unlock()
			r.checkMapLock.Unlock()
			return
		}
		currentRound.checkMap[string(m.GetHash())] <- true
		currentRound.checkMapLock.Unlock()
		r.checkMapLock.Unlock()
		// go func() {
		// 	time.Sleep(2 * time.Second)
		// 	currentRound.checkMapLock.Lock()

		// 	close(currentRound.checkMap[string(m.GetHash())])
		// 	currentRound.checkMapLock.Unlock()
		// }()
		return
	}
	fmt.Println("cant attach" + strconv.Itoa(id))
	// r.checkMapLock.Lock()
	//fmt.Println(1)
	for _, ref := range missingrefs {
		if _, ok := r.checkMap[string(ref)]; !ok {
			r.checkMap[string(ref)] = make(chan bool, 1)
		}
	}
	r.checkMapLock.Unlock()
	currentRound.checkMapLock.Unlock()
	var wg sync.WaitGroup

	for _, ref := range missingrefs {
		wg.Add(1)
		rcopy := make([]byte, len(ref))
		copy(rcopy, ref)
		c := r.checkMap[string(rcopy)]

		go func() {
			b := <-c
			c <- b
			wg.Done()
		}()

	}
	wg.Wait()
	fmt.Println("still there?")
	currentRound.checkMapLock.Lock()

	if _, ok := currentRound.checkMap[string(m.GetHash())]; !ok {
		//currentRound.checkMap[string(m.GetHash())] = make(chan bool)
		currentRound.attachMsg(m, id)
		currentRound.checkMapLock.Unlock()
		return
	}
	currentRound.checkMap[string(m.GetHash())] <- true

	// go func() {
	// 	time.Sleep(2 * time.Second)
	// 	currentRound.checkMapLock.Lock()

	// 	close(currentRound.checkMap[string(m.GetHash())])
	// 	currentRound.checkMapLock.Unlock()
	// }()
	currentRound.attachMsg(m, id)
	currentRound.checkMapLock.Unlock()
	return
}
func (r *Round) attachMsg(m Message, id int) {
	r.messageLock.Lock()

	//fmt.Println(id)
	r.msgs[id] = append(r.msgs[id], m)
	r.messageLock.Unlock()

}

func (r *Round) retMsgsToRef() [][]byte {
	msgsByte := make([][]byte, 0)
	r.messageLock.RLock()
	for _, msg := range r.msgs {

		if msg == nil {
			continue
		}
		//fmt.Println(len(msg))
		for _, m := range msg {

			if m == nil {
				continue
			}
			msgsByte = append(msgsByte, m.GetHash())
			break
		}

	}
	r.messageLock.RUnlock()
	return msgsByte
}

// this func is used to remove the committed message from the memory to avoid memory leaks
// In the future we use a database to store the committed messages but not for now
func (r *Round) rmvMsgWhenCommitted(msg Message, id int) error {
	r.messageLock.Lock()
	defer r.messageLock.Unlock()
	for i, m := range r.msgs[id] {
		if bytes.Equal(m.GetHash(), msg.GetHash()) {
			r.msgs[id] = append(r.msgs[id][:i], r.msgs[id][i+1:]...)
			return nil
		}
	}
	return errors.New("message not found")
}

// This function is only used for testing and lets the system continue running when the code is not yet finished.
func (r *Round) rmvAllMsgsWhenCommitted() {
	r.messageLock.Lock()
	defer r.messageLock.Unlock()
	for i := 0; i < 5*f+1; i++ {
		r.msgs[i] = make([]Message, 0)
	}
}

func (r *Round) getIndexByRefsBatch(refs [][]byte) ([]int, error) {
	r.messageLock.RLock()
	defer r.messageLock.RUnlock()
	indexs := make([]int, 0)
	refsmap := make(map[string]bool)
	for _, ref := range refs {
		refsmap[string(ref)] = true
	}
	for i, msg := range r.msgs {
		for _, m := range msg {
			if _, ok := refsmap[string(m.GetHash())]; ok {
				indexs = append(indexs, i)
				break
			}
		}
	}

	return indexs, nil
}

func (r *Round) genArefs(msg Message, mround *Round) ([][]byte, error) {
	refs := msg.GetRefs()
	arefs := make([][]byte, 0)
	msgs, err := mround.getMsgByRefsBatch(refs)
	if err != nil {
		return nil, err
	}
	refs, err = getRefsByMsgsBatch(msgs)
	if err != nil {
		return nil, err
	}
	targetmsgs, err := r.getMsgByRefsBatch(refs)
	if err != nil {
		return nil, err
	}

	uniqueMsgs := make(map[string]bool)
	for _, m := range targetmsgs {
		if _, ok := uniqueMsgs[string(m.GetSource())]; !ok {
			arefs = append(arefs, m.GetHash())
			uniqueMsgs[string(m.GetSource())] = true
		}
	}
	return arefs, nil
}
func newRound(rn int, m Message, id int) (*Round, error) {
	if m.GetRN() != rn {
		return nil, errors.New("round number not match")
	}
	// var msglists [5*f + 1][]Message
	var msglists [][]Message

	for i := 0; i < 5*f+1; i++ {
		// msglists[i] = make([]Message, 0)
		msglists = append(msglists, make([]Message, 0))
	}
	msglists[id] = append(msglists[id], m)
	return &Round{
		msgs:          msglists,
		roundNumber:   rn,
		checkMap:      make(map[string]chan bool),
		isReceivedMap: make(map[string]bool),
		messageLock:   sync.RWMutex{},
		checkMapLock:  sync.RWMutex{},
	}, nil
}

// this function returns the first round(round 0) in the DAG and is hardcoded
func newStaticRound() (*Round, error) {
	var msglists [][]Message

	for i := 0; i < 5*f+1; i++ {
		// msglists[i] = make([]Message, 0)
		msglists = append(msglists, make([]Message, 0))
		if msg, err := GenesisBasicMsg(0, [][]byte{}, []byte("Source"+strconv.Itoa(i))); err != nil {
			return nil, err
		} else {
			msglists[i] = append(msglists[i], msg)
		}

	}
	return &Round{
		msgs:          msglists,
		roundNumber:   0,
		checkMap:      make(map[string]chan bool),
		isReceivedMap: make(map[string]bool),
		messageLock:   sync.RWMutex{},
		checkMapLock:  sync.RWMutex{},
	}, nil
}
