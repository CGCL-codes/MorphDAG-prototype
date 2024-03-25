package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/PlainDAG/go-PlainDAG/core/ttype"
)

func (m *BasicMsg) DisplayinJson() error {

	b, _ := json.Marshal(m)

	fmt.Println(string(b))
	return nil
}

func (m *BasicMsg) Encode() ([]byte, error) {
	h := sha256.Sum256([]byte(fmt.Sprintf("%v", m)))
	return h[:], nil
}

func (m *BasicMsg) GetRN() int {
	return m.Rn
}

func (m *BasicMsg) GetRefs() [][]byte {
	return m.References
}

func (m *BasicMsg) GetHash() []byte {
	return m.Hash
}

func (m *BasicMsg) GetTimestamp() int64 {
	return m.Timestamp
}

func (m *BasicMsg) GetSource() []byte {
	return m.Source
}

func (m *BasicMsg) Serialize() ([]byte, error) {
	var encoded bytes.Buffer

	enc := gob.NewEncoder(&encoded)
	err := enc.Encode(*m)
	if err != nil {
		return nil, err
	}

	return encoded.Bytes(), nil
}

func (m *BasicMsg) Deserialize(data []byte) {
	decoder := gob.NewDecoder(bytes.NewReader(data))
	err := decoder.Decode(m)
	if err != nil {
		log.Panic(err)
	}
}

// msg is the target message to be checked
// msgbyrounds are the messages whose round number is less than message m but larger than the target message
// targetmsground is the messageround whose round number is equal to the target message

func (m *BasicMsg) HavePath(msg Message, rounds []*Round, targetround *Round) (bool, error) {
	// hashes, indexes := m.GetRefs()
	refs := m.GetRefs()
	for _, round := range rounds {
		msgs, err := round.getMsgByRefsBatch(refs)
		if err != nil {
			panic(err)
		}
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
		refs = trueRefs

	}
	msgtocheck, err := targetround.getMsgByRefsBatch(refs)
	if err != nil {
		panic(err)
	}
	for _, m := range msgtocheck {
		if bytes.Equal(m.GetHash(), msg.GetHash()) {
			return true, nil
		}
	}
	return false, nil

}

func (m *BasicMsg) VerifyFields(n *Node) error {
	if len(m.References) < 4*f+1 {
		return errors.New("not enough references")
	}
	if n.cfg.StringpubkeyMap[string(m.Source)] == nil {
		return errors.New("no such public key")
	}
	// newm := BasicMsg{
	// 	Rn:         m.Rn,
	// 	References: m.References,
	// 	Source:     m.Source,
	// 	plaintext:  m.plaintext,
	// }
	// hash, err := newm.Encode()
	// if err != nil {
	// 	return err
	// }
	// if !bytes.Equal(hash, m.Hash) {
	// 	return errors.New("hash not match")
	// }
	return nil

}

func (m *BasicMsg) AfterAttach(n *Node) error {
	//fmt.Println("mround message, do nothing, wave ", m.GetRN()/3)
	return nil
}

func (m *BasicMsg) Commit(n *Node) error {
	//todo
	return nil
}

func (m *BasicMsg) GetPlainMsgs() [][]byte {
	return m.Plainmsg
}

func NewBasicMsg(rn int, refs [][]byte, source []byte, txs []*ttype.Transaction) (*BasicMsg, error) {
	plainmsgs := make([][]byte, Batchsize)

	for _, t := range txs {
		payload, _ := json.Marshal(*t)
		plainmsgs = append(plainmsgs, payload)
	}

	m := BasicMsg{
		Rn:         rn,
		References: refs,
		Source:     source,
		Plainmsg:   plainmsgs,
	}
	m.Timestamp = time.Now().UnixMilli()

	var err error
	m.Hash, err = m.Encode()
	return &m, err
}

func GenesisBasicMsg(rn int, refs [][]byte, source []byte) (*BasicMsg, error) {
	plainmsgs := make([][]byte, Batchsize)
	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	//z := zipf.NewZipf(r, skew, 10000)

	for i := 0; i < Batchsize; i++ {
		//payload := generatePayload("getBalance", strconv.Itoa(30), strconv.Itoa(40), strconv.Itoa(50), strconv.Itoa(60))
		//newTx := ttype.GenesisTransaction(0, uint32(10), uint64(20), []byte("A"), []byte("K"), payload)
		//data, _ := json.Marshal(*newTx)
		plainmsgs = append(plainmsgs, messageconst)
	}
	//message = append(message, messageconst...)
	m := BasicMsg{
		Rn:         rn,
		References: refs,
		Source:     source,
		Plainmsg:   plainmsgs,
	}

	var err error
	m.Hash, err = m.Encode()
	return &m, err
}
