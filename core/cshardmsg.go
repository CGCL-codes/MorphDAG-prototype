package core

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/PlainDAG/go-PlainDAG/core/ttype"
	"github.com/PlainDAG/go-PlainDAG/sign"
	"go.dedis.ch/kyber/v3/share"
)

func NewCShardMsg(
	sourceShard int, targetShard int, accStateMap ttype.Payload,
	id int, source []byte, tSPrvKey *share.PriShare,
) (*CShardMsg, error) {

	var err error

	m := CShardMsg{
		SourceShard: sourceShard,
		TargetShard: targetShard,
		AccStateMap: accStateMap,
		CShardMsgId: id,
		Source:      source,
	}

	// get hash
	m.MsgHash = m.Encode()
	m.ThresSig = sign.SignTSPartial(tSPrvKey, m.MsgHash)

	return &m, err
}

func (m *CShardMsg) DisplayinJson() error {

	b, _ := json.Marshal(m)

	fmt.Println(string(b))
	return nil
}

func (m *CShardMsg) Encode() []byte {
	// log.Println("encode: ", m.AccStateMap, " ", m.CShardMsgId)
	// var buffer bytes.Buffer
	// payloadData := m.AccStateMap.Serialize()
	// buffer.Write(payloadData)
	// buffer.Write([]byte(fmt.Sprintf("%d", m.CShardMsgId)))
	// h := sha256.Sum256(buffer.Bytes())
	h := sha256.Sum256([]byte(fmt.Sprintf("%v%v%v", m.CShardMsgId, m.SourceShard, m.TargetShard)))
	// log.Println("encode: ", h)
	return h[:]
}

func (m *CShardMsg) GetSourceShard() int {
	return m.SourceShard
}

func (m *CShardMsg) GetTargetShard() int {
	return m.TargetShard
}

func (m *CShardMsg) GetSource() []byte {
	return m.Source
}

func (m *CShardMsg) GetHash() []byte {
	return m.MsgHash
}

func (m *CShardMsg) GetId() int {
	return m.CShardMsgId
}
