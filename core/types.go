package core

import (
	"reflect"

	"github.com/PlainDAG/go-PlainDAG/core/ttype"
)

const (
	FMsgTag uint8 = iota

	BMsgTag
	LMsgTag
	TMsgTag
	TxTag
	CMsgTag
)

var f = 1

const rPerwave = 3

var Batchsize = 500

const Ratio = 2
const Skew = 0.6
const SafeConcurrency = 50
const MaximumPoolSize = 100000

// const DB1 = "chaindata_%s"
// const DB2 = "statedata_%s"
const DB1 = "chaindata_%d_%d"
const DB2 = "statedata_%d_%d"
const MaximumProcessors = 100000
const Frequency = 50

var messageconst = []byte{1, 2, 3, 5, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

const plainMsgSize = 250

var RecvNums = f + 1
var SendNums = 2*f + 1

const PackagerNums = 1

var ConThreshold = f + 1

const AccAggInterval = 1

const CSRatio = 3
const MAXTemAcc = 10

type FRoundMsg struct {
	*BasicMsg     `json:"basicmsg"`
	EmbededLeader int    `json:"embededleader"`
	LeaderRef     []byte `json:"leaderref"`

	LeastEmbededRn int              `json:"leastembededrn"`
	EmbededRefs    map[int][][]byte `json:"embededrefs"`
}

type LRoundMsg struct {
	*BasicMsg
	ARefs [][]byte //`json:arefs`
}

type BasicMsg struct {
	Rn         int      `json:"rn"`
	References [][]byte `json:"references"`
	Source     []byte   `json:"source"`
	Hash       []byte   `json:"hash"`
	Plainmsg   [][]byte `json:"plaintext"`
	Timestamp  int64    `json:"timestamp"`
}

type Records struct {
	Rds map[int]Output `json:"records"`
}

type Output struct {
	Hashes  []string `json:"hashes"`
	Latency string   `json:"query_latency"`
}

type StoredMsgs struct {
	Rn     int
	Hashes [][]byte
}

func NewRecords() Records {
	return Records{
		Rds: make(map[int]Output),
	}
}

func NewStoredMsgs(round int, hashes [][]byte) StoredMsgs {
	return StoredMsgs{
		Rn:     round,
		Hashes: hashes,
	}
}

//type PlainMsg struct {
//	Msg []byte
//}

type ThresSigMsg struct {
	Sig []byte `json:"sig"`
	//wave number
	Wn     int    `json:"wn"`
	Source []byte `json:"source"`
}

type Message interface {
	Encode() ([]byte, error)
	Serialize() ([]byte, error)
	Deserialize(data []byte)
	DisplayinJson() error
	GetRefs() [][]byte
	HavePath(msg Message, msgbyrounds []*Round, targetmsground *Round) (bool, error)
	GetRN() int
	GetHash() []byte
	GetSource() []byte
	GetTimestamp() int64
	GetPlainMsgs() [][]byte
	VerifyFields(*Node) error
	AfterAttach(*Node) error
	Commit(*Node) error
}

// cross-shard msg
type CShardMsg struct {
	SourceShard int
	TargetShard int
	AccStateMap ttype.Payload

	CShardMsgId int
	MsgHash     []byte

	ThresSig []byte
	Source   []byte
}

var fmsg FRoundMsg
var lmsg LRoundMsg
var bmsg BasicMsg
var tmsg ThresSigMsg
var cmsg CShardMsg
var tx ttype.Transaction
var ReflectedTypesMap = map[uint8]reflect.Type{
	FMsgTag: reflect.TypeOf(fmsg),
	LMsgTag: reflect.TypeOf(lmsg),
	BMsgTag: reflect.TypeOf(bmsg),
	TMsgTag: reflect.TypeOf(tmsg),
	CMsgTag: reflect.TypeOf(cmsg),
	TxTag:   reflect.TypeOf(tx),
}
