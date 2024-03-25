package core

import (
	"log"
	"sort"
	"strconv"
	"time"

	"github.com/PlainDAG/go-PlainDAG/core/ttype"
	"github.com/PlainDAG/go-PlainDAG/utils"
)

type ShardM struct {
	ExecutedEpoches int
	// AccToShardMap   map[string]int

	TemAccs []map[string]struct{}

	n *Node
}

func NewShardM(n *Node) *ShardM {
	temAccs := make([]map[string]struct{}, n.scfg.ShardNum)
	for shardid := 0; shardid < n.scfg.ShardNum; shardid++ {
		temAccs[shardid] = map[string]struct{}{}
	}
	return &ShardM{
		ExecutedEpoches: 0,
		n:               n,
		TemAccs:         temAccs,
	}
}

func (s *ShardM) MarkTemAccs(txs map[string][]*ttype.Transaction) {
	start := time.Now()
	for blkNum := range txs {
		txSets := txs[blkNum]
		for _, tx := range txSets {
			payload := tx.Data()
			for addr := range payload {
				shardid, _ := strconv.Atoi(addr[len(addr)-2:])
				if shardid != s.n.cfg.ShardId {
					// log.Println("crate tem acc: ", addr, "for shard: ", shardid)
					if s.TemAccs[shardid] == nil {
						s.TemAccs[shardid] = make(map[string]struct{})
					}
					s.TemAccs[shardid][addr] = struct{}{}
				}
			}
		}
	}
	total := 0
	for shardid := 0; shardid < s.n.scfg.ShardNum; shardid++ {
		total += len(s.TemAccs[shardid])
	}
	duration := time.Since(start)
	log.Println("Time of marking ", total, " tem acc is ", duration)
}

func (s *ShardM) GetShard(acc string) int {
	byteToInt, _ := strconv.Atoi(acc[len(acc)-2:])
	return byteToInt
	// shardid := binary.BigEndian.Uint64(acc) % uint64(s.n.scfg.ShardNum)
	// return int(shardid)
}

func (s *ShardM) ClearTemAccs() {
	s.ExecutedEpoches++
	// log.Println("ExecutedEpoches: ", s.ExecutedEpoches)

	if s.ExecutedEpoches%AccAggInterval == 0 {
		start := time.Now()

		// get the state of all tem acc
		temAccState := make([]map[string]int64, s.n.scfg.ShardNum)
		for shardid, temAccs := range s.TemAccs {
			// target shard: shardid
			temAccState[shardid] = make(map[string]int64)
			for temAcc := range temAccs {
				//get state of tem acc
				bal, err := s.n.stateDB.GetBalance([]byte(temAcc))
				if err != nil {
					panic(err)
				}
				temAccState[shardid][temAcc] = bal
			}
		}

		// generate cross-shard acc agg msg
		go s.genCSAccAggMsg(&temAccState)

		// clear tem acc
		deletedTemAccNums := s.n.stateDB.RemoveTemAcc(&s.TemAccs)
		s.TemAccs = make([]map[string]struct{}, s.n.scfg.ShardNum)
		for shardid := 0; shardid < s.n.scfg.ShardNum; shardid++ {
			s.TemAccs[shardid] = map[string]struct{}{}
		}
		duration := time.Since(start)
		log.Println("Time of generating cross-shard msg for ", deletedTemAccNums, "tem accs is ", duration)
	}
}

// func (s *ShardM) ClearTemAccs() {
// 	s.ExecutedEpoches++
// 	// log.Println("ExecutedEpoches: ", s.ExecutedEpoches)

// 	if s.ExecutedEpoches%AccAggInterval == 0 {
// 		start := time.Now()
// 		// send a cross-shard msg

// 		for shardid, temAccs := range s.TemAccs {
// 			// target shard: shardid
// 			if len(temAccs) == 0 {
// 				continue
// 			}

//				// get the state of tem accs of shardid and generate temAccState
//				temAccState := make(map[string][]*ttype.RWSet)
//				// log.Println("gen ", len(temAccs), " tem acc for shard: ", shardid)
//				id := 0
//				var temAccList []string
//				for temAcc := range temAccs {
//					temAccList = append(temAccList, temAcc)
//				}
//				sort.Strings(temAccList)
//				for _, temAcc := range temAccList {
//					//get state of tem acc
//					bal, err := s.n.stateDB.GetBalance([]byte(temAcc))
//					if err != nil {
//						panic(err)
//					}
//					rw := &ttype.RWSet{Label: "iw", Addr: []byte("0"), Value: bal}
//					temAccState[temAcc] = append(temAccState[temAcc], rw)
//					if len(temAccState) == MAXTemAcc {
//						// create and send
//						go s.genAndSend(shardid, temAccState, id)
//						temAccState = make(map[string][]*ttype.RWSet)
//						id++
//					}
//				}
//				if len(temAccState) != 0 {
//					go s.genAndSend(shardid, temAccState, id)
//				}
//			}
//			// clear tem acc
//			deletedTemAccNums := s.n.stateDB.RemoveTemAcc(&s.TemAccs)
//			s.TemAccs = make([]map[string]struct{}, s.n.scfg.ShardNum)
//			for shardid := 0; shardid < s.n.scfg.ShardNum; shardid++ {
//				s.TemAccs[shardid] = map[string]struct{}{}
//			}
//			duration := time.Since(start)
//			log.Println("Time of generating cross-shard msg for ", deletedTemAccNums, "tem accs is ", duration)
//		}
//	}

func (s *ShardM) genCSAccAggMsg(temAccState *[]map[string]int64) {
	for shardid, temAccs := range *temAccState {
		// target shard: shardid
		if len(temAccs) == 0 {
			continue
		}
		// generate payload
		id := 0
		payload := make(map[string][]*ttype.RWSet)
		var temAccList []string
		for temAcc := range temAccs {
			temAccList = append(temAccList, temAcc)
		}
		sort.Strings(temAccList)
		for _, temAcc := range temAccList {
			rw := &ttype.RWSet{Label: "iw", Addr: []byte("0"), Value: temAccs[temAcc]}
			payload[temAcc] = append(payload[temAcc], rw)
			if len(payload) == MAXTemAcc {
				// create and send
				go s.genAndSend(shardid, payload, id)
				payload = make(map[string][]*ttype.RWSet)
				id++
			}
		}
		if len(payload) != 0 {
			go s.genAndSend(shardid, payload, id)
		}
	}
}

func (s *ShardM) genAndSend(targetShard int, temAccState map[string][]*ttype.RWSet, id int) {
	// start := time.Now()
	CShardMsgId := s.ExecutedEpoches*1000 + id

	csMsg, err := NewCShardMsg(s.n.cfg.ShardId, targetShard, temAccState, CShardMsgId, s.n.cfg.Pubkeyraw, s.n.cfg.TSPrvKey)
	if err != nil {
		panic(err)
	}

	// sign and send
	msgbytes, sig, err := utils.MarshalAndSign(csMsg, s.n.cfg.Prvkey)
	if err != nil {
		panic(err)
	}
	// log.Println("generate cross-shard msg for target shard: ", targetShard, " in ExecutedEpoches", CShardMsgId, "tem accs: ", len(temAccState), "size: ", len(msgbytes))
	// log.Println("cross-shard msg: ", csMsg)

	s.n.SendMsgToShard(targetShard, CShardMsgId, CMsgTag, csMsg.GetHash(), msgbytes, sig)
	// duration := time.Since(start)
	// log.Println("Time of genAndSend is ", duration)
}
