package p2p

import (
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/libp2p/go-libp2p/core/crypto"
)

func Startpeer(port int, prvkey crypto.PrivKey, reflectedTypesMap map[uint8]reflect.Type) (*NetworkDealer, error) {
	n, err := NewnetworkDealer(port, prvkey, reflectedTypesMap)
	if err != nil {
		return nil, err
	}
	n.Listen()
	return n, nil

}

// func (n *NetworkDealer) Connectpeers(peerid int, idaddrmap map[int]string, idportmap map[int]int, pubstringsmap map[int]string) error {

// 	if len(idaddrmap) == 0 { // client
// 		return nil
// 	}
// 	wg := sync.WaitGroup{}
// 	wg.Add(len(idaddrmap) - 1)
// 	log.Println("connect to ", len(idaddrmap)-1, "peers in local shard")
// 	for id, addr := range idaddrmap {
// 		if id != peerid {
// 			port := idportmap[id]
// 			addr := addr
// 			pubstring := pubstringsmap[id]

// 			go func() {
// 				writer, err := n.Connect(port, addr, pubstring)
// 				if err != nil {
// 					for try_times := 3; try_times > 0; try_times-- {
// 						time.Sleep(time.Duration(n.latencyrand.Poisson(20)) * time.Millisecond)
// 						log.Println("reconnect to ", addr, port)
// 						writer, err = n.Connect(port, addr, pubstring)
// 						if err == nil {
// 							break
// 						} else if try_times == 1 {
// 							panic(err)
// 						}
// 					}
// 				}

// 				log.Println("connect to ", addr, port, " success")
// 				destMultiAddr := PackMultiaddr(port, addr, pubstring)
// 				log.Println("dest: ", destMultiAddr)
// 				n.connPoolLock.Lock()
// 				n.connPool[destMultiAddr] = &conn{
// 					w: writer,

// 					dest: destMultiAddr,

// 					encode: codec.NewEncoder(writer, &codec.MsgpackHandle{}),
// 				}
// 				n.connPoolLock.Unlock()
// 				wg.Done()
// 			}()
// 		}
// 	}

// 	wg.Wait()
// 	return nil
// }

// /*
// maintain the connnection with other shards in n.otherShardConnPool
// */
// func (n *NetworkDealer) Connectothershardpeers(shardId int, nodeId int, allIdIpMap map[string]string, allIdPortMap map[string]int, allIdPubstringsMap map[string]string, shardNum int, shardSize int) error {

// 	peers := (shardNum - 1) * shardSize
// 	if nodeId == 6 { // client
// 		peers = shardNum * shardSize
// 	}
// 	wg := sync.WaitGroup{}
// 	wg.Add(peers)
// 	log.Println("connect to ", peers, "peers in other shards")

// 	var tmpShardId, tmpNodeId int
// 	for id, addr := range allIdIpMap {
// 		_, err := fmt.Sscanf(id, "%d-%d", &tmpShardId, &tmpNodeId)
// 		if err != nil {
// 			panic(err)
// 		}
// 		if tmpShardId != shardId {

// 			port := allIdPortMap[id]
// 			addr := addr
// 			pubstring := allIdPubstringsMap[id]
// 			go func() {
// 				writer, err := n.Connect(port, addr, pubstring)
// 				if err != nil {
// 					for try_times := 3; try_times > 0; try_times-- {
// 						time.Sleep(time.Duration(n.latencyrand.Poisson(20)) * time.Millisecond)
// 						log.Println("reconnect to ", port, addr, pubstring)
// 						writer, err = n.Connect(port, addr, pubstring)
// 						if err == nil {
// 							break
// 						} else if try_times == 1 {
// 							panic(err)
// 						}
// 					}
// 				}
// 				// log.Println("[Connectothershardpeers] connect to ", addr, allIdPortMap[id], "in shard ", tmpShardId, " success")
// 				destMultiAddr := PackMultiaddr(port, addr, pubstring)
// 				fmt.Println("dest: ", destMultiAddr)
// 				n.otherShardConnPoolLock.Lock()
// 				n.otherShardConnPool[destMultiAddr] = &conn{
// 					w: writer,

// 					dest: destMultiAddr,

// 					encode: codec.NewEncoder(writer, &codec.MsgpackHandle{}),
// 				}
// 				n.otherShardConnPoolLock.Unlock()
// 				wg.Done()
// 			}()
// 		}
// 	}
// 	log.Println("[Connectothershardpeers] connect to ", peers, " nodes in other shards")
// 	return nil
// }

func (n *NetworkDealer) Connectpeers(peerid int, idaddrmap map[int]string, idportmap map[int]int, pubstringsmap map[int]string) error {
	log.Println("connect to ", len(idaddrmap)-1, "peers in local shard")
	for id, addr := range idaddrmap {
		if id != peerid {

			writer, err := n.Connect(idportmap[id], addr, pubstringsmap[id])
			if err != nil {
				return err
			}
			log.Println("connect to ", addr, idportmap[id], " success")
			destMultiAddr := PackMultiaddr(idportmap[id], addr, pubstringsmap[id])
			log.Println("dest: ", destMultiAddr)
			n.connPoolLock.Lock()
			n.connPool[destMultiAddr] = &conn{
				w: writer,

				dest: destMultiAddr,

				encode: codec.NewEncoder(writer, &codec.MsgpackHandle{}),
			}
			n.connPoolLock.Unlock()
		}
	}
	return nil

}

func (n *NetworkDealer) Connectothershardpeers(shardId int, allIdIpMap map[string]string, allIdPortMap map[string]int, allIdPubstringsMap map[string]string, shardNum int) error {
	var tmpShardId, tmpNodeId int
	totalConnNodes := 0
	for id, addr := range allIdIpMap {
		_, err := fmt.Sscanf(id, "%d-%d", &tmpShardId, &tmpNodeId)
		if err != nil {
			panic(err)
		}
		if tmpShardId != shardId && tmpShardId != shardNum {

			writer, err := n.Connect(allIdPortMap[id], addr, allIdPubstringsMap[id])
			if err != nil {
				return err
			}
			totalConnNodes++
			// log.Println("[Connectothershardpeers] connect to ", addr, allIdPortMap[id], "in shard ", tmpShardId, " success")
			destMultiAddr := PackMultiaddr(allIdPortMap[id], allIdIpMap[id], allIdPubstringsMap[id])
			fmt.Println("dest: ", destMultiAddr)
			n.otherShardConnPoolLock.Lock()
			n.otherShardConnPool[destMultiAddr] = &conn{
				w: writer,

				dest: destMultiAddr,

				encode: codec.NewEncoder(writer, &codec.MsgpackHandle{}),
			}
			n.otherShardConnPoolLock.Unlock()
		}
	}
	log.Println("[Connectothershardpeers] connect to ", totalConnNodes, " nodes in other shards")
	return nil

}

// func (n *NetworkDealer) PrintConnPool() {
// 	for k, v := range n.connPool {
// 		log.Println(k, v)
// 	}

// }

func (n *NetworkDealer) Broadcast(messagetype uint8, msg interface{}, sig []byte, simlatency float64) error {
	// log.Println("Broadcast msg type: ", messagetype)

	n.BroadcastSyncLock.Lock()

	//time.Sleep(time.Duration(n.latencyrand.Poisson(simlatency)) * time.Millisecond)
	var wg sync.WaitGroup
	// fmt.Println("round 2?")
	for _, conn := range n.connPool {
		//fmt.Println(conn)
		wg.Add(1)
		c := conn
		go func() {

			err := n.SendMsg(messagetype, msg, sig, c.dest, false)
			if err != nil {
				// reconnect the node and resend
				success := false
				for i := 2; i > 0; i-- {
					log.Println("fail to send msg to dest: ", c.dest, "try reconnecting and sending again")
					err := n.SendMsg(messagetype, msg, sig, c.dest, true)
					if err == nil {
						success = true
						break
					}
				}
				if !success {
					log.Println("fail to send msg to dest after retry: ", c.dest, "try reconnecting and sending again")
				}
			}
			wg.Done()

		}()
	}
	wg.Wait()
	//fmt.Println("are you finished?")
	n.BroadcastSyncLock.Unlock()
	return nil

}

/*
broad a msg to shard shardid
@param destList a list of destMultiAddr like "/ip4/0.0.0.0/tcp/9000/p2p/QmUSiw4JrFmqf4mAPozaSas2mAY8vxBow3gzyZtXdPPEqV"
*/
func (n *NetworkDealer) BroadcastToAnotherShard(shardid int, destList []string, messagetype uint8, msg interface{}, sig []byte, simlatency float64) error {
	// log.Println("BroadcastToAnotherShard, messagetype: ", messagetype, " destList: ", destList)
	n.otherShardBroadcastSyncLock.Lock()

	//time.Sleep(time.Duration(n.latencyrand.Poisson(simlatency)) * time.Millisecond)
	var wg sync.WaitGroup

	for _, dest := range destList {
		wg.Add(1)
		dest_ := dest
		go func() {

			err := n.SendMsgToAnotherShard(messagetype, msg, sig, dest_, false)
			if err != nil {
				// reconnect the node and resend
				success := false
				for i := 2; i > 0; i-- {
					// log.Println("fail to send cross-shard msg to shard: ", shardid, "dest: ", dest_, "try reconnecting and sending again")
					err := n.SendMsgToAnotherShard(messagetype, msg, sig, dest_, true)
					if err == nil {
						success = true
						break
					}
				}
				if !success {
					log.Println("fail to send cross-shard msg to shard after retry: ", shardid, "dest: ", dest_)
				}
			}

			wg.Done()
		}()
	}

	wg.Wait()
	//fmt.Println("are you finished?")
	n.otherShardBroadcastSyncLock.Unlock()
	return nil

}

// func (n *NetworkDealer) HandleMsgForever() {

// 	for {
// 		select {
// 		case <-n.shutdownCh:
// 			return
// 		case msg := <-n.msgch:
// 			log.Println("receive msg: ", msg.Msg)
// 		}

// 	}
// }

// func printconfig(c *config.P2pconfig) {
// 	log.Println("id: ", c.Id)
// 	log.Println("nodename: ", c.Nodename)
// 	log.Println("port: ", c.Port)
// 	log.Println("addr: ", c.Ipaddress)
// 	log.Println("idportmap: ", c.IdportMap)
// 	log.Println("idaddrmap: ", c.IdaddrMap)
// 	log.Println("pubkeyothersmap: ", c.Pubkeyothersmap)
// 	log.Println("prvkey: ", c.Prvkey)
// 	log.Println("pubkey: ", c.Pubkey)

// }
