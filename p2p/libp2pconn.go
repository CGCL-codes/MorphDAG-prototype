package p2p

import (
	"bufio"
	"context"
	"encoding/json"
	"sync"
	"time"

	"fmt"
	"log"
	"reflect"

	"github.com/PlainDAG/go-PlainDAG/utils"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/libp2p/go-libp2p"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/multiformats/go-multiaddr"
)

type MsgWithSigandSrc struct {
	Msg      interface{}
	Sig      []byte
	Msgbytes []byte
}

type NetworkDealer struct {
	connPool     map[string]*conn
	connPoolLock sync.Mutex
	msgch        chan MsgWithSigandSrc
	txch         chan MsgWithSigandSrc
	H            host.Host

	// ctx               context.Context
	// ctxCancel         context.CancelFunc
	// ctxLock           sync.RWMutex

	reflectedTypesMap map[uint8]reflect.Type

	BroadcastSyncLock sync.Mutex
	latencyrand       *utils.PoissonGenerator

	// multiaddr -> connection
	// func PackMultiaddr()
	// fmt.Sprintf("/ip4/%s/tcp/%v/p2p/%s", addr, port, pubKey)
	otherShardConnPool          map[string]*conn
	otherShardConnPoolLock      sync.Mutex
	otherShardBroadcastSyncLock sync.Mutex
}

type conn struct {
	dest   string
	w      *bufio.Writer
	encode *codec.Encoder
}

/*
write me some code to serialize the struct NetworkDealer
*/

func MakeHost(port int, prvKey crypto.PrivKey) host.Host {
	// Make the host that will handle the network requests
	sourceMultiAddr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port))
	host, err := libp2p.New(libp2p.Identity(prvKey), libp2p.ListenAddrs(sourceMultiAddr))
	if err != nil {
		log.Fatal(err)
	}
	return host

}

func (n *NetworkDealer) Listen() {
	listenStream := func(s network.Stream) {
		// log.Println("Received a connection from ", s.Conn().RemotePeer().String())

		r := bufio.NewReader(s)
		//peerId := s.Conn().RemotePeer()
		//pubkey := n.H.Peerstore().PubKey(peerId)
		// if err != nil {
		// 	log.Println("error extracting public key: ", err)
		// }
		n.HandleConn(r)

	}
	n.H.SetStreamHandler(protocol.ID("PlainDAG"), listenStream)

}

func (n *NetworkDealer) HandleConn(r *bufio.Reader) {
	for {

		rpcType, err := r.ReadByte()
		dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
		if err != nil {
			log.Println("error reading byte: ", err)
		}
		_, ok := n.reflectedTypesMap[rpcType]

		if !ok {
			log.Panicln("unknown rpc type: ", rpcType)
		}

		// knowing the type of the struct, construct it with a known byte array

		// var msgBody interface{}
		// var bytearray []byte
		// if err := dec.Decode(&bytearray); err != nil {
		// 	log.Println("error decoding msg: ", err)
		// }
		// if rpcType == core.TestMsgTag {
		// 	var msg core.TestMsg
		// 	json.Unmarshal(bytearray, &msg)
		// 	msgBody = msg
		// }
		//fmt.Println("received rpc type: ", rpcType)

		var sig []byte
		if err := dec.Decode(&sig); err != nil {
			log.Println("error decoding sig: ", err)
		}

		msgBody := reflect.New(n.reflectedTypesMap[rpcType]).Interface()

		msgbytes := []byte{}
		if err := dec.Decode(&msgbytes); err != nil {
			log.Println("error decoding msg: ", err)
		}
		// fmt.Println(msgbytes)

		json.Unmarshal(msgbytes, &msgBody)

		// var msgBodyBytes []byte
		// if err := dec.Decode(&msgBodyBytes); err != nil {
		// 	log.Println("error decoding msgBodyBytes: ", err)
		// }

		// json.Unmarshal(msgBodyBytes, &msgBody)

		// var sigok bool
		// sigok, err = sourcepubkey.Verify(msgbytes, sig)
		// if err != nil {
		// 	panic(err)
		// }
		// if !sigok {
		// 	log.Println("signature verification failed")
		// 	return
		// }
		MsgWithSigandSrc := MsgWithSigandSrc{
			Msg:      msgBody,
			Sig:      sig,
			Msgbytes: msgbytes,
		}

		if rpcType == 4 {
			select {
			case n.txch <- MsgWithSigandSrc:
				//knowing the type of the struct, how to construct it with a known byte array?
				// msg := reflect.New(n.reflectedTypesMap[rpcType]).Interface()
				// if err := dec.Decode(msg); err != nil {
				// 	log.Println("error decoding msg: ", err)
				// }
				// var sig []byte
			}
		} else {
			select {
			case n.msgch <- MsgWithSigandSrc:
			}
		}
	}
}

func (n *NetworkDealer) Connect(port int, addr string, pubKey string) (*bufio.Writer, error) {
	return n.ConnectWithMultiaddr(PackMultiaddr(port, addr, pubKey))
}

func (n *NetworkDealer) ConnectWithMultiaddr(multi string) (*bufio.Writer, error) {

	// log.Println("Connecting to ", multi)
	maddr, err := multiaddr.NewMultiaddr(multi)
	if err != nil {
		return nil, err
	}

	// Extract the peer ID from the multiaddr.
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return nil, err
	}
	n.H.Peerstore().AddAddr(info.ID, maddr, peerstore.PermanentAddrTTL)
	s, err := n.H.NewStream(context.Background(), info.ID, protocol.ID("PlainDAG"))
	if err != nil {
		return nil, err
	}
	return bufio.NewWriter(s), nil
}

func PackMultiaddr(port int, addr string, pubKey string) string {
	return fmt.Sprintf("/ip4/%s/tcp/%v/p2p/%s", addr, port, pubKey)
}

func (n *NetworkDealer) SendMsg(messagetype uint8, msg interface{}, sig []byte, dest string, reconn bool) error {
	// log.Println("SendMsg to ", dest)
	n.connPoolLock.Lock()
	c, ok := n.connPool[dest]
	if !ok || reconn {
		log.Println("[SendMsgToAnotherShard] reconnect to: ", dest)

		w, err := n.ConnectWithMultiaddr(dest)
		if err != nil {
			return err
		}

		c = &conn{
			dest:   dest,
			w:      w,
			encode: codec.NewEncoder(w, &codec.MsgpackHandle{}),
		}
		n.connPool[dest] = c
		n.connPoolLock.Unlock()
	} else {
		n.connPoolLock.Unlock()
	}

	if err := c.w.WriteByte(messagetype); err != nil {
		log.Println("writebyte error", err, " ", dest)
		return err
	}

	if err := c.encode.Encode(sig); err != nil {
		log.Println("encode sig error", err, " ", dest)
		return err
	}

	if err := c.encode.Encode(msg); err != nil {
		log.Println("encode msg error", err, " ", dest)
		return err
	}
	if err := c.w.Flush(); err != nil {
		log.Println("flush error", err, " ", dest)
		return err
	}
	// log.Println("SendMsg success to ", dest)

	return nil
}

func NewnetworkDealer(port int, prvkey crypto.PrivKey, reflectedTypesMap map[uint8]reflect.Type) (*NetworkDealer, error) {

	h := MakeHost(port, prvkey)
	n := &NetworkDealer{
		connPool:           make(map[string]*conn),
		msgch:              make(chan MsgWithSigandSrc, 10000),
		txch:               make(chan MsgWithSigandSrc, 20000),
		H:                  h,
		latencyrand:        utils.NewPoissonGenerator(time.Now().Unix()),
		reflectedTypesMap:  reflectedTypesMap,
		otherShardConnPool: make(map[string]*conn),
	}

	return n, nil
}

func (n *NetworkDealer) ExtractMsg() chan MsgWithSigandSrc {
	return n.msgch
}

/*
send a msg to a dest in another shard
@param dest is a multiAddr like "/ip4/0.0.0.0/tcp/9000/p2p/QmUSiw4JrFmqf4mAPozaSas2mAY8vxBow3gzyZtXdPPEqV"
*/
func (n *NetworkDealer) SendMsgToAnotherShard(messagetype uint8, msg interface{}, sig []byte, dest string, reconn bool) error {
	// log.Println("SendMsgToAnotherShard(): ", dest)
	n.otherShardConnPoolLock.Lock()
	c, ok := n.otherShardConnPool[dest]
	if !ok || reconn {
		// log.Println("[SendMsgToAnotherShard] reconnect to: ", dest)
		w, err := n.ConnectWithMultiaddr(dest)
		if err != nil {
			return err
		}

		c = &conn{
			dest:   dest,
			w:      w,
			encode: codec.NewEncoder(w, &codec.MsgpackHandle{}),
		}
		n.otherShardConnPool[dest] = c
		n.otherShardConnPoolLock.Unlock()
	} else {
		n.otherShardConnPoolLock.Unlock()
	}
	// log.Println("otherShardConnPool", n.otherShardConnPool)

	if err := c.w.WriteByte(messagetype); err != nil {
		// log.Println("writebyte error", err, " ", dest)
		return err
	}

	if err := c.encode.Encode(sig); err != nil {
		// log.Println("encode sig error", err, " ", dest)
		return err
	}

	if err := c.encode.Encode(msg); err != nil {
		// log.Println("encode msg error", err, " ", dest)
		return err
	}
	if err := c.w.Flush(); err != nil {
		// log.Println("flush error", err, " ", dest)
		return err
	}
	// log.Println("SendMsgToAnotherShard(): ", dest, " success", "type: ", messagetype)

	return nil
}

func (n *NetworkDealer) ExtractTx() chan MsgWithSigandSrc {
	return n.txch
}
