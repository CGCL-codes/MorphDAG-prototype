package main

import (
	"flag"
	"time"

	"github.com/PlainDAG/go-PlainDAG/config"
	"github.com/PlainDAG/go-PlainDAG/core"
)

func main() {
	// operation type
	optype := flag.Int("o", 1, "specify the operation type")

	// parameters for generate keys
	port := flag.Int("p", 9000, "port")

	// parameters of generating config for local benchmark
	shardnum := flag.Int("sn", 1, "config shard number for generate config")
	shardsize := flag.Int("ss", 6, "config shard size for generate config")
	simlatency := flag.Float64("sl", 20.0, "config simlatency")

	// parameters of nodes
	shardid := flag.Int("s", 0, "config shardid")
	nodeid := flag.Int("n", 0, "config nodeid")
	configpath := flag.String("f", "config", "config path")
	batchsize := flag.Int("b", 500, "specify the batch size")

	// parameters of client
	cycle := flag.Int("c", 500, "specify the number of tx sending cycles")
	rate := flag.Int("r", 2000, "specify the sending rate per round")

	flag.Parse()

	switch *optype {

	case 0:
		config.Gen_keys(*port)

	case 1:
		n, err := core.StartandConnect(*shardid, *nodeid, *configpath, *batchsize)
		if err != nil {
			panic(err)
		}

		// assume node 6 is a client for sending txs
		if *nodeid == n.GetShardSize() {
			go n.SendTxsForLoop(*cycle, *rate)
		} else {
			go n.HandleTxForever()
			// receive sufficient transactions
			time.Sleep(5 * time.Second)
			go n.SendForever()
			go n.HandleMsgForever()
		}

		select {}

	case 2: // gen config for local benchmark
		config.Gen_config(*shardnum, *shardsize, *simlatency)

	case 3:
		config.GenTSKeys(*shardsize)

	default:

	}
}
