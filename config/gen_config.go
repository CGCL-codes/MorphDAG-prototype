package config

import (
	"encoding/hex"
	"fmt"

	"github.com/PlainDAG/go-PlainDAG/p2p"
	crypto "github.com/libp2p/go-libp2p/core/crypto"

	// "github.com/seafooler/sign_tools"
	sign_tools "github.com/PlainDAG/go-PlainDAG/sign"
	"github.com/spf13/viper"
)

func Gen_keys(port int) {

	privateKey, _, _ := crypto.GenerateKeyPair(0, 2048)
	// create host
	h := p2p.MakeHost(port, privateKey)
	Pubkey := h.ID().Pretty() // pubkey *
	privateKeyString, _ := crypto.MarshalPrivateKey(privateKey)
	Prvkey := crypto.ConfigEncodeKey(privateKeyString) // prikey *
	fmt.Println(Pubkey)
	fmt.Println(Prvkey)
	h.Close()
}

func Gen_config(shardNum int, shardSize int, simLatency float64) {

	// viperRead := viper.New()

	// // for environment variables
	// viperRead.SetEnvPrefix("")
	// viperRead.AutomaticEnv()
	// replacer := strings.NewReplacer(".", "_")
	// viperRead.SetEnvKeyReplacer(replacer)

	// viperRead.SetConfigName("configs")
	// //viperRead.SetConfigName("config_ecs")
	// viperRead.AddConfigPath("./config")

	// err := viperRead.ReadInConfig()
	// if err != nil {
	// 	panic(err)
	// }
	beginPort := 9000 // for local benchmark
	shareAsStrings, tsPubkey := GenTSKeys(shardSize)
	// beginPort := viperRead.GetInt("begin_port")
	// simLatency := viperRead.GetFloat64("simlatency")

	// fmt.Println(shardNum, shardSize, beginPort, simLatency)

	// fmt.Println(tsPubkey)
	// //idipmap
	// idIPMapInterface := viperRead.GetStringMap("host_ip")
	// hostIPMap := make(map[int]string, len(idIPMapInterface))
	// for idString, ipInterface := range idIPMapInterface {
	// 	id, err := strconv.Atoi(idString)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	if ip, ok := ipInterface.(string); ok {
	// 		hostIPMap[id] = ip
	// 	} else {
	// 		panic("host_ip in the config file cannot be decoded correctly")
	// 	}
	// }
	// fmt.Println(hostIPMap)

	idIpMap := make(map[string]string, shardNum*shardSize)
	idPubkeymap := make(map[string]string, shardNum*shardSize)
	idPrvkeyMap := make(map[string]string, shardNum*shardSize)
	idPortMap := make(map[string]int, shardNum*shardSize)
	idNameMap := make(map[string]string, shardNum*shardSize)

	// generate config file for each shard
	for shardid := 0; shardid < shardNum+1; shardid++ {
		// generate private keys and public keys for each node
		for nodeid := 0; nodeid < shardSize+1; nodeid++ {
			if (shardid != shardNum && nodeid == shardSize) || (shardid == shardNum && nodeid != shardSize) {
				continue
			}
			id := fmt.Sprintf("%d-%d", shardid, nodeid)
			privateKey, _, _ := crypto.GenerateKeyPair(0, 2048)

			// create host
			// idIpMap[id] = hostIPMap[nodeid]
			idIpMap[id] = "0.0.0.0"
			h := p2p.MakeHost(beginPort, privateKey)
			idPubkeymap[id] = h.ID().Pretty() // pubkey *
			privateKeyString, _ := crypto.MarshalPrivateKey(privateKey)
			idPrvkeyMap[id] = crypto.ConfigEncodeKey(privateKeyString) // prikey *
			idPortMap[id] = beginPort                                  // port
			beginPort++
			idNameMap[id] = "node-" + id // name
			h.Close()
		}
	}

	// fmt.Println(idPubkeymap)
	// fmt.Println(idPortMap)

	//generate config file
	for shardid := 0; shardid < shardNum+1; shardid++ {
		for nodeid := 0; nodeid < shardSize+1; nodeid++ {
			if (shardid != shardNum && nodeid == shardSize) || (shardid == shardNum && nodeid != shardSize) {
				continue
			}
			id := fmt.Sprintf("%d-%d", shardid, nodeid)
			//generate private key and public key
			nodename := fmt.Sprintf("node-%d-%d", shardid, nodeid)
			viperWrite := viper.New()
			viperWrite.SetConfigFile(fmt.Sprintf("./config/%s.yaml", nodename))

			viperWrite.Set("id", id)
			viperWrite.Set("nodename", nodename)
			viperWrite.Set("ip", idIpMap[id])
			viperWrite.Set("p2p_port", idPortMap[id])
			viperWrite.Set("private_key", idPrvkeyMap[id])
			viperWrite.Set("public_key", idPubkeymap[id])

			// info of all nodes
			viperWrite.Set("shard_number", shardNum)
			viperWrite.Set("shard_size", shardSize)
			viperWrite.Set("id_name", idNameMap)
			viperWrite.Set("id_ip", idIpMap)
			viperWrite.Set("id_p2p_port", idPortMap)
			viperWrite.Set("id_public_key", idPubkeymap)

			viperWrite.Set("tspubkey", tsPubkey)
			viperWrite.Set("tsshare", shareAsStrings[nodeid])
			viperWrite.Set("simlatency", simLatency)

			err := viperWrite.WriteConfig()
			if err != nil {
				panic(err)
			}

		}
	}
}

func GenTSKeys(nodeNumber int) ([]string, string) {
	// create the threshold signature keys
	// numT := nodeNumber - nodeNumber/5
	numT := nodeNumber/5 + 1
	shares, pubPoly := sign_tools.GenTSKeys(numT, nodeNumber)
	var shareAsStrings []string
	var shareAsString string
	var err error
	for _, share := range shares {
		shareAsBytes, err := sign_tools.EncodeTSPartialKey(share)
		if err != nil {
			panic("encode the share")
		}
		shareAsString = hex.EncodeToString(shareAsBytes)
		shareAsStrings = append(shareAsStrings, shareAsString)
		fmt.Println(shareAsString)
	}
	shareAsStrings = append(shareAsStrings, shareAsString) // client
	fmt.Println(shareAsString)
	tsPubKeyAsBytes, err := sign_tools.EncodeTSPublicKey(pubPoly)
	if err != nil {
		panic("encode the share")
	}
	tsPubKeyAsString := hex.EncodeToString(tsPubKeyAsBytes)
	fmt.Println(tsPubKeyAsString)
	return shareAsStrings, tsPubKeyAsString
}

// func Gen_config() {
// 	viperRead := viper.New()

// 	// for environment variables
// 	viperRead.SetEnvPrefix("")
// 	viperRead.AutomaticEnv()
// 	replacer := strings.NewReplacer(".", "_")
// 	viperRead.SetEnvKeyReplacer(replacer)

// 	viperRead.SetConfigName("config")
// 	viperRead.AddConfigPath("./config")

// 	err := viperRead.ReadInConfig()
// 	if err != nil {
// 		panic(err)
// 	}
// 	idNameMapInterface := viperRead.GetStringMap("id_name")
// 	nodeNumber := len(idNameMapInterface)
// 	idNameMap := make(map[int]string, nodeNumber)
// 	for idString, nodenameInterface := range idNameMapInterface {
// 		if nodename, ok := nodenameInterface.(string); ok {
// 			id, err := strconv.Atoi(idString)
// 			if err != nil {
// 				panic(err)
// 			}
// 			idNameMap[id] = nodename
// 		} else {
// 			panic("id_name in the config file cannot be decoded correctly")
// 		}
// 	}
// 	fmt.Println("idNameMap: ", idNameMap)

// 	idP2PPortMapInterface := viperRead.GetStringMap("id_p2p_port")
// 	if nodeNumber != len(idP2PPortMapInterface) {
// 		panic("id_p2p_port does not match with id_name")
// 	}
// 	idP2PPortMap := make(map[int]int, nodeNumber)
// 	for idString, portInterface := range idP2PPortMapInterface {
// 		id, err := strconv.Atoi(idString)
// 		if err != nil {
// 			panic(err)
// 		}
// 		if port, ok := portInterface.(int); ok {
// 			idP2PPortMap[id] = port
// 		} else {
// 			panic("id_p2p_port in the config file cannot be decoded correctly")
// 		}
// 	}
// 	fmt.Println("idP2PPortMap: ", idP2PPortMap)

// 	//idipmap
// 	idIPMapInterface := viperRead.GetStringMap("id_ip")
// 	if nodeNumber != len(idIPMapInterface) {
// 		panic("id_ip does not match with id_name")
// 	}
// 	idIPMap := make(map[int]string, nodeNumber)
// 	for idString, ipInterface := range idIPMapInterface {
// 		id, err := strconv.Atoi(idString)
// 		if err != nil {
// 			panic(err)
// 		}
// 		if ip, ok := ipInterface.(string); ok {
// 			idIPMap[id] = ip
// 		} else {
// 			panic("id_ip in the config file cannot be decoded correctly")
// 		}
// 	}

// 	// generate private keys and public keys for each node
// 	// generate config file for each node
// 	idPrvkeyMap := make(map[int][]byte, nodeNumber)

// 	//idPubkeyMapHex := make(map[int]string, nodeNumber)
// 	for id, _ := range idNameMap {
// 		privateKey, _, _ := crypto.GenerateKeyPair(0, 2048)
// 		privateKeyString, _ := crypto.MarshalPrivateKey(privateKey)

// 		//publicKeyString, _ := crypto.MarshalPublicKey(publicKey)

// 		idPrvkeyMap[id] = privateKeyString

// 		//idPubkeyMapHex[id] = crypto.ConfigEncodeKey(publicKeyString)

// 		// }
// 	}
// 	for id, nodename := range idNameMap {
// 		//generate private key and public key

// 		//generate config file
// 		viperWrite := viper.New()
// 		viperWrite.SetConfigFile(fmt.Sprintf("%s.yaml", nodename))
// 		viperWrite.Set("id", id)
// 		viperWrite.Set("nodename", nodename)
// 		viperWrite.Set("privkey_sig", crypto.ConfigEncodeKey(idPrvkeyMap[id]))
// 		// viperWrite.Set("pubkey_sig", idPubkeyMapHex[id])
// 		// viperWrite.Set("id_pubkey_sig", idPubkeyMapHex)
// 		viperWrite.Set("p2p_port", idP2PPortMap[id])
// 		viperWrite.Set("ip", idIPMap[id])
// 		viperWrite.Set("id_name", idNameMap)
// 		viperWrite.Set("id_p2p_port", idP2PPortMap)
// 		viperWrite.Set("id_ip", idIPMap)
// 		viperWrite.Set("node_number", nodeNumber)
// 		//viperWrite.SetConfigName(nodename)
// 		//viperWrite.AddConfigPath("./")
// 		err := viperWrite.WriteConfig()
// 		if err != nil {
// 			panic(err)
// 		}
// 	}

// }
