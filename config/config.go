package config

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/PlainDAG/go-PlainDAG/sign"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/spf13/viper"
	"go.dedis.ch/kyber/v3/share"
)

type Config struct {
	Ipaddress string
	Port      int
	Id        int
	ShardId   int
	Nodename  string
	IdnameMap map[int]string

	Prvkey      crypto.PrivKey
	Pubkey      crypto.PubKey
	Pubkeyraw   []byte
	IdPubkeymap map[int]string
	// this map references the id.pretty() to id
	PubkeyIdMap map[string]int
	IdportMap   map[int]int
	IdaddrMap   map[int]string // ip

	//the first  map is to store the public key of of each node, the key string is the string(pubkey) field

	StringpubkeyMap map[string]crypto.PubKey
	// the second map is to store the index of each node and reference the public key to id. the key string is the string(pubkey) field
	StringIdMap map[string]int

	TSPubKey *share.PubPoly
	TSPrvKey *share.PriShare

	Simlatency float64
}

type ShardConfig struct {
	ShardNum       int
	ShardSize      int
	AllIdNameMap   map[string]string
	AllIdIpMap     map[string]string
	AllIdPortMap   map[string]int
	AllIdPubkeymap map[string]string
	AllPubkeyIdMap map[string]string

	AllStringpubkeyMap map[string]crypto.PubKey
}

func Loadconfig(shardid int, nodeid int, config_path string) (*Config, *ShardConfig) {

	filepath := fmt.Sprintf("node-%d-%d", shardid, nodeid)
	fmt.Println("load config with filfpath: " + filepath)

	// // find the number index in string
	// var fileindex int
	// for i := 0; i < len(filepath); i++ {
	// 	if filepath[i] >= '0' && filepath[i] <= '9' {

	// 		//convert byte to int
	// 		fileindex, _ = strconv.Atoi(string(filepath[i]))
	// 		break
	// 	}
	// }

	viperRead := viper.New()

	// for environment variables
	viperRead.SetEnvPrefix("")
	viperRead.AutomaticEnv()
	replacer := strings.NewReplacer(".", "_")
	viperRead.SetEnvKeyReplacer(replacer)

	viperRead.SetConfigName(filepath)
	fmt.Println(filepath)
	viperRead.AddConfigPath(config_path)

	err := viperRead.ReadInConfig()
	if err != nil {
		panic(err)
	}
	var tmpShardId, tmpNodeId int
	// id_name_map
	idNameMapInterface := viperRead.GetStringMap("id_name")
	nodeNumber := len(idNameMapInterface)
	allIdNameMap := make(map[string]string, nodeNumber)
	idNameMap := make(map[int]string, nodeNumber)
	for idString, nodenameInterface := range idNameMapInterface {
		if nodename, ok := nodenameInterface.(string); ok {
			_, err := fmt.Sscanf(idString, "%d-%d", &tmpShardId, &tmpNodeId)
			// id, err := strconv.Atoi(idString)
			if err != nil {
				panic(err)
			}
			allIdNameMap[idString] = nodename
			if tmpShardId == shardid {
				idNameMap[tmpNodeId] = nodename
			}
		} else {
			panic("id_name in the config file cannot be decoded correctly")
		}
	}
	// id_p2p_port_map
	idP2PPortMapInterface := viperRead.GetStringMap("id_p2p_port")
	if nodeNumber != len(idP2PPortMapInterface) {
		panic("id_p2p_port does not match with id_name")
	}
	allIdP2PPortMap := make(map[string]int, nodeNumber)
	idP2PPortMap := make(map[int]int, nodeNumber)
	for idString, portInterface := range idP2PPortMapInterface {
		_, err := fmt.Sscanf(idString, "%d-%d", &tmpShardId, &tmpNodeId)
		if err != nil {
			panic(err)
		}
		if port, ok := portInterface.(int); ok {
			allIdP2PPortMap[idString] = port
			if tmpShardId == shardid {
				idP2PPortMap[tmpNodeId] = port
			}

		} else {
			panic("id_p2p_port in the config file cannot be decoded correctly")
		}
	}
	// id_ip_map
	_ip := viperRead.GetString("ip")
	idIPMapInterface := viperRead.GetStringMap("id_ip")
	if nodeNumber != len(idIPMapInterface) {
		panic("id_ip does not match with id_name")
	}
	allIdIPMap := make(map[string]string, nodeNumber)
	idIPMap := make(map[int]string, nodeNumber)
	for idString, ipInterface := range idIPMapInterface {
		_, err := fmt.Sscanf(idString, "%d-%d", &tmpShardId, &tmpNodeId)
		if err != nil {
			panic(err)
		}
		if ip, ok := ipInterface.(string); ok {
			//
			if ip == _ip {
				ip = "0.0.0.0"
			}
			allIdIPMap[idString] = ip
			if tmpShardId == shardid {
				idIPMap[tmpNodeId] = ip
			}
		} else {
			panic("id_ip in the config file cannot be decoded correctly")
		}
	}

	// id_public_key_map
	// convert the map into map[int]crypto.PubKey
	pubkeyothersmap := viperRead.GetStringMap("id_public_key")
	// convert the bytes into private key and public key
	allPubkeysmap := make(map[string]string, nodeNumber)
	allPubkeyIdMap := make(map[string]string, nodeNumber)
	pubkeysmap := make(map[int]string, nodeNumber)
	for idString, pubkeyothersInterface := range pubkeyothersmap {
		if pubkeyothers, ok := pubkeyothersInterface.(string); ok {
			_, err := fmt.Sscanf(idString, "%d-%d", &tmpShardId, &tmpNodeId)
			if err != nil {
				panic(err)
			}

			allPubkeysmap[idString] = pubkeyothers
			allPubkeyIdMap[pubkeyothers] = idString
			if tmpShardId == shardid {
				pubkeysmap[tmpNodeId] = pubkeyothers
			}
		} else {
			panic("public_key_others in the config file cannot be decoded correctly")
		}
	}
	pubkeyidmap := make(map[string]int, nodeNumber)
	for id, pubkeyothers := range pubkeysmap {
		pubkeyidmap[pubkeyothers] = id
	}

	// extract private key and public key and pubkeysmap using config
	privkey := viperRead.GetString("private_key")
	// convert the strings obove into bytes
	privkeybytes, err := crypto.ConfigDecodeKey(privkey)

	if err != nil {
		panic(err)
	}
	// fmt.Println(privkey)
	privkeyobj, err := crypto.UnmarshalPrivateKey(privkeybytes)
	if err != nil {
		panic(err)
	}

	// tspubkey
	tsPubKeyAsString := viperRead.GetString("tspubkey")
	// log.Println("tsPubKeyAsString: ", tsPubKeyAsString)
	tsPubKeyAsBytes, err := hex.DecodeString(tsPubKeyAsString)
	if err != nil {
		panic(err)
	}
	tsPubKey, err := sign.DecodeTSPublicKey(tsPubKeyAsBytes)
	if err != nil {
		panic(err)
	}
	// tsshare
	tsShareAsString := viperRead.GetString("tsshare")
	// log.Println("tsShareAsString: ", tsShareAsString)
	tsShareAsBytes, err := hex.DecodeString(tsShareAsString)
	if err != nil {
		panic(err)
	}
	tsShareKey, err := sign.DecodeTSPartialKey(tsShareAsBytes)
	if err != nil {
		panic(err)
	}

	// simlatency
	simlatency := viperRead.GetFloat64("simlatency")

	// shardNum
	shardNum := viperRead.GetInt("shard_number")
	shardSize := viperRead.GetInt("shard_size")

	return &Config{

			Id:        nodeid,
			ShardId:   shardid,
			Nodename:  idNameMap[nodeid],
			Ipaddress: idIPMap[nodeid],
			Port:      idP2PPortMap[nodeid],
			Prvkey:    privkeyobj,

			IdnameMap: idNameMap,
			IdaddrMap: idIPMap,
			IdportMap: idP2PPortMap,

			IdPubkeymap: pubkeysmap,
			PubkeyIdMap: pubkeyidmap,

			TSPubKey:   tsPubKey,
			TSPrvKey:   tsShareKey,
			Simlatency: simlatency,
		},

		&ShardConfig{
			ShardNum:       shardNum,
			ShardSize:      shardSize,
			AllIdNameMap:   allIdNameMap,
			AllIdIpMap:     allIdIPMap,
			AllIdPortMap:   allIdP2PPortMap,
			AllIdPubkeymap: allPubkeysmap,
			AllPubkeyIdMap: allPubkeyIdMap,
		}
}
