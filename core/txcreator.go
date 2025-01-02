package core

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/PlainDAG/go-PlainDAG/core/ttype"
	"github.com/chinuy/zipf"
)

var selectFunc = []string{"almagate", "updateBalance", "updateSaving", "sendPayment", "writeCheck", "getBalance"}

// CreateMimicWorkload creates a transaction using smallbank benchmark
func CreateMimicWorkload(ratio int, z *zipf.Zipf, shardid int, shardnum int) *ttype.Transaction {
	rand.Seed(time.Now().UnixNano())
	random := rand.Intn(10)

	// read-write ratio
	var function string
	if random <= ratio {
		function = selectFunc[5]
	} else {
		random2 := rand.Intn(5)
		function = selectFunc[random2]
	}

	var duplicated = make(map[string]struct{})
	var addr1, addr2, addr3, addr4 string

	addr1 = strconv.FormatUint(z.Uint64(), 10)
	duplicated[addr1] = struct{}{}

	for {
		addr2 = strconv.FormatUint(z.Uint64(), 10)
		if _, ok := duplicated[addr2]; !ok {
			duplicated[addr2] = struct{}{}
			break
		}
	}

	for {
		addr3 = strconv.FormatUint(z.Uint64(), 10)
		if _, ok := duplicated[addr3]; !ok {
			duplicated[addr3] = struct{}{}
			break
		}
	}

	for {
		addr4 = strconv.FormatUint(z.Uint64(), 10)
		if _, ok := duplicated[addr4]; !ok {
			break
		}
	}

	shardidstr := fmt.Sprintf("%02d", shardid)
	addr1 = convertToGAddr(addr1, shardidstr)
	addr2 = convertToGAddr(addr2, shardidstr)
	addr3 = convertToGAddr(addr3, shardidstr)
	random = rand.Intn(10)
	fmt.Println(random)
	// cross-shard ratio
	if shardnum != 1 && random <= CSRatio {
		// create cross-shard common transfet tx
		fmt.Println("create cross-shard common transfet tx")
		var recvshardid int
		for {
			recvshardid = rand.Intn(shardnum)
			if recvshardid != shardid {
				break
			}
		}
		shardidstr := fmt.Sprintf("%02d", recvshardid)
		addr4 = convertToGAddr(addr4, shardidstr)
		function = "sendPayment"
	} else {
		addr4 = convertToGAddr(addr4, shardidstr)
	}
	// fmt.Println(addr1, addr2, addr3, addr4)
	payload := generatePayload(function, addr1, addr2, addr3, addr4)
	newTx := ttype.NewTransaction(0, []byte("A"), []byte("K"), payload)

	return newTx
}

func convertToGAddr(addr string, shardidStr string) string {
	var buffer bytes.Buffer
	buffer.WriteString(addr)
	buffer.WriteString(shardidStr)
	return buffer.String()
}

// // CreateMimicWorkload creates a transaction using smallbank benchmark
// func CreateMimicWorkload(ratio int, z *zipf.Zipf) *ttype.Transaction {
// 	rand.Seed(time.Now().UnixNano())
// 	random := rand.Intn(10)

// 	// read-write ratio
// 	var function string
// 	if random <= ratio {
// 		function = selectFunc[5]
// 	} else {
// 		random2 := rand.Intn(5)
// 		function = selectFunc[random2]
// 	}

// 	var duplicated = make(map[string]struct{})
// 	var addr1, addr2, addr3, addr4 string

// 	addr1 = strconv.FormatUint(z.Uint64(), 10)
// 	duplicated[addr1] = struct{}{}

// 	for {
// 		addr2 = strconv.FormatUint(z.Uint64(), 10)
// 		if _, ok := duplicated[addr2]; !ok {
// 			duplicated[addr2] = struct{}{}
// 			break
// 		}
// 	}

// 	for {
// 		addr3 = strconv.FormatUint(z.Uint64(), 10)
// 		if _, ok := duplicated[addr3]; !ok {
// 			duplicated[addr3] = struct{}{}
// 			break
// 		}
// 	}

// 	for {
// 		addr4 = strconv.FormatUint(z.Uint64(), 10)
// 		if _, ok := duplicated[addr4]; !ok {
// 			break
// 		}
// 	}

// 	payload := generatePayload(function, addr1, addr2, addr3, addr4)
// 	newTx := ttype.NewTransaction(0, []byte("A"), []byte("K"), payload)

// 	return newTx
// }

func generatePayload(funcName string, addr1, addr2, addr3, addr4 string) ttype.Payload {
	var payload = make(map[string][]*ttype.RWSet)
	switch funcName {
	// addr1 & addr3 --> savingStore, addr2 & addr4 --> checkingStore
	case "almagate":
		rw1 := &ttype.RWSet{Label: "r", Addr: []byte("0")}
		payload[addr1] = append(payload[addr1], rw1)
		rw2 := &ttype.RWSet{Label: "w", Addr: []byte("0"), Value: 1000}
		payload[addr2] = append(payload[addr2], rw2)
		rw3 := &ttype.RWSet{Label: "r", Addr: []byte("0")}
		payload[addr4] = append(payload[addr4], rw3)
		rw4 := &ttype.RWSet{Label: "iw", Addr: []byte("0"), Value: 10}
		payload[addr3] = append(payload[addr3], rw4)
	case "getBalance":
		rw1 := &ttype.RWSet{Label: "r", Addr: []byte("0")}
		payload[addr1] = append(payload[addr1], rw1)
		rw2 := &ttype.RWSet{Label: "r", Addr: []byte("0")}
		payload[addr2] = append(payload[addr2], rw2)
	case "updateBalance":
		rw1 := &ttype.RWSet{Label: "iw", Addr: []byte("0"), Value: 10}
		payload[addr2] = append(payload[addr2], rw1)
	case "updateSaving":
		rw1 := &ttype.RWSet{Label: "iw", Addr: []byte("0"), Value: 10}
		payload[addr1] = append(payload[addr1], rw1)
	case "sendPayment":
		rw1 := &ttype.RWSet{Label: "r", Addr: []byte("0")}
		rw2 := &ttype.RWSet{Label: "w", Addr: []byte("0"), Value: -10}
		payload[addr2] = append(payload[addr2], rw1, rw2)
		rw3 := &ttype.RWSet{Label: "iw", Addr: []byte("0"), Value: 10}
		payload[addr4] = append(payload[addr4], rw3)
	case "writeCheck":
		rw1 := &ttype.RWSet{Label: "r", Addr: []byte("0")}
		payload[addr1] = append(payload[addr1], rw1)
		rw2 := &ttype.RWSet{Label: "iw", Addr: []byte("0"), Value: 5}
		payload[addr2] = append(payload[addr2], rw2)
	default:
		fmt.Println("Invalid inputs")
		return nil
	}
	return payload
}
