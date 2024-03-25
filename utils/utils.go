package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func MarshalAndSign(msg interface{}, prvkey crypto.PrivKey) ([]byte, []byte, error) {
	msgbytes, err := json.Marshal(msg)
	if err != nil {
		return nil, nil, err
	}
	sig, err := prvkey.Sign(msgbytes)
	if err != nil {
		return nil, nil, err
	}

	return msgbytes, sig, nil
}

func VerifySig(m map[string]crypto.PubKey, sig []byte, msgbytes []byte, source []byte) (bool, error) {

	//fmt.Println(m.Source)
	publickey := m[string(source)]
	//fmt.Println(source)
	if publickey == nil {
		panic("none")
	}

	return publickey.Verify(msgbytes, sig)
}

func ConvertByte2String(bytes []byte) string {
	newString := fmt.Sprintf("%x", bytes)
	return newString
}

// func PoissonDistribution(parameter float64) int {
// 	expRand := rand.ExpFloat64() / parameter
// 	poissonRand := int(math.Round(math.Log(expRand) / math.Log(math.E) * (-expRand)))

// 	return poissonRand
// }

type PoissonGenerator struct {
	rand *rand.Rand
}

func NewPoissonGenerator(seed int64) *PoissonGenerator {
	return &PoissonGenerator{rand: rand.New(rand.NewSource(seed))}
}

func (p *PoissonGenerator) Poisson(mean float64) int {
	// Use the Knuth algorithm to generate a Poisson-distributed random number
	L := math.Exp(-mean)
	k := 0.0
	pa := 1.0

	for pa >= L {
		k++
		pa *= p.rand.Float64()
	}

	return int(k - 1)
}

func ShuffleNodeIdList(shardSize int, msgHash []byte) []int {
	candiNodeId := make([]int, shardSize) // TODO simplification
	for i := 0; i < shardSize; i++ {
		candiNodeId[i] = i
	}

	// get seed from msgHash
	var seed int64
	buf := bytes.NewBuffer(msgHash)
	binary.Read(buf, binary.BigEndian, &seed)
	rand.Seed(seed)
	rand.Shuffle(len(candiNodeId), func(i, j int) { candiNodeId[i], candiNodeId[j] = candiNodeId[j], candiNodeId[i] })

	return candiNodeId
}
