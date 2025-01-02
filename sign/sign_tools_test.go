package sign

import (
	"bytes"
	"fmt"
	"testing"
)

// test if keys created twice are the sanme
func TestGenED25519KeysDuplicate(t *testing.T) {
	privKey1, pubKey1 := GenED25519Keys()
	privKey2, pubKey2 := GenED25519Keys()
	if bytes.Equal(privKey1, privKey2) || bytes.Equal(pubKey1, pubKey2) {
		fmt.Println("keys generated twice may be the same")
	} else {
		fmt.Println("keys generated twice are different")
	}
}

// test if the TS private key can be encoded correctly
func TestEncodeTSPrivKeys(t *testing.T) {
	shares, _ := GenTSKeys(3, 4)
	share0 := shares[0]
	share0AsBytes, err := EncodeTSPartialKey(share0)
	if err != nil {
		t.Fatal(err)
	}

	share0Decoded, err := DecodeTSPartialKey(share0AsBytes)
	if err != nil {
		t.Fatal(err)
	}

	if !EqualTSPartialKey(share0, share0Decoded) {
		t.Fatal("TS private keys encoded/decoded failure")
	}
}

// test if the TS public key can be encoded correctly
func TestEncodeTSPublicKeys(t *testing.T) {
	_, pubKey := GenTSKeys(3, 4)

	pubKeyAsBytes, err := EncodeTSPublicKey(pubKey)
	if err != nil {
		t.Fatal(err)
	}

	pubKeyDecoded, err := DecodeTSPublicKey(pubKeyAsBytes)
	if err != nil {
		t.Fatal(err)
	}

	if !pubKeyDecoded.Equal(pubKey) {
		t.Fatal("TS public keys encoded/decoded failure")
	}
}

// test if ts keys after encoding and decoding can also do verification successfully
func TestTSVerificationAfterEncoding(t *testing.T) {
	shares, pubKey := GenTSKeys(3, 4)

	data := []byte("A message to be signed")

	// encode and decode the keys & mock the network transfer via channel
	shareKeyAsBytes := make([][]byte, 4)
	var err error
	for i := 0; i < 4; i++ {
		shareKeyAsBytes[i], err = EncodeTSPartialKey(shares[i])
		if err != nil {
			t.Fatal()
		}
	}
	pubKeyAsBytes, err := EncodeTSPublicKey(pubKey)
	if err != nil {
		t.Fatal()
	}
	// transfer three private keys to node0, node1, node2, and public key to node3
	chs := make([]chan []byte, 4)
	for i := 0; i < 4; i++ {
		chs[i] = make(chan []byte, 1)
	}
	for i := 0; i < 3; i++ {
		go func(index int) {
			chs[index] <- shareKeyAsBytes[index]
		}(i)
	}
	go func() {
		chs[3] <- pubKeyAsBytes
	}()

	shareKeyByNodes := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		shareKeyByNodes[i] = <-chs[i]
	}

	var pubKeyByNode3 []byte
	pubKeyByNode3 = <-chs[3]

	// Node0, Node1, Node2: sign the ts partial signature and send to Node3
	for i := 0; i < 3; i++ {
		// decode the private keys
		shareKey, err := DecodeTSPartialKey(shareKeyByNodes[i])
		if err != nil {
			t.Fatal(err)
		}
		sig := SignTSPartial(shareKey, data)
		// transfer to node3
		go func(index int) {
			chs[index] <- sig
		}(i)
	}

	// Node3: receive the signatures
	sigs := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		sigs[i] = <-chs[i]
	}

	// Node3: decode the public key
	pubKeyDecodedByNode3, err := DecodeTSPublicKey(pubKeyByNode3)
	if err != nil {
		t.Fatal(err)
	}
	intactSigByNode3 := AssembleIntactTSPartial(sigs, pubKeyDecodedByNode3, data, 3, 4)
	ok, err := VerifyTS(pubKeyDecodedByNode3, data, intactSigByNode3)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("fail to verify the threshold signature")
	}

}
