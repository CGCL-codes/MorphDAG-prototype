package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/PlainDAG/go-PlainDAG/core/ttype"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"strconv"
)

// StoreMsgs stores a batch of messages to db
func StoreMsgs(db *leveldb.DB, msgs []Message) {
	for _, msg := range msgs {
		if err := storeMsg(db, msg); err != nil {
			log.Printf("error happened in msg storing: %v\n", err)
		}
	}
}

func storeMsg(db *leveldb.DB, msg Message) error {
	key := bytes.Join([][]byte{[]byte("m"), msg.GetHash()}, []byte{})
	msgData, err := msg.Serialize()
	if err != nil {
		return err
	}
	err = db.Put(key, msgData, nil)
	if err != nil {
		return err
	}
	return nil
}

// StoreMsgsByRound stores a batch of messages in a round to db
func StoreMsgsByRound(db *leveldb.DB, msgs StoredMsgs) error {
	key := bytes.Join([][]byte{[]byte("r"), []byte(strconv.Itoa(msgs.Rn))}, []byte{})
	data, err := json.Marshal(msgs)
	if err != nil {
		return err
	}
	err = db.Put(key, data, nil)
	if err != nil {
		return err
	}
	return nil
}

// StoreLeaderByRound stores a leader message in a round to db
func StoreLeaderByRound(db *leveldb.DB, msg Message) error {
	key := bytes.Join([][]byte{[]byte("l"), []byte(strconv.Itoa(msg.GetRN()))}, []byte{})
	data := msg.GetHash()
	err := db.Put(key, data, nil)
	if err != nil {
		return err
	}
	return nil
}

// FetchMsgsByRound fetches message data by round from db
func FetchMsgsByRound(db *leveldb.DB, round int) ([]byte, error) {
	key := bytes.Join([][]byte{[]byte("r"), []byte(strconv.Itoa(round))}, []byte{})
	value, err := db.Get(key, nil)
	if err != nil {
		return nil, errors.New("error happened in msg fetching")
	}
	return value, nil
}

// FetchLeaderByRound fetches leader message data by round from db
func FetchLeaderByRound(db *leveldb.DB, round int) ([]byte, error) {
	key := bytes.Join([][]byte{[]byte("l"), []byte(strconv.Itoa(round))}, []byte{})
	value, err := db.Get(key, nil)
	if err != nil {
		return nil, errors.New("error happened in msg fetching")
	}
	return value, nil
}

// RemoveMsg removes block from db
func RemoveMsg(db *leveldb.DB, msgHash []byte) error {
	key := bytes.Join([][]byte{[]byte("m"), msgHash[:]}, []byte{})
	err := db.Delete(key, nil)
	if err != nil {
		return errors.New("error happened in msg removing")
	}
	return nil
}

// FetchMsg gets block data via key from db
func FetchMsg(db *leveldb.DB, msgHash []byte) ([]byte, error) {
	key := bytes.Join([][]byte{[]byte("m"), msgHash[:]}, []byte{})
	value, err := db.Get(key, nil)
	if err != nil {
		return nil, errors.New("error happened in msg fetching")
	}
	return value, nil
}

// StoreTxs stores a batch of txs to db
func StoreTxs(db *leveldb.DB, txs []*ttype.Transaction) {
	for _, tx := range txs {
		if err := storeTx(db, *tx); err != nil {
			log.Printf("error happened in tx storing: %v\n", err)
		}
	}
}

func storeTx(db *leveldb.DB, tx ttype.Transaction) error {
	txData := tx.Serialize()
	txHash := tx.Hash()
	key := bytes.Join([][]byte{[]byte("t"), txHash[:]}, []byte{})
	err := db.Put(key, txData, nil)
	if err != nil {
		return err
	}
	return nil
}

// RemoveTx removes tx from db
func RemoveTx(db *leveldb.DB, txHash []byte) error {
	key := bytes.Join([][]byte{[]byte("t"), txHash[:]}, []byte{})
	err := db.Delete(key, nil)
	if err != nil {
		return errors.New("error happened in tx removing")
	}
	return nil
}

// FetchTx gets value via key from db
func FetchTx(db *leveldb.DB, txHash []byte) ([]byte, error) {
	key := bytes.Join([][]byte{[]byte("t"), txHash[:]}, []byte{})
	log.Println("Fetch transaction key:", key)
	value, err := db.Get(key, nil)
	if err != nil {
		return nil, errors.New("error happened in tx fetching")
	}
	return value, nil
}

// LoadDB opens db instance or create a db if not exist
func LoadDB(blkFile string) (*leveldb.DB, error) {
	db, err := leveldb.OpenFile(blkFile, nil)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return db, nil
}
