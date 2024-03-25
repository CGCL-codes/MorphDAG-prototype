package state

import (
	"encoding/json"
	"log"

	kv "github.com/zllai/go-MerklePatriciaTree/kvstore"
	trie "github.com/zllai/go-MerklePatriciaTree/mpt"
)

type TrieDB struct {
	Db   *kv.LevelDB
	Trie *trie.Trie
	//Values map[string][]byte
}

// StoreState stores the account state into the state trie
func (tr *TrieDB) StoreState(obj *stateObject) error {
	addr := obj.Address()
	//key := bytes.Join([][]byte{[]byte("a"), addr}, []byte{})
	acc := obj.Account()
	value, _ := json.Marshal(acc)
	//tr.Values[string(addr)] = value
	if err := tr.Trie.Put(addr, value); err != nil {
		return err
	}
	return nil
}

// FetchState fetches the account state from the state trie
func (tr *TrieDB) FetchState(addr []byte) (*Account, error) {
	// key := bytes.Join([][]byte{[]byte("a"), addr}, []byte{})
	data, err := tr.Trie.Get(addr)
	if err != nil {
		return nil, err
	}
	var acc Account
	err = json.Unmarshal(data, &acc)
	if err != nil {
		log.Panic(err)
	}
	return &acc, nil
}

// RemoveState removes the account state from the underlying database
func (tr *TrieDB) RemoveState(addr []byte) error {
	// key := bytes.Join([][]byte{[]byte("a"), addr}, []byte{})
	if err := tr.Db.Delete(addr); err != nil {
		return err
	}
	return nil
}

// Commit commits the state trie into the underlying database
func (tr *TrieDB) Commit() []byte {
	tr.Trie.Commit()
	root, _ := tr.Trie.Serialize()
	rootHash := root
	if len(root) > 4 {
		rootHash = root[:4]
	}
	// rootHash := root[:4]
	return rootHash
}

// Abort aborts operations and rolls back to the last committed state
func (tr *TrieDB) Abort() {
	tr.Trie.Abort()
}

// Close closes the underlying leveldb
func (tr *TrieDB) Close() {
	tr.Db.Close()
}

// Clear clears in-memory trie nodes
func (tr *TrieDB) Clear() {
	newT := trie.New(nil, tr.Db)
	tr.Trie = newT
}

// NewTrieDB creates a new trieDB from a given state root
// if the root hash is nil, create an empty state trie
func NewTrieDB(blkFile string, rootHash []byte) (*TrieDB, error) {
	db, err := kv.NewLevelDB(blkFile)
	if err != nil {
		return nil, err
	}

	var tr *trie.Trie
	if rootHash == nil {
		tr = trie.New(nil, db)
	} else {
		hashNode := trie.HashNode(rootHash)
		tr = trie.New(&hashNode, db)
	}

	return &TrieDB{db, tr}, nil
}
