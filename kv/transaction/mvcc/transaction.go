package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	valBytes := write.ToBytes()
	// encode Key With Ts
	keyBytes := EncodeKey(key, ts)

	modify := storage.Put{
		Cf:    engine_util.CfWrite,
		Key:   keyBytes,
		Value: valBytes,
	}
	txn.writes = append(txn.writes, storage.Modify{
		Data: modify,
	})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	reader := txn.Reader

	val, err := reader.GetCF(engine_util.CfLock, key)

	if err != nil || len(val) == 0 {
		return nil, err
	}

	return ParseLock(val)
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	modify := storage.Put{
		Cf:    engine_util.CfLock,
		Key:   key,
		Value: lock.ToBytes(),
	}

	txn.writes = append(txn.writes, storage.Modify{
		Data: modify,
	})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	modify := storage.Delete{
		Cf:  engine_util.CfLock,
		Key: key,
	}

	txn.writes = append(txn.writes, storage.Modify{
		Data: modify,
	})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).

	// 1.get all commited entry order by commit ts desending with provided key
	writes, commitedTss := totalWriteWithCommitTsByKey(txn.Reader, key)

	var readTsAfterRecentCommited uint64
	for idx, commitedTs := range commitedTss {
		if commitedTs <= txn.StartTS {
			if writes[idx].Kind == WriteKindDelete { // deleted already
				return nil, nil
			}

			readTsAfterRecentCommited = writes[idx].StartTS
			break
		}
	}

	if readTsAfterRecentCommited == 0 {
		return nil, nil
	}

	// 2.get from default data
	iter := txn.Reader.IterCF(engine_util.CfDefault)
	defer iter.Close()

	iter.Seek(EncodeKey(key, readTsAfterRecentCommited))
	if !iter.Valid() {
		return nil, nil
	}

	return iter.Item().Value()
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	modify := storage.Put{
		Cf:    engine_util.CfDefault,
		Key:   EncodeKey(key, txn.StartTS),
		Value: value,
	}

	txn.writes = append(txn.writes, storage.Modify{
		Data: modify,
	})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	modify := storage.Delete{
		Cf:  engine_util.CfDefault,
		Key: EncodeKey(key, txn.StartTS),
	}

	txn.writes = append(txn.writes, storage.Modify{
		Data: modify,
	})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	commitedTs := uint64(0)
	var currentWrite *Write

	totalWrites, commitedTsList := totalWriteWithCommitTsByKey(txn.Reader, key)
	if len(totalWrites) != len(commitedTsList) {
		panic("The len of read Writes and CommitTsList must equal")
	}

	for idx, write := range totalWrites {
		if write.StartTS == txn.StartTS {
			currentWrite = &Write{
				StartTS: write.StartTS,
				Kind:    write.Kind,
			}
			commitedTs = commitedTsList[idx]
			break
		}
	}
	return currentWrite, commitedTs, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	commitedTs := uint64(0)
	var currentWrite *Write

	iter.Seek(key)

	if !iter.Valid() {
		return currentWrite, commitedTs, nil
	}

	item := iter.Item()
	userKey := DecodeUserKey(item.Key())

	// fmt.Println(key, item.Key(), userKey, decodeTimestamp(item.Key()))
	if !bytes.Equal(userKey, key) {
		return currentWrite, commitedTs, nil
	}

	val, err := item.Value()
	if err != nil {
		return nil, commitedTs, err
	}

	write, err := ParseWrite(val)
	if err != nil {
		return nil, commitedTs, err
	}

	currentWrite = write
	commitedTs = decodeTimestamp(item.Key())

	return currentWrite, commitedTs, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}

func totalWriteWithCommitTsByKey(reader storage.StorageReader, key []byte) ([]*Write, []uint64) {
	iter := reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	writes := []*Write{}
	commitedTs := []uint64{}
	for iter.Seek(key); iter.Valid(); iter.Next() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())

		if !bytes.Equal(userKey, key) {
			break
		}
		val, err := item.Value()
		if err != nil {
			panic(err)
		}

		write, err := ParseWrite(val)
		if err != nil {
			panic(err)
		}

		writes = append(writes, write)
		commitedTs = append(commitedTs, decodeTimestamp(item.Key()))
	}
	return writes, commitedTs
}
