package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn  *MvccTxn
	key  []byte
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C). m          vbn
	return &Scanner{
		key:  startKey,
		txn:  txn,
		iter: txn.Reader.IterCF(engine_util.CfWrite),
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	iter := scan.iter
	if !iter.Valid() {
		return nil, nil, nil
	}

	iter.Seek(EncodeKey(scan.key, scan.txn.StartTS))
	if !iter.Valid() {
		return nil, nil, nil
	}

	item := iter.Item()
	key := item.KeyCopy(nil)
	userKey := DecodeUserKey(key)

	if !bytes.Equal(userKey, scan.key) {
		scan.key = userKey
		return scan.Next()
	}

	// jump other same key
	for iter.Next(); iter.Valid(); iter.Next() {
		tempItem := iter.Item()
		userK := DecodeUserKey(tempItem.Key())

		if !bytes.Equal(userK, key) {
			scan.key = userK
			break
		}
	}

	// if !iter.Valid() {
	// 	return nil, nil, nil
	// }

	val, err := item.ValueCopy(nil)
	if err != nil {
		return nil, nil, err
	}
	write, err := ParseWrite(val)
	if err != nil {
		return nil, nil, err
	}

	data, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
	if err != nil {
		return nil, nil, err
	}
	return userKey, data, nil
}
