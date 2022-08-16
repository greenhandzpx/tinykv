package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"math"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	iter    engine_util.DBIterator
	lastKey []byte
	txn     *MvccTxn
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		txn: txn,
	}
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	// we let the scanner seek to the right place first
	// and the first call to Next will get the value at once
	iter.Seek(EncodeKey(startKey, math.MaxUint64))
	//for ; iter.Valid(); iter.Next() {
	//}
	scanner.iter = iter
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for ; scan.iter.Valid(); scan.iter.Next() {
		userKey := DecodeUserKey(scan.iter.Item().Key())
		if bytes.Equal(userKey, scan.lastKey) {
			// continues util we find the next key that differs from the last key
			continue
		}
		if ts := decodeTimestamp(scan.iter.Item().Key()); ts <= scan.txn.StartTS {
			// we find a key whose ts satisfies
			// (we may skip some keys that shouldn't appear in this txn's ts)
			scan.lastKey = userKey
			writeValue, err := scan.iter.Item().Value()
			if err != nil {
				return nil, nil, err
			}
			write, err := ParseWrite(writeValue)
			if err != nil {
				return nil, nil, err
			}
			value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(scan.iter.Item().Key(), write.StartTS))
			if err != nil {
				return nil, nil, err
			}
			return userKey, value, nil
		}
	}
	return nil, nil, nil
}
