package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"math"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	iter engine_util.DBIterator
	txn  *MvccTxn
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
	for ; iter.Valid(); iter.Next() {
		log.Infof("new scanner: key %v ts %v", DecodeUserKey(iter.Item().Key()), decodeTimestamp(iter.Item().Key()))
		if ts := decodeTimestamp(iter.Item().Key()); ts <= txn.StartTS {
			// we find a key whose ts satisfies
			// (we may skip some keys that shouldn't appear in this txn's ts)
			log.Infof("new scanner: find a valid kv")
			break
		}
	}
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
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	retKey := DecodeUserKey(scan.iter.Item().Key())
	writeValue, err := scan.iter.Item().Value()
	if err != nil {
		return nil, nil, err
	}
	write, err := ParseWrite(writeValue)
	if err != nil {
		return nil, nil, err
	}
	retVal, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(retKey, write.StartTS))
	if err != nil {
		return nil, nil, err
	}
	log.Infof("scan ret key %v value %v", retKey, retVal)

	lastKey := retKey
	for ; scan.iter.Valid(); scan.iter.Next() {
		userKey := DecodeUserKey(scan.iter.Item().Key())
		log.Infof("scan key %v ", userKey)
		if bytes.Equal(userKey, lastKey) {
			// continues util we find the next key that differs from the last key
			continue
		}
		if ts := decodeTimestamp(scan.iter.Item().Key()); ts <= scan.txn.StartTS {
			// we find a key whose ts satisfies
			// (we may skip some keys that shouldn't appear in this txn's ts)
			writeValue, err := scan.iter.Item().Value()
			if err != nil {
				return nil, nil, err
			}
			write, err := ParseWrite(writeValue)
			if err != nil {
				return nil, nil, err
			}
			if write.Kind != WriteKindPut {
				// this write record isn't put, so no default kv in db
				lastKey = userKey
				continue
			}
			break
		}
	}

	return retKey, retVal, nil
}
