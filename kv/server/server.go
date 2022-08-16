package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler

	mvccTxns map[uint64]*mvcc.MvccTxn
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage:  storage,
		Latches:  latches.NewLatches(),
		mvccTxns: make(map[uint64]*mvcc.MvccTxn),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

func (server *Server) getTxn(startVersion uint64, ctx *kvrpcpb.Context) (*mvcc.MvccTxn, error) {
	if _, ok := server.mvccTxns[startVersion]; !ok {
		// if this is the first request of one txn
		// then we should create a txn
		reader, err := server.storage.Reader(ctx)
		if err != nil {
			return nil, err
		}
		txn := mvcc.NewMvccTxn(reader, startVersion)
		server.mvccTxns[startVersion] = txn
	}
	return server.mvccTxns[startVersion], nil
}

func (server *Server) checkLocked(txn *mvcc.MvccTxn, key []byte) (*kvrpcpb.KeyError, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts < txn.StartTS {
		// TODO don't know why should check the ts
		// means this key has been locked
		keyError := &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         key,
				LockTtl:     lock.Ttl,
			},
		}
		log.Infof("lock already exists ts %v", lock.Ts)
		return keyError, nil
	}
	return nil, nil
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	txn, err := server.getTxn(req.Version, req.Context)
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.GetResponse{}

	// check lock
	keyError, err := server.checkLocked(txn, req.Key)
	if err != nil {
		return nil, err
	}
	if keyError != nil {
		resp.Error = keyError
		return resp, nil
	}

	// get value
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
	// TODO handle region error
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	txn, err := server.getTxn(req.StartVersion, req.Context)
	if err != nil {
		return nil, err
	}

	resp := &kvrpcpb.PrewriteResponse{}

	// TODO not sure whether i should remove duplicate keys
	keys := make([][]byte, len(req.Mutations))
	for i, _ := range keys {
		keys[i] = req.Mutations[i].Key
	}
	//keys = append(keys, req.PrimaryLock)
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	//// first prewrite the primary key
	//// check lock
	//keyError, err := server.checkLocked(txn, req.PrimaryLock)
	//if err != nil {
	//	return nil, err
	//}
	//if keyError != nil {
	//	resp.Errors = append(resp.Errors, keyError)
	//	return resp, nil
	//}

	// then prewrite all secondary keys
	for _, mut := range req.Mutations {
		// check lock
		keyError, err := server.checkLocked(txn, mut.Key)
		if err != nil {
			return nil, err
		}
		if keyError != nil {
			resp.Errors = append(resp.Errors, keyError)
			return resp, nil
		}
		// check write-conflict
		write, ts, err := txn.MostRecentWrite(mut.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && ts > req.StartVersion {
			// write-conflict
			keyError := &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts,
					Key:        mut.Key,
					Primary:    req.PrimaryLock,
				},
			}
			resp.Errors = append(resp.Errors, keyError)
			return resp, nil
		}
	}

	//// lock primary key
	//lockInfo := &mvcc.Lock{
	//	Primary: req.PrimaryLock,
	//	Ts:      req.StartVersion,
	//	Ttl:     req.LockTtl,
	//}
	//txn.PutLock(req.PrimaryLock, lockInfo)

	// lock all keys and execute all mutations
	for _, mut := range req.Mutations {
		// lock the key
		lockInfo := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
		}
		// write data
		switch mut.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mut.Key, mut.Value)
			lockInfo.Kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mut.Key)
			lockInfo.Kind = mvcc.WriteKindDelete
		case kvrpcpb.Op_Rollback:
			// TODO not sure
			lockInfo.Kind = mvcc.WriteKindRollback
		}
		txn.PutLock(mut.Key, lockInfo)
	}

	// write all modifies to the engine
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
	// TODO handle region error
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	server.Latches.AcquireLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	txn, err := server.getTxn(req.StartVersion, req.Context)
	if err != nil {
		log.Infof("get txn err %v", err)
		return nil, err
	}
	resp := &kvrpcpb.CommitResponse{}
	for _, key := range req.Keys {
		// 1. check whether the lock still exists & release the lock
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			// the lock has been released
			// (maybe because of repeated commit or rollback)
			write, ts, err := txn.MostRecentWrite(key)
			if err != nil {
				return nil, err
			}
			if write != nil && ts == req.CommitVersion &&
				write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					// TODO not sure
					Abort: "the txn has rollbacked",
				}
			}
			return resp, nil
		}
		if lock.Ts != txn.StartTS {
			// the lock has been fetched by other txn
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "the lock has changed",
			}
			return resp, nil
		}
		// release the lock
		txn.DeleteLock(key)
		// 2. put a write entry to the DB
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
	}
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	txn, err := server.getTxn(req.Version, req.Context)
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.ScanResponse{}
	scanner := mvcc.NewScanner(req.StartKey, txn)
	for i := uint32(0); i < req.Limit; i++ {
		var key, value []byte
		if key, value, err = scanner.Next(); err != nil {
			return nil, err
		}
		if key == nil || value == nil {
			break
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	txn, err := server.getTxn(req.LockTs, req.Context)
	if err != nil {
		return nil, err
	}
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	if lock == nil {
		// 1. the lock doesn't exist
		// maybe 1) the lock has committed 2) the lock is missing
		write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
		if err != nil {
			return nil, err
		}
		if write != nil {
			// 1) the lock has committed
			resp.CommitVersion = commitTs
		} else {
			// 2) the lock is missing
			resp.Action = kvrpcpb.Action_LockNotExistRollback
		}
	} else {
		// 2. the lock exists
		// maybe 1) the lock expires 2) the lock doesn't expire
		if mvcc.PhysicalTime(req.CurrentTs)-mvcc.PhysicalTime(lock.Ts) >= lock.Ttl {
			// 1) this lock has expired, we should roll back the lock
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			resp.LockTtl = 0
			// rollback the lock
			txn.DeleteLock(req.PrimaryKey)
		} else {
			// 2) the lock doesn't expire
			resp.LockTtl = lock.Ttl
		}

	}
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	txn, err := server.getTxn(req.StartVersion, req.Context)
	if err != nil {
		return nil, err
	}

	resp := &kvrpcpb.BatchRollbackResponse{}
	// check whether the key is still locked by this txn
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			// TODO handle not locked
			continue
		}
		if lock.Ts != req.StartVersion {
			// this lock isn't held by this txn
			resp.Error = &kvrpcpb.KeyError{
				Abort: "the lock is locked by another txn",
			}
			return resp, nil
		}
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			// means this key has committed
			resp.Error = &kvrpcpb.KeyError{
				Abort: "the key has been committed",
			}
			return resp, nil
		}
		// then we can delete the lock and the value
		txn.DeleteLock(key)
		// TODO not sure
		txn.DeleteValue(key)
	}

	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil

}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	txn, err := server.getTxn(req.StartVersion, req.Context)
	if err != nil {
		return nil, err
	}

	keys := make([][]byte, 0)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	// get all the locks in this txn
	iter := reader.IterCF(engine_util.CfLock)
	for ; iter.Valid(); iter.Next() {
		value, err := iter.Item().Value()
		if err != nil {
			return nil, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return nil, err
		}
		if lock.Ts == txn.StartTS {
			keys = append(keys, iter.Item().Key())
		}
	}
	iter.Close()

	resp := kvrpcpb.ResolveLockResponse{}
	if req.CommitVersion == 0 {
		// rollback txn
		// TODO not sure
		rollbackReq := &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		}
		rollbackResp, err := server.KvBatchRollback(nil, rollbackReq)
		if err != nil {
			return nil, err
		}
		resp.Error = rollbackResp.Error

	} else {
		// commit txn
		commitReq := &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  txn.StartTS,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		}
		commitResp, err := server.KvCommit(nil, commitReq)
		if err != nil {
			return nil, err
		}
		resp.Error = commitResp.Error

	}
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
