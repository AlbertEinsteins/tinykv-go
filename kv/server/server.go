package server

import (
	"context"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
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

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}

	if lock != nil && req.Version >= lock.Ts {
		return &kvrpcpb.GetResponse{
			Error: &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         req.Key,
					LockTtl:     lock.Ttl,
				},
			},
		}, err
	}

	val, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}

	fmt.Println(val, err)
	resp := &kvrpcpb.GetResponse{}
	if val == nil {
		resp.NotFound = true
		return resp, nil
	}

	resp.Value = val
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var resp kvrpcpb.PrewriteResponse
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, mutation := range req.Mutations { // try lock every key
		// check write
		write, commitedTs, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}

		if write != nil && commitedTs >= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: commitedTs,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}

		// check if locked
		lock, err := txn.GetLock(mutation.Key)

		if err != nil {
			return nil, err
		}

		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         mutation.Key,
					LockTtl:     lock.Ttl,
				},
			})
			continue
		}

		// write in cf_default
		var kind mvcc.WriteKind
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			kind = mvcc.WriteKindPut
			txn.PutValue(mutation.Key, mutation.Value)
		case kvrpcpb.Op_Del:
			kind = mvcc.WriteKindDelete
			txn.DeleteValue(mutation.Key)
		}

		// lock with primary key
		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})
	}

	if len(resp.Errors) > 0 {
		return &resp, nil
	}

	// save txn locally
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	// check every lock first
	var resp kvrpcpb.CommitResponse
	for _, key := range req.Keys {
		// get most update write
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}

		// fmt.Println(write, write.Kind == mvcc.WriteKindRollback)
		if write != nil && write.Kind != mvcc.WriteKindRollback && write.StartTS == req.StartVersion {
			return &resp, nil
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		// lock is nil means the prewirte is late after lock.Ts
		// or lock.Ts != req.StartVersion iif when the former lock
		//		is rolled back by another txn
		if lock == nil || lock.Ts != req.StartVersion {
			// if lock != nil {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
			}
			// }

			return &resp, nil
		}

		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})

		// delete lock of this key
		txn.DeleteLock(key)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	size := req.Limit
	var resp kvrpcpb.ScanResponse

	for size > 0 {
		key, val, err := scanner.Next()
		if err != nil {
			return &resp, err
		}

		if key == nil {
			break
		}

		// get lock with key, check if in write state
		lock, err := txn.GetLock(key)
		if err != nil {
			return &resp, err
		}

		if lock != nil && lock.Ts <= txn.StartTS {
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{},
				},
			})

			size--
			continue
		}

		if val != nil {
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
				Key:   key,
				Value: val,
			})
			size--
		}

	}
	return &resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// check write first
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	currentWrite, commitedTs, err := txn.CurrentWrite(req.PrimaryKey)
	var resp kvrpcpb.CheckTxnStatusResponse

	if err != nil {
		return nil, err
	}

	if currentWrite != nil {
		// if has been commited
		if currentWrite.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = commitedTs
		}
		return &resp, nil
	}

	// check primary lock
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	if lock == nil { // if not exists, had been rolledback
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			Kind:    mvcc.WriteKindRollback,
			StartTS: req.LockTs,
		})

		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}

		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return &resp, nil
	}

	// check if is expired
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)

		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			Kind:    mvcc.WriteKindRollback,
			StartTS: req.LockTs,
		})

		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		return &resp, nil
	}

	return &resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	var resp kvrpcpb.BatchRollbackResponse
	for _, k := range req.Keys {
		write, _, err := txn.CurrentWrite(k)
		if err != nil {
			return nil, err
		}

		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			}

			resp.Error = &kvrpcpb.KeyError{
				Abort: "true",
			}
			return &resp, nil
		}

		// get lock
		lock, err := txn.GetLock(k)
		if err != nil {
			return nil, err
		}

		if lock != nil && lock.Ts == req.StartVersion { // delete lock, and val
			txn.DeleteLock(k)
			txn.DeleteValue(k)
		}

		// put a rollback write
		txn.PutWrite(k, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}

	// save locally
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var resp kvrpcpb.ResolveLockResponse
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()

	var keys [][]byte

	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}

		// in lock keys with provided ts
		lock, err := mvcc.ParseLock(val)
		if err != nil {
			return nil, err
		}

		if lock.Ts == req.StartVersion {
			key := item.KeyCopy(nil)
			keys = append(keys, key)
		}
	}

	if req.CommitVersion != 0 {
		commitReq := kvrpcpb.CommitRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		}

		rtnResp, err := server.KvCommit(context.TODO(), &commitReq)
		resp.Error = rtnResp.Error
		resp.RegionError = rtnResp.RegionError
		return &resp, err
	}

	rollbackReq := kvrpcpb.BatchRollbackRequest{
		Context:      req.Context,
		Keys:         keys,
		StartVersion: req.StartVersion,
	}

	rtnResp, err := server.KvBatchRollback(context.TODO(), &rollbackReq)
	resp.Error = rtnResp.Error
	resp.RegionError = rtnResp.RegionError
	return &resp, err
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
