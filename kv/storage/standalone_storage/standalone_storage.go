package standalone_storage

import (
	"fmt"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// opts := badger.DefaultOptions
	// opts.Dir = conf.DBPath
	instance := &StandAloneStorage{
		conf: conf,
	}
	return instance
}

func (s *StandAloneStorage) Start() error {
	if s == nil {
		panic(fmt.Sprintf("nil pointer exception %v", s))
	}

	s.db = engine_util.CreateDB(s.conf.DBPath, false)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if s == nil {
		panic(fmt.Sprintf("nil pointer exception %v", s))
	}

	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{
		db:  s.db,
		txn: s.db.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			putModify := modify.Data.(storage.Put)

			err := engine_util.PutCF(s.db, putModify.Cf, putModify.Key, putModify.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			deleteModify := modify.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.db, deleteModify.Cf, deleteModify.Key)
			if err != nil {
				return err
			}
		}
	}
	return txn.Commit()
}

// implemnt StorageReader Interface
type StandAloneStorageReader struct {
	db  *badger.DB
	txn *badger.Txn
}

func (sasr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(sasr.txn, cf, key)
	if len(val) == 0 {
		return nil, nil
	}
	return val, err
}

func (sasr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sasr.txn)
}

func (sasr *StandAloneStorageReader) Close() {
	sasr.txn.Discard()
}
