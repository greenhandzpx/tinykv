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
	engine     *engine_util.Engines
	reader     *Reader
	writeBatch *engine_util.WriteBatch
}

type Reader struct {
	txn   *badger.Txn
	iters []engine_util.DBIterator
}

func (r *Reader) GetCF(cf string, key []byte) ([]byte, error) {
	iter := engine_util.NewCFIterator(cf, r.txn)
	defer iter.Close()

	iter.Seek(key)
	if iter.Valid() == false {
		return nil, nil
	}

	value, err := iter.Item().Value()
	// not sure whether we can close the iter immediately
	return value, err
}

func (r *Reader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, r.txn)
	// add this new iter to the iter set and close them when finishing txn
	r.iters = append(r.iters, iter)
	fmt.Println("add an iter")
	return iter
}

func (r *Reader) Close() {
	// TODO
	//for _, iter := range r.iters {
	//	iter.Close()
	//}
	r.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		engine: engine_util.NewEngines(engine_util.CreateDB(conf.DBPath, false),
			nil, conf.StoreAddr, ""),
		writeBatch: &engine_util.WriteBatch{},
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.reader = nil
	s.writeBatch.Reset()
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.reader != nil {
		s.reader.Close()
	}
	return s.engine.Destroy()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	if s.reader != nil {
		return s.reader, nil
	}
	txn := s.engine.Kv.NewTransaction(false)
	reader := Reader{
		txn:   txn,
		iters: make([]engine_util.DBIterator, 0),
	}
	return &reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			put := modify.Data.(storage.Put)
			s.writeBatch.SetCF(put.Cf, put.Key, put.Value)
		case storage.Delete:
			del := modify.Data.(storage.Delete)
			fmt.Printf("delete a key: %v\n", del.Key)
			s.writeBatch.DeleteCF(del.Cf, del.Key)
		}
	}
	err := s.writeBatch.WriteToDB(s.engine.Kv)
	s.writeBatch.Reset()
	return err
}
