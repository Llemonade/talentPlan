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
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	//return nil
	db := engine_util.CreateDB(conf.DBPath, false)
	return &StandAloneStorage{db}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	//return nil
	if s.db == nil {
		return fmt.Errorf("storage nil!")
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	return &StandAloneStorageReader{txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	err := s.db.Update(func(txn *badger.Txn) error {
		wb := new(engine_util.WriteBatch)
		for _, m := range batch {
			switch m := m.Data.(type) {
			case storage.Put:
				wb.SetCF(m.Cf, m.Key, m.Value)
			case storage.Delete:
				wb.SetCF(m.Cf, m.Key, nil)
			}
		}
		return wb.WriteToDB(s.db)

	})
	return err
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (r StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	//TODO implement me
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return []byte(nil), nil
	}
	return val, err
}

func (r StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	//TODO implement me
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r StandAloneStorageReader) Close() {
	//TODO implement me
	r.txn.Discard()
}
