package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	re, _ := server.storage.Reader(nil)
	defer re.Close()
	rr, err := re.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	if rr == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: rr}, nil

}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	p := storage.Put{req.Key, req.Value, req.Cf}
	m := storage.Modify{p}
	err := server.storage.Write(nil, []storage.Modify{m})
	return nil, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	d := storage.Delete{req.Key, req.Cf}
	m := storage.Modify{d}
	err := server.storage.Write(nil, []storage.Modify{m})
	return nil, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	r, _ := server.storage.Reader(nil)
	iter := r.IterCF(req.Cf)
	defer r.Close()
	defer iter.Close()
	limit := req.Limit
	KVs := []*kvrpcpb.KvPair{}
	for iter.Seek(req.StartKey); iter.Valid() && limit > 0; iter.Next() {
		item := iter.Item()
		value, _ := item.ValueCopy(nil)
		kv := &kvrpcpb.KvPair{Error: nil, Key: item.KeyCopy(nil), Value: value}
		KVs = append(KVs, kv)
		limit = limit - 1
	}
	return &kvrpcpb.RawScanResponse{Kvs: KVs}, nil
}
