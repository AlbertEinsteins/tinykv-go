package server

import (
	"context"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	resp := &kvrpcpb.RawGetResponse{
		Value: val,
	}

	if len(val) == 0 {
		resp.NotFound = true
	}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := storage.Put{
		Cf:    req.Cf,
		Key:   req.Key,
		Value: req.Value,
	}

	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: modify,
		},
	})

	resp := &kvrpcpb.RawPutResponse{}

	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modify := storage.Delete{
		Cf:  req.Cf,
		Key: req.Key,
	}

	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: modify,
		},
	})

	resp := &kvrpcpb.RawDeleteResponse{}

	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	iter := reader.IterCF(req.Cf)
	limit := req.Limit
	startKey := req.StartKey

	// search pos
	iter.Seek(startKey)

	kvParis := []*kvrpcpb.KvPair{}
	for ; iter.Valid() && limit > 0; iter.Next() {
		k := iter.Item().Key()
		v, err := iter.Item().ValueCopy(nil)
		if err != nil {
			panic(fmt.Sprintf("Error copying value when k=%v", k))
		}

		kvParis = append(kvParis, &kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		})
		limit--
	}
	// fmt.Println(kvParis)
	response := &kvrpcpb.RawScanResponse{
		Kvs: kvParis,
	}

	return response, nil
}
