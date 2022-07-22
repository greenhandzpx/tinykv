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
	response, err := reader.GetCF(req.GetCf(), req.GetKey())
	rawResponse := &kvrpcpb.RawGetResponse{Value: response}
	if response == nil {
		rawResponse.NotFound = true
	} else {
		rawResponse.NotFound = false
	}
	return rawResponse, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := storage.Modify{
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.GetCf(),
		},
	}
	modifies := []storage.Modify{modify}
	err := server.storage.Write(nil, modifies)
	// not sure here
	rawResponse := &kvrpcpb.RawPutResponse{}
	return rawResponse, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modify := storage.Modify{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf:  req.GetCf(),
		},
	}
	//fmt.Printf("delete cf:%v\n", req.GetCf())
	modifies := []storage.Modify{modify}
	err := server.storage.Write(nil, modifies)
	// not sure here
	rawResponse := &kvrpcpb.RawDeleteResponse{}
	return rawResponse, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	var kvs []*kvrpcpb.KvPair

	if !iter.Valid() {
		fmt.Printf("iter not valid\n")
	}
	for iter.Valid() && string(iter.Item().Key()) != string(req.StartKey) {
		fmt.Printf("iter: key:%v\n", iter.Item().Key())
		iter.Next()
	}
	if iter.Valid() {
		fmt.Printf("find the start key:%v\n", iter.Item().Key())
	}
	cnt := uint32(0)
	for iter.Valid() && cnt < req.Limit {
		value, err := iter.Item().Value()
		if err != nil {
			return nil, err
		}
		kv := &kvrpcpb.KvPair{
			Key:   iter.Item().Key(),
			Value: value,
		}
		kvs = append(kvs, kv)
		iter.Next()
		cnt++
	}
	rawResponse := &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}
	return rawResponse, nil
}
