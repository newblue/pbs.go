package pbs

import (
	"os"
	"goprotobuf.googlecode.com/hg/proto"
	"crypto/sha256"
	"encoding/hex"
)

type DataStore interface{
	Put(value interface{}) (key string, err os.Error)
	Get(key string, value interface{}) (err os.Error)
}

type PBSDataStore struct{
	kvstore map[string][]byte
}

func NewPBS() *PBSDataStore {
	ds := new(PBSDataStore)
	ds.kvstore = make(map[string][]byte)
	return ds
}

func (ds *PBSDataStore) Put(value interface{}) (key string, err os.Error) {
	data, err := proto.Marshal(value)
	if err == nil {
		h := sha256.New()
		nr, err := h.Write([]byte(data))
		if err == nil && nr == len([]byte(data)) {
			t_key := hex.EncodeToString(h.Sum())
			if ds.kvstore[t_key] == nil {
				ds.kvstore[t_key] = data
				key = t_key
			} else {
				err = os.NewError("Key already exists")
			}
		}
	}
	return
}

func (ds *PBSDataStore) Get(key string, value interface{}) (err os.Error) {
	if ds.kvstore[key] != nil {
		err = proto.Unmarshal(ds.kvstore[key], value)
	}
	return
}
