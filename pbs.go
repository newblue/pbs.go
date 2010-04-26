package pbs

import (
	"os"
	"goprotobuf.googlecode.com/hg/proto"
	"crypto/sha256"
	"encoding/hex"
)

type PBSStore interface{
	Put(value interface{}) (key string, err os.Error)
	Get(key string, value interface{}) (err os.Error)
	Delete(key string) (err os.Error)
	Push(key string) (err os.Error)
	Pull(key String) (err os.Error)
}
		
type PBSMemoryStore struct{
	kvstore map[string][]byte
}

type PBSDiskStore struct{
	keys []string
	filename string
}

type PBSNetworkStore struct{
	keys []string
	hostname string
}

type PBSDataStore struct{
	stores []PBSStore
}

func NewPBSDataStore(stores []PBSStore) *PBSDataStore {
	ds := new(PBSDataStore)
	ds.stores = stores
	return ds
}

func NewPBSMemoryStore() *PBSMemoryStore {
	ds := new(PBSMemoryStore)
	ds.kvstore = make(map[string][]byte)
	return ds
}

func NewPBSDiskStore(filename string) *PBSDiskStore {
	ds := new(PBSDiskStore)
	ds.keys = make([]string)
	ds.filename = filename
	return ds
}

func NewPBSNetworkStore(hostname string) *PBSNetworkStore {
	ds := new(PBSNetworkStore)
	ds.keys = make([]string)
	ds.hostname = hostname
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
