package pbs

import (
	"os"
	"goprotobuf.googlecode.com/hg/proto"
	"crypto/sha256"
	"encoding/hex"
	"container/heap"
)

type DataStore interface{
	Put(key string, data []byte) (err os.Error)
	Get(key string, data []byte) (err os.Error)
	Delete(key string) (err os.Error)
}

type PBSStorage interface{
	Put(value interface{}) (key string, err os.Error)
	Get(key string, value interface{}) (err os.Error)
	Delete(key string) (err os.Error)
	Expire(key string) (err os.Error)
}

type MemoryElem struct{
	data []byte
	nextLRU string
	prevLRU string
}	
	
type MemoryStore struct{
	kvstore map[string]MemoryElem
	mruKey string
	lruKey string
	maxBytes int64
	PBSDataStore
}

type DiskStore struct{
	filename string
	maxBytes int64
	PBSDataStore
}

type NetworkStore struct{
	hostname string
	maxBytes int64
	PBSDataStore
}

type PBSDataStore struct{
	keys map[string] PBSDataStore
	DataStore
	next PBSDataStore
	prev PBSDataStore
}

func DoMarshalling(value interface{}) (key string, data byte[], err os.Error) {
	data, err := proto.Marshal(value)
	if err == nil {
		h := sha256.New()
		nr, err := h.Write([]byte(data))
		if err == nil && nr == len([]byte(data)) {
			key := hex.EncodeToString(h.Sum())
		}
	}
	return
}

func NewPBSDataStore(stores []DataStore) *PBSDataStore {
	ds := new(PBSDataStore)
	ds.prev = nil
	ds.DataStore = stores[0]
	stores[0].PBSDataStore = ds
	ds.keys = make(map[string] PBSDataStore)
	prev = ds
	for store in stores[1:] {
		ns := new(PBSDataStore)
		prev.next = ns
		ns.prev = prev
		ns.DataStore = store
		store.PBSDataStore = ns
		prev = ns
	}
	return ds
}

func (ds *PBSDataStore) Put(value interface{}) (key string, err os.Error) {
	t_key, data, err := DoMarshalling(value)
	if err == nil && ds.keys[t_key] == nil {
		err = ds.PBSStore.Put(t_key, data)
		if err == nil {
			ds.keys[t_key] = ds.PBSStore
			key = t_key
		}
	}
	return
}

func (ds *PBSDataStore) Get(key string, value interface{}) (err os.Error) {
	
}

func (ds *PBSDataStore) Delete(key string) (err os.Error) {
	
}

func (ds *PBSDataStore) Push(key string) (err os.Error) {
	
}

func (ds *PBSDataStore) Pull(key string) (err os.Error) {

}

func NewMemoryStore() *MemoryStore {
	ds := new(MemoryStore)
	ds.kvstore = make(map[string][]byte)
	return ds
}

func (ds *PBSMemoryStore) Put(value interface{}) (key string, err os.Error) {
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

func (ds *PBSMemoryStore) Get(key string, value interface{}) (err os.Error) {
	if ds.kvstore[key] != nil {
		err = proto.Unmarshal(ds.kvstore[key], value)
	}
	return
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

