package pbs

import (
	"os"
	"goprotobuf.googlecode.com/hg/proto"
	"crypto/sha256"
	"encoding/hex"
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
	Next() (ds PBSDataStore)
	Prev() (ds PBSDataStore)
}

type MemoryElem struct{
	data []byte
	nextLRU string
	prevLRU string
}	
	
type MemoryStore struct{
	kvstore map[string] *MemoryElem
	mruKey string
	lruKey string
	maxBytes int64
}

type DiskStore struct{
	filename string
	maxBytes int64
}

type NetworkStore struct{
	hostname string
	maxBytes int64
}

type PBSDataStore struct{
	keys map[string] *PBSDataStore
	*DataStore
	next *PBSDataStore
	prev *PBSDataStore
}

func DoMarshalling(value interface{}) (key string, data []byte, err os.Error) {
	data, err = proto.Marshal(value)
	if err == nil {
		h := sha256.New()
		nr, err := h.Write([]byte(data))
		if err == nil && nr == len([]byte(data)) {
			key = hex.EncodeToString(h.Sum())
		}
	}
	return
}

func NewPBSDataStore(stores []DataStore) *PBSDataStore {
	ds := new(PBSDataStore)
	ds.prev = nil
	ds.DataStore = &stores[0]
	ds.keys = make(map[string] *PBSDataStore)
	prev := ds
	for _, store := range stores[1:] {
		ns := new(PBSDataStore)
		prev.next = ns
		ns.prev = prev
		ns.DataStore = &store
		prev = ns
	}
	return ds
}

func (ds *PBSDataStore) Put(value interface{}) (key string, err os.Error) {
	t_key, data, err := DoMarshalling(value)
	if err == nil && ds.keys[t_key] == nil {
		err = ds.DataStore.Put(t_key, data)
		if err == nil {
			ds.keys[t_key] = ds
			key = t_key
		}
	}
	return
}

func (ds *PBSDataStore) Get(key string, value interface{}) (err os.Error) {
	return
}

func (ds *PBSDataStore) Delete(key string) (err os.Error) {
	return
}

func (ds *PBSDataStore) Push(key string) (err os.Error) {
	return
}

func (ds *PBSDataStore) Pull(key string) (err os.Error) {
	return
}

func NewMemoryStore() *MemoryStore {
	ds := new(MemoryStore)
	ds.kvstore = make(map[string] *MemoryElem)
	ds.mruKey = ""
	ds.lruKey = ""
	return ds
}

func (ds *MemoryStore) Put(key string, data []byte) (err os.Error) {
	if ds.kvstore[key] == nil {
		if ds.mruKey == "" && ds.lruKey == "" {
			ds.mruKey = key
			ds.lruKey = key
			elem := new(MemoryElem)
			elem.data = data
			elem.nextLRU = ""
			elem.prevLRU = ""
			ds.kvstore[key] = elem
		} else if ds.kvstore[ds.mruKey] != nil && ds.kvstore[ds.mruKey].nextLRU == "" {
			ds.kvstore[ds.mruKey].nextLRU = key
			elem := new(MemoryElem)
			elem.data = data
			elem.nextLRU = ""
			elem.prevLRU = ds.mruKey
			ds.kvstore[key] = elem
			ds.mruKey = key
		} else {
			err = os.NewError("Should never happen")
		}
	} else {
		err = os.NewError("Key already exists")
	}
	return
}

func (ds *MemoryStore) Get(key string, value interface{}) (err os.Error) {
	if ds.kvstore[key] != nil {

		err = proto.Unmarshal(ds.kvstore[key].data, value)

		if err == nil {

			if ds.lruKey == key {
				ds.lruKey = ds.kvstore[key].nextLRU
				ds.kvstore[ds.mruKey].nextLRU = key
				ds.kvstore[key].prevLRU = ds.mruKey
				ds.mruKey = key
			} else if ds.mruKey != key {
				prev := ds.kvstore[key].prevLRU
				next := ds.kvstore[key].nextLRU

				ds.kvstore[prev].nextLRU = next
				ds.kvstore[next].prevLRU = prev

				ds.kvstore[ds.mruKey].nextLRU = key
				ds.kvstore[key].prevLRU = ds.mruKey
				ds.mruKey = key
			} // Otherwise key == ds.mruKey, do nothing
		}
	}
	return
}

func NewDiskStore(filename string) *DiskStore {
	ds := new(DiskStore)
	ds.filename = filename
	return ds
}

func NewNetworkStore(hostname string) *NetworkStore {
	ds := new(NetworkStore)
	ds.hostname = hostname
	return ds
}

