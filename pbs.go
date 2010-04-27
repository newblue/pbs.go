package pbs

import (
	"os"
	"goprotobuf.googlecode.com/hg/proto"
	"crypto/sha256"
	"encoding/hex"
)

type DataStore interface{
	Put(key string, data []byte) (err os.Error)
	Get(key string) (data []byte, err os.Error)
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
	datastore DataStore
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
	ds.datastore = stores[0]
	ds.keys = make(map[string] *PBSDataStore)
	prev := ds
	for _, store := range stores[1:] {
		ns := new(PBSDataStore)
		prev.next = ns
		ns.prev = prev
		ns.datastore = store
		prev = ns
	}
	return ds
}

func (ds *PBSDataStore) Put(value interface{}) (key string, err os.Error) {
	t_key, data, err := DoMarshalling(value)
	if err == nil && ds.keys[t_key] == nil {
		err = ds.datastore.Put(t_key, data)
		if err == nil {
			ds.keys[t_key] = ds
			key = t_key
		}
	}
	return
}

func (ds *PBSDataStore) Get(key string, value interface{}) (err os.Error) {
	if ds.keys[key] != nil {
		data, err := ds.datastore.Get(key)
		if err == nil {
			err = proto.Unmarshal(data, value)
		}
	}
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

func (ds *MemoryStore) Get(key string) (data []byte, err os.Error) {
	if ds.kvstore[key] != nil {

		data = ds.kvstore[key].data
		
		if ds.mruKey != "" && ds.mruKey == key {
			// Pass
		}
		if ds.lruKey != "" && ds.lruKey == key {
			ds.lruKey = ds.kvstore[key].nextLRU
			ds.kvstore[ds.mruKey].nextLRU = key
			ds.kvstore[key].prevLRU = ds.mruKey
			ds.mruKey = key
		} else if key != "" && ds.mruKey != "" {
			prev := ds.kvstore[key].prevLRU
			next := ds.kvstore[key].nextLRU
			
			if prev != "" {
				ds.kvstore[prev].nextLRU = next
			}
			if next != "" {
				ds.kvstore[next].prevLRU = prev
			}
			
			ds.kvstore[ds.mruKey].nextLRU = key
			ds.kvstore[key].prevLRU = ds.mruKey
			ds.mruKey = key
		} else {
			err = os.NewError("Key is empty")
		}
	} else {
		err = os.NewError("Key not found")
	}
	return
}

func (ds *MemoryStore) Delete(key string) (err os.Error) {
	if ds.kvstore[key] != nil {
		if ds.lruKey == key {
			ds.lruKey = ds.kvstore[key].nextLRU
			ds.kvstore[ds.lruKey].prevLRU = ""
		} else if ds.mruKey == key {
			ds.mruKey = ds.kvstore[key].prevLRU
			ds.kvstore[ds.mruKey].nextLRU = ""
		} else {
			prev := ds.kvstore[key].prevLRU
			next := ds.kvstore[key].nextLRU
			
			ds.kvstore[prev].nextLRU = next
			ds.kvstore[next].prevLRU = prev
		}
		ds.kvstore[key] = nil
	} else {
		err = os.NewError("Key not found")
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

