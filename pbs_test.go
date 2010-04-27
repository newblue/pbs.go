package pbs

import (
	"testing"
	"goprotobuf.googlecode.com/hg/proto"
)

func TestPBS(t *testing.T){

	memStore := NewMemoryStore()
	stores := [1]DataStore { memStore }
	ds := NewPBSDataStore( &stores )

	test := &Test {
	Label: proto.String("hello"),
	Type: proto.Int32(17),
	Optionalgroup: &Test_OptionalGroup {
		RequiredField: proto.String("good bye"),
		},
	}
	
	key, err := ds.Put(test)
	if err != nil {
		t.Fatalf("Put failed: %q", err.String())
	}
	t.Logf("Key: %q", key)

	test2 := NewTest()
	err2 := ds.Get(key, test2)

	if err2 != nil {
		t.Fatalf("Get failed: %q", err.String())
	}
	t.Logf("Value: %q", *test2.Label)
}

