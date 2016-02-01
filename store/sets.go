package store

import (
	"fmt"
	"log"
	"sync"
)

import (
	"github.com/timtadh/data-structures/list"
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/data-structures/set"
	"github.com/timtadh/fs2/bptree"
	"github.com/timtadh/fs2/fmap"
)


func serializeBytesSet(s *set.SortedSet) ([]byte, error) {
	marshal, unmarshal := types.ByteSliceMarshals()
	m := set.NewMSortedSet(s, marshal, unmarshal)
	return m.MarshalBinary()
}

func deserializeBytesSet(bytes []byte) (*set.SortedSet, error) {
	marshal, unmarshal := types.ByteSliceMarshals()
	m := &set.MSortedSet{MSorted: list.MSorted{MList: list.MList{MarshalItem: marshal, UnmarshalItem: unmarshal}}}
	err := m.UnmarshalBinary(bytes)
	if err != nil {
		return nil, err
	}
	return m.SortedSet(), nil
}

type Fs2Sets struct {
	bf *fmap.BlockFile
	bpt *bptree.BpTree
	mutex sync.Mutex
}

func AnonFs2Sets() *Fs2Sets {
	bf, err := fmap.Anonymous(fmap.BLOCKSIZE)
	assert_ok(err)
	return newFs2Sets(bf)
}

func NewFs2Sets(path string) *Fs2Sets {
	bf, err := fmap.CreateBlockFile(path)
	assert_ok(err)
	return newFs2Sets(bf)
}

func OpenFs2Sets(path string) *Fs2Sets {
	bf, err := fmap.OpenBlockFile(path)
	assert_ok(err)
	bpt, err := bptree.Open(bf)
	assert_ok(err)
	return &Fs2Sets {
		bf: bf,
		bpt: bpt,
	}
}

func newFs2Sets(bf *fmap.BlockFile) *Fs2Sets {
	bpt, err := bptree.New(bf, -1, -1)
	assert_ok(err)
	return &Fs2Sets {
		bf: bf,
		bpt: bpt,
	}
}

func (s *Fs2Sets) Has(key []byte) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	has, err := s.bpt.Has(key)
	assert_ok(err)
	return has
}

func (s *Fs2Sets) Put(key []byte, set *set.SortedSet) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	has, err := s.bpt.Has(key)
	assert_ok(err)
	if has {
		assert_ok(s.bpt.Remove(key, func(_ []byte) bool { return true }))
	}
	if len(key) < 0 {
		log.Panic(fmt.Errorf("Key was a bad value %d %v %p\n%p", len(key), key, key, set))
	}
	if set == nil {
		log.Panic(fmt.Errorf("set was a nil %d %v %p\n%p", len(key), key, key, set))
	}
	value, err := serializeBytesSet(set)
	assert_ok(err)
	if len(value) < 0 {
		log.Panic(fmt.Errorf("Could not serialize set, %v\n%v\n%v", len(value), set, value))
	}
	assert_ok(s.bpt.Add(key, value))
	has, err = s.bpt.Has(key)
	assert_ok(err)
	if !has {
		log.Panic("didn't have key just added")
	}
}

func (s *Fs2Sets) Get(key []byte) (set *set.SortedSet) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	assert_ok(s.bpt.DoFind(key, func(_, bytes []byte) error {
		s, err := deserializeBytesSet(bytes)
		if err != nil {
			return err
		}
		set = s
		return nil
	}))
	if set == nil {
		log.Panic(fmt.Errorf("Could not find key %v", key))
	}
	return set
}

