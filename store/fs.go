package store

import (
	"fmt"
	"log"
	"sync"
)

import(
	"github.com/timtadh/goiso"
	"github.com/timtadh/fs2"
	"github.com/timtadh/fs2/bptree"
	"github.com/timtadh/fs2/fmap"
)

func assert_ok(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func serializeValue(value *goiso.SubGraph) []byte {
	return value.Serialize()
}

type Fs2BpTree struct {
	g *goiso.Graph
	bf *fmap.BlockFile
	bpt *bptree.BpTree
	mutex sync.Mutex
}

func AnonFs2BpTree(g *goiso.Graph) *Fs2BpTree {
	bf, err := fmap.Anonymous(fmap.BLOCKSIZE)
	assert_ok(err)
	return newFs2BpTree(g, bf)
}

func NewFs2BpTree(g *goiso.Graph, path string) *Fs2BpTree {
	bf, err := fmap.CreateBlockFile(path)
	assert_ok(err)
	return newFs2BpTree(g, bf)
}

func OpenFs2BpTree(g *goiso.Graph, path string) *Fs2BpTree {
	bf, err := fmap.OpenBlockFile(path)
	assert_ok(err)
	bpt, err := bptree.Open(bf)
	assert_ok(err)
	return &Fs2BpTree {
		g: g,
		bf: bf,
		bpt: bpt,
	}
}

func newFs2BpTree(g *goiso.Graph, bf *fmap.BlockFile) *Fs2BpTree {
	bpt, err := bptree.New(bf, -1, -1)
	assert_ok(err)
	return &Fs2BpTree {
		g: g,
		bf: bf,
		bpt: bpt,
	}
}

func (self *Fs2BpTree) Size() int {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.bpt.Size()
}

func (self *Fs2BpTree) Keys() (it BytesIterator) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	raw, err := self.bpt.Keys()
	assert_ok(err)
	it = func() (k []byte, _ BytesIterator) {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		var err error
		k, err, raw = raw()
		assert_ok(err)
		if raw == nil {
			return nil, nil
		}
		return k, it
	}
	return it
}

func (self *Fs2BpTree) Values() (it SGIterator) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	kvi, err := self.bpt.Iterate()
	assert_ok(err)
	raw := self.kvIter(kvi)
	it = func() (v *goiso.SubGraph, _ SGIterator) {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		_, v, raw = raw()
		if raw == nil {
			return nil, nil
		}
		return v, it
	}
	return it
}

func (self *Fs2BpTree) Iterate() (it Iterator) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	kvi, err := self.bpt.Iterate()
	assert_ok(err)
	return self.kvIter(kvi)
}

func (self *Fs2BpTree) Backward() (it Iterator) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	kvi, err := self.bpt.Backward()
	assert_ok(err)
	return self.kvIter(kvi)
}

func (self *Fs2BpTree) Has(key []byte) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	has, err := self.bpt.Has(key)
	assert_ok(err)
	return has
}

func (self *Fs2BpTree) Count(key []byte) int {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	count, err := self.bpt.Count(key)
	assert_ok(err)
	return count
}

func (self *Fs2BpTree) Add(key []byte, sg *goiso.SubGraph) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if len(key) < 0 {
		panic(fmt.Errorf("Key was a bad value %d %v %p\n%p", len(key), key, key, sg))
	}
	if sg == nil {
		panic(fmt.Errorf("sg was a nil %d %v %p\n%p", len(key), key, key, sg))
	}
	value := sg.Serialize()
	if len(value) < 0 {
		panic(fmt.Errorf("Could not serialize sg, %v\n%v\n%v", len(value), sg, value))
	}
	assert_ok(self.bpt.Add(key, value))
	has, err := self.bpt.Has(key)
	assert_ok(err)
	if !has {
		panic("didn't have key just added")
	}
	// assert_ok(self.bf.Sync())
}

func (self *Fs2BpTree) kvIter(kvi fs2.Iterator) (it Iterator) {
	it = func() ([]byte, *goiso.SubGraph, Iterator) {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		var key []byte
		var bytes []byte
		var err error
		key, bytes, err, kvi = kvi()
		// log.Println("kv iter", bytes, err, kvi)
		assert_ok(err)
		if kvi == nil {
			return nil, nil, nil
		}
		sg := goiso.DeserializeSubGraph(self.g, bytes)
		return key, sg, it
	}
	return it
}

func (self *Fs2BpTree) Find(key []byte) (it Iterator) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	kvi, err := self.bpt.Find(key)
	assert_ok(err)
	return self.kvIter(kvi)
}

func (self *Fs2BpTree) Remove(key []byte, where func(*goiso.SubGraph) bool) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.bpt.Remove(key, func(bytes []byte) bool {
		sg := goiso.DeserializeSubGraph(self.g, bytes)
		return where(sg)
	})
}

func (self *Fs2BpTree) Close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	err := self.bf.Close()
	assert_ok(err)
}

func (self *Fs2BpTree) Delete() {
	self.Close()
	if self.bf.Path() != "" {
		err := self.bf.Remove()
		assert_ok(err)
	}
}

