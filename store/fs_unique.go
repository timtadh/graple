package store

import (
	"fmt"
	"sync"
)

import(
	"github.com/timtadh/goiso"
	"github.com/timtadh/fs2/bptree"
	"github.com/timtadh/fs2/fmap"
)


type Fs2UniqueIndex struct {
	g *goiso.Graph
	bf *fmap.BlockFile
	bpt *bptree.BpTree
	mutex sync.Mutex
}

func AnonFs2UniqueIndex(g *goiso.Graph) *Fs2UniqueIndex {
	bf, err := fmap.Anonymous(fmap.BLOCKSIZE)
	assert_ok(err)
	return newFs2UniqueIndex(g, bf)
}

func NewFs2UniqueIndex(g *goiso.Graph, path string) *Fs2UniqueIndex {
	bf, err := fmap.CreateBlockFile(path)
	assert_ok(err)
	return newFs2UniqueIndex(g, bf)
}

func newFs2UniqueIndex(g *goiso.Graph, bf *fmap.BlockFile) *Fs2UniqueIndex {
	bpt, err := bptree.New(bf, -1, 0)
	assert_ok(err)
	return &Fs2UniqueIndex {
		g: g,
		bf: bf,
		bpt: bpt,
	}
}

func (self *Fs2UniqueIndex) Close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	err := self.bf.Close()
	assert_ok(err)
}

func (self *Fs2UniqueIndex) Delete() {
	self.Close()
	if self.bf.Path() != "" {
		err := self.bf.Remove()
		assert_ok(err)
	}
}

func (self *Fs2UniqueIndex) Has(sg *goiso.SubGraph) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	key := sg.Serialize()
	if len(key) < 0 {
		panic(fmt.Errorf("Could not serialize sg, %v\n%v\n%v", len(key), sg, key))
	}
	has, err := self.bpt.Has(key)
	assert_ok(err)
	return has
}

func (self *Fs2UniqueIndex) Add(sg *goiso.SubGraph) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if sg == nil {
		panic(fmt.Errorf("sg was a nil\n%p",  sg))
	}
	key := sg.Serialize()
	if len(key) < 0 {
		panic(fmt.Errorf("Could not serialize sg, %v\n%v\n%v", len(key), sg, key))
	}
	assert_ok(self.bpt.Add(key, []byte{}))
	has, err := self.bpt.Has(key)
	assert_ok(err)
	if !has {
		panic("didn't have key just added")
	}
	// assert_ok(self.bf.Sync())
}

