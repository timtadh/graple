package store

import (
	"encoding/binary"
)

import (
	"github.com/timtadh/goiso"
	"github.com/timtadh/data-structures/set"
)

type ParentedSg struct {
	Parent []byte
	Sg *goiso.SubGraph
}

func NewParentedSg(parent []byte, sg *goiso.SubGraph) *ParentedSg {
	return &ParentedSg{
		Parent: parent,
		Sg: sg,
	}
}

func (psg *ParentedSg) Serialize() []byte {
	sg := psg.Sg.Serialize()
	bytes := make([]byte, 8 + len(psg.Parent) + len(sg))
	binary.LittleEndian.PutUint32(bytes[0:4], uint32(len(psg.Parent)))
	binary.LittleEndian.PutUint32(bytes[4:8], uint32(len(sg)))
	off := 8
	{
		s := off
		e := s + len(psg.Parent)
		copy(bytes[s:e], psg.Parent)
	}
	off += len(psg.Parent)
	{
		s := off
		e := s + len(sg)
		copy(bytes[s:e], sg)
	}
	return bytes
}

func DeserializeParentedSg(g *goiso.Graph, bytes []byte) *ParentedSg {
	lenP := binary.LittleEndian.Uint32(bytes[0:4])
	lenSG := binary.LittleEndian.Uint32(bytes[4:8])
	off := 8
	parent := make([]byte, lenP)
	{
		s := off
		e := s + len(parent)
		copy(parent, bytes[s:e])
	}
	off += len(parent)
	var sg *goiso.SubGraph
	{
		s := off
		e := s + int(lenSG)
		sg = goiso.DeserializeSubGraph(g, bytes[s:e])
	}
	return NewParentedSg(parent, sg)
}

type SubGraphsIterable interface {
	Keys() BytesIterator
	Values() SGIterator
	Iterate() Iterator
	Backward() Iterator
}

type Findable interface {
	Find(key []byte) Iterator
}

type SubGraphsOperable interface {
	Findable
	Has(key []byte) bool
	Count(key []byte) int
	Add(key []byte, value *goiso.SubGraph)
	Remove(key []byte, where func(*goiso.SubGraph) bool) error
}

type SubGraphs interface {
	SubGraphsIterable
	SubGraphsOperable
	Size() int
	Delete()
}

type SGIterator func() (*goiso.SubGraph, SGIterator)
type BytesIterator func() ([]byte, BytesIterator)
type Iterator func() ([]byte, *goiso.SubGraph, Iterator)


type UniqueIndexOperable interface {
	Has(value *goiso.SubGraph) bool
	Add(value *goiso.SubGraph)
}

type UniqueIndex interface {
	UniqueIndexOperable
	Delete()
}

type SetsMap interface {
	Has(key []byte) bool
	Put(key []byte, set *set.SortedSet)
	Get(key []byte) (set *set.SortedSet)
}

