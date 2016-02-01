package mine

import (
	"bytes"
	"hash/fnv"
)

import (
	"github.com/timtadh/graple/store"
	"github.com/timtadh/goiso"
)

type labelGraph struct {
	label []byte
	sg *goiso.SubGraph
}

type partition []*goiso.SubGraph
type partitionIterator func() (part store.Iterator, next partitionIterator)

type CollectAction func(lg *labelGraph)
type Collector func(<-chan *labelGraph, chan<- bool)

type Collectors interface {
	close()
	delete()
	send(sg *goiso.SubGraph)
	size() int
	keys() (kit store.BytesIterator)
	partsCh() <-chan store.Iterator
	Find(key []byte) (pit store.Iterator)
}

func (m *RandomWalkMiner) makeCollectors(N int) Collectors {
	// return MakeSerCollector(m.StoreMaker, m.collector)
	return MakeParHashCollector(N, m.MakeStore, m.MakeUnique, func (tree store.SubGraphs, unique store.UniqueIndex) Collector {
		return BasicCollector(CheckUnique(unique, TreeAdd(tree)))
	})
}


func TreeAdd(tree store.SubGraphs) CollectAction {
	return func(lg *labelGraph) {
		tree.Add(lg.label, lg.sg)
	}
}

func CheckCount(tree store.SubGraphs, limit int, action CollectAction) CollectAction {
	return func(lg *labelGraph) {
		if tree.Count(lg.label) <= limit {
			action(lg)
		}
	}
}

func CheckUnique(unique store.UniqueIndex, action CollectAction) CollectAction {
	return func(lg *labelGraph) {
		if !unique.Has(lg.sg) {
			unique.Add(lg.sg)
			action(lg)
		}
	}
}

func BasicCollector(action CollectAction) Collector {
	return func(in <-chan *labelGraph, done chan<- bool) {
		for lg := range in {
			action(lg)
		}
		done<-true
	}
}

type SerialCollector struct {
	tree store.SubGraphs
	ch chan *labelGraph
	done chan bool
}

func MakeSerCollector(makeStore func()store.SubGraphs, makeCollector func(tree store.SubGraphs) Collector) Collectors {
	done := make(chan bool)
	tree := makeStore()
	ch := make(chan *labelGraph, 1)
	go makeCollector(tree)(ch, done)
	return &SerialCollector{tree, ch, done}
}

func (c *SerialCollector) close() {
	close(c.ch)
	<-c.done
}

func (c *SerialCollector) delete() {
	c.tree.Delete()
}

func (c *SerialCollector) send(sg *goiso.SubGraph) {
	c.ch<-&labelGraph{sg.ShortLabel(), sg}
}

func (c *SerialCollector) size() int {
	return c.tree.Size()
}

func (c *SerialCollector) keys() (kit store.BytesIterator) {
	return c.tree.Keys()
}

func (c *SerialCollector) partsCh() <-chan store.Iterator {
	out := make(chan store.Iterator, 100)
	go func() {
		for k, keys := c.keys()(); keys != nil; k, keys = keys() {
			out <- c.Find(k)
		}
		close(out)
	}()
	return out
}

func (c *SerialCollector) Find(key []byte) (pit store.Iterator) {
	return c.tree.Find(key)
}

type ParHashCollector struct {
	graphs []store.SubGraphs
	unique []store.UniqueIndex
	chs []chan<- *labelGraph
	done chan bool
}

func MakeParHashCollector(
	N int,
	makeStore func()store.SubGraphs,
	makeIndex func()store.UniqueIndex,
	makeCollector func(graphs store.SubGraphs, unique store.UniqueIndex) Collector,
) (
	Collectors,
){
	graphs := make([]store.SubGraphs, 0, N)
	unique := make([]store.UniqueIndex, 0, N)
	chs := make([]chan<- *labelGraph, 0, N)
	done := make(chan bool)
	for i := 0; i < N; i++ {
		tree := makeStore()
		idx := makeIndex()
		ch := make(chan *labelGraph, 1)
		graphs = append(graphs, tree)
		unique = append(unique, idx)
		chs = append(chs, ch)
		go makeCollector(tree, idx)(ch, done)
	}
	return &ParHashCollector{graphs, unique, chs, done}
}

func (c *ParHashCollector) close() {
	for _, ch := range c.chs {
		close(ch)
	}
	for i := 0; i < len(c.chs); i++ {
		<-c.done
	}
}

func (c *ParHashCollector) delete() {
	for _, bpt := range c.graphs {
		bpt.Delete()
	}
	for _, idx := range c.unique {
		idx.Delete()
	}
}

func (c *ParHashCollector) size() int {
	sum := 0
	for _, tree := range c.graphs {
		sum += tree.Size()
	}
	return sum
}

func (c *ParHashCollector) partsCh() <-chan store.Iterator {
	out := make(chan store.Iterator)
	done := make(chan bool)
	for _, tree := range c.graphs {
		go func(tree store.SubGraphs) {
			for part, next := c.makePartitions(tree)(); next != nil; part, next = next() {
				out <- part
			}
			done <- true
		}(tree)
	}
	go func() {
		for _ = range c.graphs {
			<-done
		}
		close(out)
		close(done)
	}()
	return out
}

func (c *ParHashCollector) send(sg *goiso.SubGraph) {
	label := sg.ShortLabel()
	idx := hash(label) % len(c.chs)
	c.chs[idx] <- &labelGraph{label, sg}
}

func (c *ParHashCollector) makePartitions(sgs store.SubGraphs) (p_it partitionIterator) {
	keys := sgs.Keys()
	p_it = func() (part store.Iterator, next partitionIterator) {
		var key []byte
		key, keys = keys()
		if keys == nil {
			return nil, nil
		}
		return bufferedIterator(sgs.Find(key), 10), p_it
	}
	return p_it
}

func (c *ParHashCollector) Find(key []byte) (pit store.Iterator) {
	idx := hash(key) % len(c.chs)
	t := c.graphs[idx]
	return bufferedIterator(t.Find(key), 10)
}

func (c *ParHashCollector) keys() (kit store.BytesIterator) {
	return keysFromTrees(c.graphs)
}

type ParCollector struct {
	trees []store.SubGraphs
	chs []chan<- *labelGraph
	done chan bool
}

func MakeParCollector(N int, makeStore func()store.SubGraphs, collector func(store.SubGraphs, <-chan *labelGraph, chan<- bool)) Collectors {
	trees := make([]store.SubGraphs, 0, N)
	chs := make([]chan<- *labelGraph, 0, N)
	done := make(chan bool)
	for i := 0; i < N; i++ {
		tree := makeStore()
		ch := make(chan *labelGraph, 1)
		trees = append(trees, tree)
		chs = append(chs, ch)
		go collector(tree, ch, done)
	}
	return &ParCollector{trees, chs, done}
}

func (c *ParCollector) close() {
	for _, ch := range c.chs {
		close(ch)
	}
	for i := 0; i < len(c.chs); i++ {
		<-c.done
	}
}

func (c *ParCollector) delete() {
	for _, bpt := range c.trees {
		bpt.Delete()
	}
}

func hash(bytes []byte) int {
	h := fnv.New32a()
	h.Write(bytes)
	return int(h.Sum32())
}

/*
*/

func (c *ParCollector) partsCh() <-chan store.Iterator {
	out := make(chan store.Iterator, 100)
	go func() {
		for k, keys := c.keys()(); keys != nil; k, keys = keys() {
			out <- c.Find(k)
		}
		close(out)
	}()
	return out
}

func (c *ParCollector) send(sg *goiso.SubGraph) {
	label := sg.ShortLabel()
	lg := &labelGraph{label, sg}
	bkt := hash(label) % len(c.chs)
	next := bkt
	for i := 0; i < len(c.chs); i++ {
		select {
		case c.chs[next]<-lg:
			return
		default:
			next = (next + 1) % len(c.chs)
		}
	}
	c.chs[bkt]<-lg
}


func (c *ParCollector) Find(key []byte) (pit store.Iterator) {
	its := make([]store.Iterator, len(c.trees))
	for i, tree := range c.trees {
		its[i] = bufferedIterator(tree.Find(key), 10)
	}
	j := 0
	pit = func() (k []byte, sg *goiso.SubGraph, _ store.Iterator) {
		for j < len(its) {
			if its[j] == nil {
				j++
			} else {
				k, sg, its[j] = its[j]()
				if its[j] != nil {
					return k, sg, pit
				}
			}
		}
		return nil, nil, nil
	}
	return pit
}

func (c *ParCollector) keys() (kit store.BytesIterator) {
	return keysFromTrees(c.trees)
}

func (c *ParCollector) size() int {
	sum := 0
	for _, tree := range c.trees {
		sum += tree.Size()
	}
	return sum
}

func keysFromTrees(trees []store.SubGraphs) (kit store.BytesIterator) {
	its := make([]store.BytesIterator, len(trees))
	peek := make([][]byte, len(trees))
	for i, tree := range trees {
		its[i] = tree.Keys()
		peek[i], its[i] = its[i]()
	}
	getMin := func() int {
		min := -1
		for i := range peek {
			if peek[i] == nil {
				continue
			}
			if min == -1 || bytes.Compare(peek[i], peek[min]) <= 0 {
				min = i
			}
		}
		return min
	}
	var last []byte = nil
	kit = func() (item []byte, _ store.BytesIterator) {
		item = last
		for bytes.Equal(item, last) {
			min := getMin()
			if min == -1 {
				return nil, nil
			}
			item = peek[min]
			peek[min], its[min] = its[min]()
		}
		last = item
		return item, kit
	}
	return kit
}

