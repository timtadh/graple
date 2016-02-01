package mine

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"runtime"
	"runtime/debug"
)

import (
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/data-structures/set"
	"github.com/timtadh/goiso"
	"github.com/timtadh/graple/store"
)


type RandomWalkMiner struct {
	Graph *goiso.Graph
	Support int
	MinVertices int
	SampleSize int
	PLevel int
	Report chan []byte
	MakeStore func() store.SubGraphs
	MakeUnique func() store.UniqueIndex
	MakeSetsMap func() store.SetsMap
	startingPoints *set.SortedSet // source of memory
	AllEmbeddings Collectors
	extended store.SetsMap // source of memory
	                               // types.ByteSlice, set.SortedSet
	supportedExtensions store.SetsMap // source of memory
	                               // types.ByteSlice, set.SortedSet
	Tries int
}


func RandomWalk(
	G *goiso.Graph,
	support, minVertices, sampleSize int,
	memProf io.Writer,
	makeStore func() store.SubGraphs,
	makeUnique func() store.UniqueIndex,
	makeSetsMap func() store.SetsMap,
) (
	m *RandomWalkMiner,
) {
	m = &RandomWalkMiner{
		Graph: G,
		Support: support,
		MinVertices: minVertices,
		SampleSize: sampleSize,
		PLevel: runtime.NumCPU(),
		Report: make(chan []byte),
		MakeStore: makeStore,
		MakeUnique: makeUnique,
		MakeSetsMap: makeSetsMap,
		extended: makeSetsMap(),
		supportedExtensions: makeSetsMap(),
	}
	go func() {
		m.Tries = m.sample(sampleSize)
	}()
	return m
}

type SparseEntry struct {
	Row, Col int
	Value float64
	Inverse int
}

type Sparse struct {
	Rows, Cols int
	Entries []*SparseEntry
}

func (m *RandomWalkMiner) PrMatrices(sg *goiso.SubGraph) (vp int, Q, R, u Sparse, err error) {
	defer func() {
		if e := recover(); e != nil {
			stack := string(debug.Stack())
			err = fmt.Errorf("%v\n%v", e, stack)
		}
	}()
	lattice := sg.Lattice()
	log.Printf("lattice size %d %v", len(lattice.V), sg.Label())
	p := m.probabilities(lattice)
	log.Println("got transistion probabilities", p)
	vp = m.startingPoints.Size()
	Q = Sparse{
		Rows: len(lattice.V)-1,
		Cols: len(lattice.V)-1,
		Entries: make([]*SparseEntry, 0, len(lattice.V)-1),
	}
	R = Sparse{
		Rows: len(lattice.V)-1,
		Cols: 1,
		Entries: make([]*SparseEntry, 0, len(lattice.V)-1),
	}
	u = Sparse{
		Rows: 1,
		Cols: len(lattice.V)-1,
		Entries: make([]*SparseEntry, 0, len(lattice.V)-1),
	}
	for i, x := range lattice.V {
		if len(x.V) == 1 && len(x.E) == 0 && i < len(lattice.V)-1 {
			u.Entries = append(u.Entries, &SparseEntry{0, i, 1.0/float64(vp), vp})
		}
	}
	for _, e := range lattice.E {
		if e.Targ >= len(lattice.V)-1 {
			R.Entries = append(R.Entries, &SparseEntry{e.Src, 0, 1.0/float64(p[e.Src]), p[e.Src]})
		} else {
			Q.Entries = append(Q.Entries, &SparseEntry{e.Src, e.Targ, 1.0/float64(p[e.Src]), p[e.Src]})
		}
	}
	return vp, Q, R, u, nil
}

func (m *RandomWalkMiner) probabilities(lattice *goiso.Lattice) []int {
	P := make([]int, len(lattice.V))
	// log.Println(startingPoints, "start")
	for i, sg := range lattice.V {
		key := sg.ShortLabel()
		part := m.partition(key) // READS
		// This is incorrect, I am doing multiple extensions of the SAME graph
		// ending up with wierdness. I need to make sure I only extend a graph
		// ONCE.
		keys := m.extensions(part) // WRITES
		count := m.supportedKeys(key, keys).Size() // READS
		if i + 1 == len(lattice.V) {
			P[i] = -1
		} else if count == 0 {
			// this case can happen occasionally, we need to ensure the
			// absorbing node will still be reachable
			log.Println("err", keys.Size(), part[0].Label())
			P[i] = 1
		} else {
			P[i] = count
			// log.Println(P[i], part[0].Label())
		}
	}
	return P
}

func (m *RandomWalkMiner) sample(size int) (tries int) {
	if m.AllEmbeddings == nil {
		m.AllEmbeddings, m.startingPoints = m.initial()
	}
	for i := 0; i < size; i++ {
		retry: for {
			tries++
			part := m.walk()
			if len(part) < m.Support {
				log.Println("found mfsg but it did not have enough support")
				continue retry
			} else if len(part[0].V) < m.MinVertices {
				log.Println("found mfsg but it was too small")
				continue retry
			}
			label := part[0].ShortLabel()
			for _, sg := range part {
				if !bytes.Equal(label, sg.ShortLabel()) {
					log.Println("different subgraphs in part")
					continue retry
				}
			}
			log.Println("found mfsg", part[0].Label())
			m.Report<-label
			break retry
		}
	}
	close(m.Report)
	return tries
}

func (m *RandomWalkMiner) walk() partition {
	node := m.randomInitialPartition()
	exts := m.extensions(node)
	// log.Printf("start node (%v) (%d) %v", exts.Size(), len(node), node[0].Label())
	next := m.randomPartition(node[0].ShortLabel(), exts)
	for len(next) >= m.Support {
		node = next
		exts = m.extensions(node)
		// log.Printf("cur node (%v) (%d) %v", exts.Size(), len(node), node[0].Label())
		next = m.randomPartition(node[0].ShortLabel(), exts)
		if len(next) >= m.Support && len(next[0].E) == len(node[0].E) {
			break
		}
	}
	return node
}

func (m *RandomWalkMiner) initial() (Collectors, *set.SortedSet) {
	groups := m.makeCollectors(m.PLevel)
	for i := range m.Graph.V {
		v := &m.Graph.V[i]
		if m.Graph.ColorFrequency(v.Color) >= m.Support {
			sg, _ := m.Graph.VertexSubGraph(v.Idx)
			groups.send(sg)
		}
	}
	startingPoints := set.NewSortedSet(10)
	for key, next := groups.keys()(); next != nil; key, next = next() {
		startingPoints.Add(types.ByteSlice(key))
	}
	return groups, startingPoints
}

func (m *RandomWalkMiner) extend(sgs []*goiso.SubGraph, send func(*goiso.SubGraph)) {
	type extension struct {
		sg *goiso.SubGraph
		e *goiso.Edge
	}
	extend := make(chan extension)
	extended := make(chan *goiso.SubGraph)
	done := make(chan bool)
	WORKERS := m.PLevel
	for i := 0; i < WORKERS; i++ {
		go func() {
			for ext := range extend {
				nsg, _ := ext.sg.EdgeExtend(ext.e)
				extended<-nsg
			}
			done <-true
		}()
	}
	go func() {
		for i := 0; i < WORKERS; i++ {
			<-done
		}
		close(extended)
		close(done)
	}()
	go func() {
		add := func(sg *goiso.SubGraph, e *goiso.Edge) {
			if m.Graph.ColorFrequency(e.Color) < m.Support {
				return
			} else if m.Graph.ColorFrequency(m.Graph.V[e.Src].Color) < m.Support {
				return
			} else if m.Graph.ColorFrequency(m.Graph.V[e.Targ].Color) < m.Support {
				return
			}
			if !sg.HasEdge(goiso.ColoredArc{e.Arc, e.Color}) {
				extend<-extension{sg, e}
			}
		}
		for i := range sgs[0].V {
			u := &sgs[0].V[i]
			for _, sg := range sgs {
				if u.Idx >= len(sg.V) {
					continue
				}
				v := sg.V[u.Idx]
				for _, e := range m.Graph.Kids[v.Id] {
					add(sg, e)
				}
				for _, e := range m.Graph.Parents[v.Id] {
					add(sg, e)
				}
			}
		}
		close(extend)
	}()
	for esg := range extended {
		send(esg)
	}
}

func (m *RandomWalkMiner) extensions(sgs []*goiso.SubGraph) *set.SortedSet {
	if len(sgs) == 0 {
		return set.NewSortedSet(10)
	}
	label := types.ByteSlice(sgs[0].ShortLabel())
	if m.extended.Has(label) {
		keys := m.extended.Get(label)
		return keys
	}
	keys := set.NewSortedSet(10)
	m.extend(sgs, func(sg *goiso.SubGraph) {
		m.AllEmbeddings.send(sg)
		keys.Add(types.ByteSlice(sg.ShortLabel()))
	})
	m.extended.Put(label, keys)
	return keys
}

func (m *RandomWalkMiner) supportedKeys(from []byte, keys *set.SortedSet) *set.SortedSet {
	key := types.ByteSlice(from)
	if m.supportedExtensions.Has(key) {
		supKeys := m.supportedExtensions.Get(key)
		return supKeys
	}
	keysCh := make(chan []byte)
	partKeys := make(chan []byte)
	done := make(chan bool)
	for i := 0; i < m.PLevel; i++ {
		go func() {
			for key := range keysCh {
				if len(m.partition(key)) >= m.Support {
					partKeys<-key
				}
			}
			done<-true
		}()
	}
	go func() {
		for k, next := keys.Items()(); next != nil; k, next = next() {
			keysCh<-[]byte(k.(types.ByteSlice))
		}
		close(keysCh)
	}()
	go func() {
		for i := 0; i < m.PLevel; i++ {
			<-done
		}
		close(partKeys)
		close(done)
	}()
	supKeys := set.NewSortedSet(10)
	for partKey := range partKeys {
		supKeys.Add(types.ByteSlice(partKey))
	}
	m.supportedExtensions.Put(key, supKeys)
	return supKeys
}

func (m *RandomWalkMiner) randomPartition(from []byte, keys *set.SortedSet) partition {
	supKeys := m.supportedKeys(from, keys)
	if supKeys.Size() <= 0 {
		return nil
	}
	key, err := supKeys.Random()
	if err != nil {
		log.Fatal(err)
	}
	return m.partition(key.(types.ByteSlice))
}

func (m *RandomWalkMiner) randomInitialPartition() partition {
	key, err := m.startingPoints.Random()
	if err != nil {
		log.Fatal(err)
	}
	return m.partition(key.(types.ByteSlice))
}

func (m *RandomWalkMiner) partition(key []byte) partition {
	part := make(partition, 0, 10)
	for _, e, next := m.AllEmbeddings.Find(key)(); next != nil; _, e, next = next() {
		part = append(part, e)
	}
	return MinimumImageSupport(part)
}

