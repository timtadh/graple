package mine

import (
	"sort"
)

import (
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/data-structures/set"
	"github.com/timtadh/goiso"
)


type isoGroupWithSet struct {
	sg *goiso.SubGraph
	vertices *set.SortedSet
}

type sortableIsoGroup []*isoGroupWithSet
func (s sortableIsoGroup) Len() int { return len(s) }
func (s sortableIsoGroup) Less(i, j int) bool { return s[i].vertices.Less(s[j].vertices) }
func (s sortableIsoGroup) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func VertexSet(sg *goiso.SubGraph) *set.SortedSet {
	s := set.NewSortedSet(len(sg.V))
	for _, v := range sg.V {
		if err := s.Add(types.Int(v.Id)); err != nil {
			panic(err)
		}
	}
	return s
}

func VertexSets(sgs partition) []*set.MapSet {
	if len(sgs) == 0 {
		return make([]*set.MapSet, 0)
	}
	sets := make([]*set.MapSet, 0, len(sgs[0].V))
	for i := range sgs[0].V {
		set := set.NewMapSet(set.NewSortedSet(len(sgs)))
		for j, sg := range sgs {
			id := types.Int(sg.V[i].Id)
			if !set.Has(id) {
				set.Put(id, j)
			}
		}
		sets = append(sets, set)
	}
	return sets
}

func MinimumImageSupport(sgs partition) partition {
	if len(sgs) <= 1 {
		return sgs
	}
	sets := VertexSets(sgs)
	arg, size := min(srange(len(sets)), func(i int) float64 {
		return float64(sets[i].Size())
	})
	supported := make(partition, 0, int(size)+1)
	for sgIdx, next := sets[arg].Values()(); next != nil; sgIdx, next = next() {
		idx := sgIdx.(int)
		supported = append(supported, sgs[idx])
	}
	return supported
}

func (m *RandomWalkMiner) nonOverlapping(sgs partition) partition {
	group := make(sortableIsoGroup, 0, len(sgs))
	for _, sg := range sgs {
		group = append(group, &isoGroupWithSet{
			sg: sg,
			vertices: VertexSet(sg),
		})
	}
	sort.Sort(group)
	vids := set.NewSortedSet(10)
	non_overlapping := make(partition, 0, len(sgs))
	for _, sg := range group {
		s := sg.vertices
		if !vids.Overlap(s) {
			non_overlapping = append(non_overlapping, sg.sg)
			for v, next := s.Items()(); next != nil; v, next = next() {
				item := v.(types.Int)
				if err := vids.Add(item); err != nil {
					panic(err)
				}
			}
		}
	}
	return non_overlapping
}

