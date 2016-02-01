package mine

import (
	"bytes"
	"log"
	"path"
)

import (
	"github.com/timtadh/fs2/bptree"
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/graple/store"
)

func MaximalSubGraphs(all store.SubGraphs, nodeAttrs *bptree.BpTree, tempDir string) (<-chan []byte, error) {
	labelsBf, err := fmap.CreateBlockFile(path.Join(tempDir, "labels.bptree"))
	if err != nil {
		return nil, err
	}
	labels, err := bptree.New(labelsBf, -1, 1)
	if err != nil {
		return nil, err
	}
	keys := make(chan []byte)
	go func() {
		defer labelsBf.Close()
		var cur []byte
		var had bool = false
		for key, sg, next := all.Backward()(); next != nil; key, sg, next = next() {
			if cur != nil && !bytes.Equal(key, cur) {
				if !had {
					keys<-cur
				}
				had = false
			}
			has, err := labels.Has(key)
			if err != nil {
				log.Fatal(err)
			}
			if has {
				had = true
			}
			if !bytes.Equal(cur, key) {
				// add all of the (potential) parents of this node
				for eIdx := range sg.E {
					nsg, _ := sg.RemoveEdge(eIdx)
					addToLabels(labels, nsg.ShortLabel())
				}
			}
			cur = key
		}
		if !had && cur != nil {
			keys<-cur
		}
		close(keys)
	}()
	return keys, nil
}

func addToLabels(labels *bptree.BpTree, label []byte) {
	has, err := labels.Has(label)
	if err != nil {
		log.Println(err)
	}
	if !has {
		err = labels.Add(label, []byte{0})
		if err != nil {
			log.Println(err)
		}
	}
}

