package mine

import (
	"github.com/timtadh/graple/store"
	"github.com/timtadh/goiso"
)

func bufferedIterator(it store.Iterator, bufSize int) (bit store.Iterator) {
	type kv struct {
		key []byte
		sg *goiso.SubGraph
	}
	buf := make([]kv, 0, bufSize)
	fillBuf := func(buf []kv, it store.Iterator) ([]kv, store.Iterator) {
		for i := 0; i < bufSize && it != nil; i++ {
			var key []byte
			var sg *goiso.SubGraph
			key, sg, it = it()
			if it != nil {
				buf = append(buf, kv{key, sg})
			}
		}
		return buf, it
	}
	pop := func(buf []kv) (kv, []kv) {
		t := buf[len(buf)-1]
		buf[len(buf)-1] = kv{}
		return t, buf[:len(buf)-1]
	}
	buf, it = fillBuf(buf, it)
	bit = func() (key []byte, sg *goiso.SubGraph, _ store.Iterator) {
		if len(buf) > 0 {
			var item kv
			item, buf = pop(buf)
			return item.key, item.sg, bit
		} else if (it == nil) {
			return nil, nil, nil
		} else {
			buf, it = fillBuf(buf, it)
			return bit()
		}
	}
	return bit
}
