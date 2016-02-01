package graph

/* Tim Henderson (tadh@case.edu)
*
* Copyright (c) 2015, Tim Henderson, Case Western Reserve University
* Cleveland, Ohio 44106. All Rights Reserved.
*
* This library is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 3 of the License, or (at
* your option) any later version.
*
* This library is distributed in the hope that it will be useful, but
* WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
* General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this library; if not, write to the Free Software
* Foundation, Inc.,
*   51 Franklin Street, Fifth Floor,
*   Boston, MA  02110-1301
*   USA
 */

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

import (
	"github.com/timtadh/data-structures/hashtable"
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/fs2/bptree"
	"github.com/timtadh/goiso"
)

type JsonObject map[string]interface{}

type error_list []error
type ParseErrors error_list
type SerializeErrors error_list

func (self error_list) Error() string {
	var s []string
	for _, err := range self {
		s = append(s, err.Error())
	}
	return "[" + strings.Join(s, ",") + "]"
}
func (self ParseErrors) Error() string     { return error_list(self).Error() }
func (self SerializeErrors) Error() string { return error_list(self).Error() }

func ProcessLines(reader io.Reader, process func([]byte)) {

	const SIZE = 4096

	read_chunk := func() (chunk []byte, closed bool) {
		chunk = make([]byte, 4096)
		if n, err := reader.Read(chunk); err == io.EOF {
			return nil, true
		} else if err != nil {
			panic(err)
		} else {
			return chunk[:n], false
		}
	}

	parse := func(buf []byte) (obuf, line []byte, ok bool) {
		for i := 0; i < len(buf); i++ {
			if buf[i] == '\n' {
				line = buf[:i+1]
				obuf = buf[i+1:]
				return obuf, line, true
			}
		}
		return buf, nil, false
	}

	var buf []byte
	read_line := func() (line []byte, closed bool) {
		ok := false
		buf, line, ok = parse(buf)
		for !ok {
			chunk, closed := read_chunk()
			if closed || len(chunk) == 0 {
				return buf, true
			}
			buf = append(buf, chunk...)
			buf, line, ok = parse(buf)
		}
		return line, false
	}

	var line []byte
	closed := false
	for !closed {
		line, closed = read_line()
		process(line)
	}
}

func renderJson(obj JsonObject) (data []byte, err error) {
	return json.Marshal(obj)
}

func ParseJson(data []byte) (obj JsonObject, err error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func parseLine(line []byte) (line_type string, data []byte) {
	split := bytes.Split(line, []byte("\t"))
	return strings.TrimSpace(string(split[0])), bytes.TrimSpace(split[1])
}

func graphSize(reader io.Reader) (V, E int) {
	ProcessLines(reader, func(line []byte) {
		if bytes.HasPrefix(line, []byte("vertex")) {
			V++
		} else if bytes.HasPrefix(line, []byte("edge")) {
			E++
		}
	})
	return V, E
}

func LoadGraph(getInput func() (io.Reader, func()), supportAttr string, nodeAttrs *bptree.BpTree, supportAttrs map[int]string) (graph *goiso.Graph, err error) {
	var errors ParseErrors
	reader, closer := getInput()
	G := goiso.NewGraph(graphSize(reader))
	closer()
	graph = &G
	vids := hashtable.NewLinearHash() // int64 ==> *goiso.Vertex

	reader, closer = getInput()
	defer closer()
	ProcessLines(reader, func(line []byte) {
		if len(line) == 0 || !bytes.Contains(line, []byte("\t")) {
			return
		}
		line_type, data := parseLine(line)
		switch line_type {
		case "vertex":
			if err := LoadVertex(graph, supportAttr, vids, nodeAttrs, supportAttrs, data); err != nil {
				errors = append(errors, err)
			}
		case "edge":
			if err := LoadEdge(graph, vids, data); err != nil {
				errors = append(errors, err)
			}
		default:
			errors = append(errors, fmt.Errorf("Unknown line type %v", line_type))
			return
		}
	})
	if len(errors) == 0 {
		return graph, nil
	}
	return graph, errors
}

func LoadVertex(g *goiso.Graph, supportAttr string, vids types.Map, nodeAttrs *bptree.BpTree, supportAttrs map[int]string, data []byte) (err error) {
	obj, err := ParseJson(data)
	if err != nil {
		return err
	}
	_id, err := obj["id"].(json.Number).Int64()
	if err != nil {
		return err
	}
	label := strings.TrimSpace(obj["label"].(string))
	id := int(_id)
	vertex := g.AddVertex(id, label)
	err = vids.Put(types.Int(id), vertex)
	if err != nil {
		return err
	}
	if nodeAttrs != nil {
		bid := make([]byte, 4)
		binary.BigEndian.PutUint32(bid, uint32(vertex.Idx))
		err = nodeAttrs.Add(bid, data)
		if err != nil {
			return err
		}
		if supportAttr != "" {
			if _, has := obj[supportAttr]; !has {
				return fmt.Errorf("vertex did not have required supportAttr %v\n%v", supportAttr, string(data))
			}
			supportAttrs[vertex.Idx] = obj[supportAttr].(string)
		}
	}
	return nil
}

func SerializeVertex(g *goiso.Graph, v *goiso.Vertex) ([]byte, error) {
	obj := make(JsonObject)
	obj["id"] = v.Id
	obj["label"] = g.Colors[v.Color]
	return renderJson(obj)
}

func LoadEdge(g *goiso.Graph, vids types.Map, data []byte) (err error) {
	obj, err := ParseJson(data)
	if err != nil {
		return err
	}
	_src, err := obj["src"].(json.Number).Int64()
	if err != nil {
		return err
	}
	_targ, err := obj["targ"].(json.Number).Int64()
	if err != nil {
		return err
	}
	src := int(_src)
	targ := int(_targ)
	label := strings.TrimSpace(obj["label"].(string))
	if o, err := vids.Get(types.Int(src)); err != nil {
		return err
	} else {
		u := o.(*goiso.Vertex)
		if o, err := vids.Get(types.Int(targ)); err != nil {
			return err
		} else {
			v := o.(*goiso.Vertex)
			g.AddEdge(u, v, label)
		}
	}
	return nil
}

func SerializeEdge(g *goiso.Graph, e *goiso.Edge) ([]byte, error) {
	obj := make(JsonObject)
	obj["src"] = g.V[e.Src].Id
	obj["targ"] = g.V[e.Targ].Id
	obj["label"] = g.Colors[e.Color]
	return renderJson(obj)
}

