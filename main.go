package main

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
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	// "encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
)

import (
	"github.com/timtadh/data-structures/hashtable"
	"github.com/timtadh/data-structures/list"
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/fs2/bptree"
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/getopt"
)

import (
	"github.com/timtadh/graple/graph"
	"github.com/timtadh/graple/mine"
	"github.com/timtadh/graple/store"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

var ErrorCodes map[string]int = map[string]int{
	"usage":   0,
	"version": 2,
	"opts":    3,
	"badint":  5,
	"baddir":  6,
	"badfile": 7,
}

var UsageMessage string = "graple --help"
var ExtendedMessage string = `
graple computes a sample of frequent subgraphs at a given support level.

Syntax

    $ graple -o <path> -c <path> \
             --support=<int> --sample-size=<int> \
             [Options]* \
             [Development Options]* \
             <input-path>

    The input path should be a file (or a gzipped file) in the veg format.

Example

    $ graple -o /tmp/output -c /tmp/cache \
             --support=5 --sample-size=10 $HOME/data/expr.gz

Options
    -h, --help                  view this message
    -o, --output=<dir>          output directory (will be over written)
    -c, --cache=<dir>           disk cache directory (will be over written)
    -s, --support=<int>         the minimum support
    -m, --min-vertices=<int>    minumum number of vertices required to sample
                                a subgraph
    --sample-size=<int>         number of samples to collect
    --probabilities             compute the probability matrices

Development Options
    --mem-profile=<path>        turn on heap profiling
    --cpu-profile=<path>        turn on cpu profiling

veg File Format
    The veg file format is a line delimited format with vertex lines and
    edge lines. For example:

    vertex	{"id":136,"label":""}
    edge	{"src":23,"targ":25,"label":"ddg"}

    Note: the spaces between vertex and {...} are tabs
    Note: the spaces between edge and {...} are tabs

veg Grammar
    line -> vertex "\n"
          | edge "\n"

    vertex -> "vertex" "\t" vertex_json

    edge -> "edge" "\t" edge_json

    vertex_json -> {"id": int, "label": string, ...}
    // other items are optional

    edge_json -> {"src": int, "targ": int, "label": int, ...}
    // other items are  optional
`

func Usage(code int) {
	fmt.Fprintln(os.Stderr, UsageMessage)
	if code == 0 {
		fmt.Fprintln(os.Stdout, ExtendedMessage)
		code = ErrorCodes["usage"]
	} else {
		fmt.Fprintln(os.Stderr, "Try -h or --help for help")
	}
	os.Exit(code)
}

func Input(input_path string) (reader io.Reader, closeall func()) {
	stat, err := os.Stat(input_path)
	if err != nil {
		panic(err)
	}
	if stat.IsDir() {
		return InputDir(input_path)
	} else {
		return InputFile(input_path)
	}
}

func InputFile(input_path string) (reader io.Reader, closeall func()) {
	freader, err := os.Open(input_path)
	if err != nil {
		panic(err)
	}
	if strings.HasSuffix(input_path, ".gz") {
		greader, err := gzip.NewReader(freader)
		if err != nil {
			panic(err)
		}
		return greader, func() {
			greader.Close()
			freader.Close()
		}
	}
	return freader, func() {
		freader.Close()
	}
}

func InputDir(input_dir string) (reader io.Reader, closeall func()) {
	var readers []io.Reader
	var closers []func()
	dir, err := ioutil.ReadDir(input_dir)
	if err != nil {
		panic(err)
	}
	for _, info := range dir {
		if info.IsDir() {
			continue
		}
		creader, closer := InputFile(path.Join(input_dir, info.Name()))
		readers = append(readers, creader)
		closers = append(closers, closer)
	}
	reader = io.MultiReader(readers...)
	return reader, func() {
		for _, closer := range closers {
			closer()
		}
	}
}

func ParseInt(str string) int {
	i, err := strconv.Atoi(str)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing '%v' expected an int\n", str)
		Usage(ErrorCodes["badint"])
	}
	return i
}

func AssertDir(dir string) string {
	dir = path.Clean(dir)
	fi, err := os.Stat(dir)
	if err != nil && os.IsNotExist(err) {
		err := os.MkdirAll(dir, 0775)
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			Usage(ErrorCodes["baddir"])
		}
		return dir
	} else if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		Usage(ErrorCodes["baddir"])
	}
	if !fi.IsDir() {
		fmt.Fprintf(os.Stderr, "Passed in file was not a directory, %s", dir)
		Usage(ErrorCodes["baddir"])
	}
	return dir
}

func EmptyDir(dir string) string {
	dir = path.Clean(dir)
	_, err := os.Stat(dir)
	if err != nil && os.IsNotExist(err) {
		err := os.MkdirAll(dir, 0775)
		if err != nil {
			log.Fatal(err)
		}
	} else if err != nil {
		log.Fatal(err)
	} else {
		// something already exists lets delete it
		err := os.RemoveAll(dir)
		if err != nil {
			log.Fatal(err)
		}
		err = os.MkdirAll(dir, 0775)
		if err != nil {
			log.Fatal(err)
		}
	}
	return dir
}

func AssertFile(fname string) string {
	fname = path.Clean(fname)
	fi, err := os.Stat(fname)
	if err != nil && os.IsNotExist(err) {
		return fname
	} else if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		Usage(ErrorCodes["badfile"])
	} else if fi.IsDir() {
		fmt.Fprintf(os.Stderr, "Passed in file was a directory, %s", fname)
		Usage(ErrorCodes["badfile"])
	}
	return fname
}

func main() {
	args, optargs, err := getopt.GetOpt(
		os.Args[1:],
        "hs:m:o:c:",
		[]string{
			"help",
			"support=",
			"cache=",
			"min-vertices=",
			"sample-size=",
			"mem-profile=",
			"cpu-profile=",
			"output=",
			"probabilities",
		},
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		Usage(ErrorCodes["opts"])
	}
	log.Printf("Number of goroutines = %v", runtime.NumGoroutine())

	support := -1
	minVertices := -1
	sampleSize := -1
	memProfile := ""
	cpuProfile := ""
	outputDir := ""
	cache := ""
	for _, oa := range optargs {
		switch oa.Opt() {
		case "-h", "--help":
			Usage(0)
		case "-o", "--output":
			outputDir = EmptyDir(AssertDir(oa.Arg()))
		case "-s", "--support":
			support = ParseInt(oa.Arg())
		case "-m", "--min-vertices":
			minVertices = ParseInt(oa.Arg())
		case "-c", "--cache":
			cache = AssertDir(oa.Arg())
		case "--sample-size":
			sampleSize = ParseInt(oa.Arg())
		case "--mem-profile":
			memProfile = AssertFile(oa.Arg())
		case "--cpu-profile":
			cpuProfile = AssertFile(oa.Arg())
		}
	}

	if support < 1 {
		fmt.Fprintf(os.Stderr, "You must supply a support greater than 0, you gave %v\n", support)
		Usage(ErrorCodes["opts"])
	}

	if sampleSize < 1 {
		fmt.Fprintf(os.Stderr, "You must supply a sample-size greater than 0, you gave %v\n", sampleSize)
		Usage(ErrorCodes["opts"])
	}

	if outputDir == "" {
		fmt.Fprintf(os.Stderr, "You must supply an output file (use -o)\n")
		Usage(ErrorCodes["opts"])
	}

	if cache == "" {
		fmt.Fprintln(os.Stderr, "you must supply a --cache=<dir>")
		Usage(ErrorCodes["opts"])
	}

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "Expected a path to the graph file")
		Usage(ErrorCodes["opts"])
	}

	getReader := func() (io.Reader, func()) { return Input(args[0]) }

	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	var memProfFile io.WriteCloser
	if memProfile != "" {
		f, err := os.Create(memProfile)
		if err != nil {
			log.Fatal(err)
		}
		memProfFile = f
		defer f.Close()
	}

	nodePath := path.Join(outputDir, "node-attrs.bptree")

	nodeBf, err := fmap.CreateBlockFile(nodePath)
	if err != nil {
		log.Fatal(err)
	}
	defer nodeBf.Close()
	nodeAttrs, err := bptree.New(nodeBf, 4, -1)
	if err != nil {
		log.Fatal(err)
	}

	G, err := graph.LoadGraph(getReader, "", nodeAttrs, nil)
	if err != nil {
		log.Println("Error loading the graph")
		log.Panic(err)
	}
	log.Print("Loaded graph, about to start mining")


	sgCount := 0
	sgMaker := func() store.SubGraphs {
		name := fmt.Sprintf("subgraphs-%d.b+tree", sgCount)
		sgCount++
		path := path.Join(cache, name)
		s := store.NewFs2BpTree(G, path)
		// os.Remove(path)
		// s, err := store.NewSqlite(G, path)
		// if err != nil {
		// 	log.Panic(err)
		// }
		return s
	}

	idxCount := 0
	idxMaker := func() store.UniqueIndex {
		name := fmt.Sprintf("unique-idx-%d.b+tree", idxCount)
		idxCount++
		path := path.Join(cache, name)
		s := store.NewFs2UniqueIndex(G, path)
		// os.Remove(path)
		// s, err := store.NewSqlite(G, path)
		// if err != nil {
		// 	log.Panic(err)
		// }
		return s
	}

	setsCount := 0
	setsMaker := func() store.SetsMap {
		name := fmt.Sprintf("sets-%d.b+tree", setsCount)
		setsCount++
		path := path.Join(cache, name)
		s := store.NewFs2Sets(path)
		// os.Remove(path)
		// s, err := store.NewSqlite(G, path)
		// if err != nil {
		// 	log.Panic(err)
		// }
		return s
	}

	// memFsMaker := func() store.SubGraphs {
	// 	return store.AnonFs2BpTree(G)
	// }

	m := mine.RandomWalk(
		G,
		support,
		minVertices,
		sampleSize,
		memProfFile,
		sgMaker,
		idxMaker,
		setsMaker,
	)
	keys := list.NewSorted(10, false)
	counts := hashtable.NewLinearHash()
	for label := range m.Report {
		key := types.ByteSlice(label)
		count := 0
		if counts.Has(key) {
			c, err := counts.Get(key)
			if err != nil {
				log.Panic(err)
			}
			count = c.(int)
		}
		counts.Put(key, count + 1)
		keys.Add(key)
	}
	log.Println("Tries", m.Tries)
	triesPath := path.Join(outputDir, "tries")
	if f, e := os.Create(triesPath); e != nil {
		log.Fatal(err)
	} else {
		fmt.Fprintln(f, m.Tries)
		f.Close()
	}
	{
		log.Println("Finished mining! Writing output...")
		keyCh := make(chan []byte)
		go func() {
			for k, next := keys.Items()(); next != nil; k, next = next() {
				keyCh<-[]byte(k.(types.ByteSlice))
			}
			close(keyCh)
		}()
		writeMaximalPatterns(keyCh, m.AllEmbeddings, nodeAttrs, outputDir)
	}

	log.Println("Finished writing patterns. Computing probabilities...")
	count := 0
	for k, next := keys.Items()(); next != nil; k, next = next() {
		patDir := path.Join(outputDir, fmt.Sprintf("%d", count))
		log.Println("-----------------------------------")
		c, err := counts.Get(k)
		if err != nil {
			log.Fatal(err)
		}
		key := []byte(k.(types.ByteSlice))
		dupCount := c.(int)
		// if max.Count(key) < support {
		// 	log.Println("wat not enough subgraphs", max.Count(key))
		// 	continue
		// }
		if c, err := os.Create(path.Join(patDir, "duplicates")); err != nil {
			log.Fatal(err)
		} else {
			fmt.Fprintln(c, dupCount)
			c.Close()
		}
		for _, sg, next := m.AllEmbeddings.Find(key)(); next != nil; _, sg, next = next() {
			vp, Q, R, u, err := m.PrMatrices(sg)
			if err != nil {
				log.Println(err)
				errPath := path.Join(patDir, "error")
				if f, e := os.Create(errPath); e != nil {
					log.Fatal(err)
				} else {
					fmt.Fprintln(f, err)
					f.Close()
				}
			} else {
				bytes, err := json.Marshal(map[string]interface{}{
					"Q": Q,
					"R": R,
					"u": u,
					"startingPoints": vp,
				})
				if err != nil {
					log.Fatal(err)
				}
				matPath := path.Join(patDir, "matrices.json")
				if m, err := os.Create(matPath); err != nil {
					log.Fatal(err)
				} else {
					_, err := m.Write(bytes)
					if err != nil {
						m.Close()
						log.Fatal(err)
					}
					m.Close()
				}
			}
			break
		}
		count++
	}
	log.Println("Done!")
}

func writeAllPatterns(all store.SubGraphs, nodeAttrs *bptree.BpTree, outputDir string) {
	alle, err := os.Create(path.Join(outputDir, "all-embeddings.dot"))
	if err != nil {
		log.Fatal(err)
	}
	defer alle.Close()
	allp, err := os.Create(path.Join(outputDir, "all-patterns.dot"))
	if err != nil {
		log.Fatal(err)
	}
	defer allp.Close()
	count := 0
	for key, next := all.Keys()(); next != nil; key, next = next() {
		writePattern(count, outputDir, alle, allp, nodeAttrs, all, key)
		count++
	}
}

func writeMaximalSubGraphs(all store.SubGraphs, nodeAttrs *bptree.BpTree, outputDir string) {
	keys, err := mine.MaximalSubGraphs(all, nodeAttrs, outputDir) 
	if err != nil {
		log.Fatal(err)
	}
	writeMaximalPatterns(keys, all, nodeAttrs, outputDir)
}

func writeMaximalPatterns(keys <-chan []byte, sgs store.Findable, nodeAttrs *bptree.BpTree, outputDir string) {
	maxe, err := os.Create(path.Join(outputDir, "maximal-embeddings.dot"))
	if err != nil {
		log.Fatal(err)
	}
	defer maxe.Close()
	maxp, err := os.Create(path.Join(outputDir, "maximal-patterns.dot"))
	if err != nil {
		log.Fatal(err)
	}
	defer maxp.Close()
	count := 0
	for key := range keys {
		writePattern(count, outputDir, maxe, maxp, nodeAttrs, sgs, key)
		count++
	}
	countPath := path.Join(outputDir, "count")
	if c, err := os.Create(countPath); err != nil {
		log.Fatal(err)
	} else {
		fmt.Fprintln(c, count)
		c.Close()
	}
}

func writePattern(count int, outDir string, embeddings, patterns io.Writer, nodeAttrs *bptree.BpTree, all store.Findable, key []byte) {
	patDir := EmptyDir(path.Join(outDir, fmt.Sprintf("%d", count)))
	patDot := path.Join(patDir, "pattern.dot")
	patVeg := path.Join(patDir, "pattern.veg")
	patName := path.Join(patDir, "pattern.name")
	patCount := path.Join(patDir, "count")
	instDir := EmptyDir(path.Join(patDir, "instances"))
	i := 0
	for _, sg, next := all.Find(key)(); next != nil; _, sg, next = next() {
		if i == 0 {
			fmt.Fprintln(patterns, "//", sg.Label())
			fmt.Fprintln(patterns)
			fmt.Fprintln(patterns, sg.String())
			fmt.Fprintln(embeddings, "//", sg.Label())
			fmt.Fprintln(embeddings)
			if pat, err := os.Create(patDot); err != nil {
				log.Fatal(err)
			} else {
				fmt.Fprintln(pat, sg.String())
				pat.Close()
			}
			if name, err := os.Create(patName); err != nil {
				log.Fatal(err)
			} else {
				fmt.Fprintln(name, sg.Label())
				name.Close()
			}
			if veg, err := os.Create(patVeg); err != nil {
				log.Fatal(err)
			} else {
				veg.Write(sg.VEG(nil))
				veg.Close()
			}
		}
		curDir := EmptyDir(path.Join(instDir, fmt.Sprintf("%d", i)))
		emDot := path.Join(curDir, "embedding.dot")
		emVeg := path.Join(curDir, "embedding.veg")
		if nodeAttrs != nil {
			attrs := make(map[int]map[string]interface{})
			for _, v := range sg.V {
				bid := make([]byte, 4)
				binary.BigEndian.PutUint32(bid, uint32(v.Id))
				err := nodeAttrs.DoFind(
					bid,
					func(key, value []byte) error {
						a, err := graph.ParseJson(value)
						if err != nil {
							log.Fatal(err)
						}
						attrs[v.Id] = a
						return nil
					})
				if err != nil {
					log.Fatal(err)
				}
			}
			fmt.Fprintln(embeddings, sg.StringWithAttrs(attrs))
			if em, err := os.Create(emDot); err != nil {
				log.Fatal(err)
			} else {
				fmt.Fprintln(em, sg.StringWithAttrs(attrs))
				em.Close()
			}
			if veg, err := os.Create(emVeg); err != nil {
				log.Fatal(err)
			} else {
				veg.Write(sg.VEG(attrs))
				veg.Close()
			}
		} else {
			fmt.Fprintln(embeddings, sg.String())
			if em, err := os.Create(emDot); err != nil {
				log.Fatal(err)
			} else {
				fmt.Fprintln(em, sg.String())
				em.Close()
			}
			if veg, err := os.Create(emVeg); err != nil {
				log.Fatal(err)
			} else {
				veg.Write(sg.VEG(nil))
				veg.Close()
			}
		}
		i++
	}
	if c, err := os.Create(patCount); err != nil {
		log.Fatal(err)
	} else {
		fmt.Fprintln(c, i)
		c.Close()
	}
	fmt.Fprintln(patterns)
	fmt.Fprintln(patterns)
	fmt.Fprintln(embeddings)
	fmt.Fprintln(embeddings)
}

