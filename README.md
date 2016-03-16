# `GRAPLE`

By Tim Henderson, Copyright 2016, All Rights Reserved

Licensed under the GNU GPL version 3. This program is part of a paper
currently under review. If you use this program as part of an academic paper
please contact me for a citation.

## Example Usage

Collect a sample of frequent subgraphs:

    $ graple -o /tmp/output \
             -c /tmp/cache \
             --support=5 \
             --sample-size=10 \
             --min-vertices=8
             --probabilities \
             ./data/expr.gz
    2016/03/15 21:29:52 Number of goroutines = 2
    2016/03/15 21:29:53 Loaded graph, about to start mining
    2016/03/15 21:29:53 found mfsg 14:13(0:this)(1:return)(2:cwru.hacsoc.expr.Parser.lex)(3:cwru.hacsoc.expr.Parser.lex)(4:new)(5:call cwru.hacsoc.expr.Lexer.peek)(6:call cwru.hacsoc.expr.Node.<init>)(7:call cwru.hacsoc.expr.Tokens.ordinal)(8:array index)(9:switch)(10:cwru.hacsoc.expr.Parser$1.$SwitchMap$cwru$hacsoc$expr$Tokens)(11:cwru.hacsoc.expr.Match.token)(12:call cwru.hacsoc.expr.Lexer.next)[9->1:][9->3:][9->4:][9->6:][9->12:][5->11:cwru.hacsoc.expr.Match:1][11->7:cwru.hacsoc.expr.Tokens:1][7->8:int:2][8->9:int:0][10->8:int[]:1][2->5:cwru.hacsoc.expr.Lexer:1][3->12:cwru.hacsoc.expr.Lexer:1][0->2:cwru.hacsoc.expr.Parser:1][0->3:cwru.hacsoc.expr.Parser:1]
    2016/03/15 21:29:53 found mfsg but it was too small
    2016/03/15 21:29:53 found mfsg but it was too small
    2016/03/15 21:29:53 found mfsg but it was too small
    2016/03/15 21:29:53 found mfsg but it was too small
    2016/03/15 21:29:53 found mfsg but it was too small
    ...

Compute the selection probabilities using SuiteSparse

    $ graple-selection-probabilities /tmp/output
    /tmp/output/
    sel-pr -m /tmp/output/0/matrices.json -o /tmp/output/0/pattern.pr
    /tmp/output/0/pattern.pr
    cowardly refusing to overwrite output
    sel-pr -m /tmp/output/1/matrices.json -o /tmp/output/1/pattern.pr
    /tmp/output/1/pattern.pr
    cowardly refusing to overwrite output


## Usage

```
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
```
