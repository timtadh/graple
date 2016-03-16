# `GRAPLE`

By Tim Henderson, Copyright 2016, All Rights Reserved

Licensed under the GNU GPL version 3. This program is part of a paper
currently under review. If you use this program as part of an academic paper
please contact me for a citation.

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
