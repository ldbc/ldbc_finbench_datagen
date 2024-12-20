# About the data profiler

This tool as a Data Profiler is developed to profile the data distribution.
It is devleoped based on [TuGraph][1], an open-source high performance graph database contributed by Ant Group
Co., Ltd.

The profiling of these metrics are currently supported:
- Count of V(vertices) and E(edges)
- Ratio of E over V
- Edge multiplicity
- In-degree and out-degree distribution including the percentiles
- WCC and Diameter results

And these features in visualization are supported:
- plot the PowerLaw Distribution of degree
- PowerLaw Distribution Regression

# How to use

## Profile
In order to compile this tool, you need to first pull TuGraph to local and set the TUGRAPH_HOME environment varible in
`CmakeLists.txt` or `compile.sh` to the repository. See `CmakeLists.txt` and `compile.sh` for
more details.

## Plot
See `plot.py` for details.

[1]: https://github.com/TuGraph-db/tugraph-db/