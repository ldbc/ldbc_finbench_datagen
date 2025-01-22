#!/bin/bash

TUGRAPH_HOME=/home/qsp/project/tugraph-db/

g++ -fno-gnu-unique -fPIC -g --std=c++14 \
	-I${TUGRAPH_HOME}/include \
	-I${TUGRAPH_HOME}/src \
	-I${TUGRAPH_HOME}/deps/fma-common \
	-rdynamic -O3 -fopenmp -DNDEBUG \
	-o profiler_standalone \
	stat.cpp  "${TUGRAPH_HOME}/build/output/liblgraph.so" -lrt