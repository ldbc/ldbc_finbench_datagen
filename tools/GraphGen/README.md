# Synthetic Graph Generator

## Intruction
graph_gen is a simple synthetic graph generator, support graph types are Kronecker, RMat and Erdos.
The generated result may has self-loop and duplicated-edges.

## Compile
make graph_gen

## Run
Use ```./graph_gen -h``` to get help 

### Input
./graph_gen ./graph_gen -s [scale] -e [edge_factor] -t [gen_type] -x [seed] -v [0/1]

Supported gen_type include kron/rmat/erdos

### Output
Edgelist file in binary or txt, each vertex is 4 Bytes.

### example
Here are some examples:

./graph_gen -s 20

./graph_gen -s 22 -e 16 -s kron -x 0

./graph_gen -s 22 -e 16 -s erdos -v 1
