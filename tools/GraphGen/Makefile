CFLAGS=-Wall -fopenmp
LDFLAGS=-fopenmp

all: graph_gen

graph_gen.o: graph_gen.c
	g++ $(CFLAGS) -o $@ -c $^ 

graph_gen: graph_gen.o
	g++ -o $@ $^ $(LDFLAGS)

clean:
	rm -f graph_gen *.o
