#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <iostream>
#include <unordered_map>

#include "core/graph.hpp"
#include "fma-common/configuration.h"
#include "fma-common/file_system.h"
#include "fma-common/string_formatter.h"

size_t parse_edge(const char *p, const char * end, EdgeUnit<double> & e) {
  const char * orig = p;
  int64_t t = 0;
  size_t r = 0;
  if (*p == '#') {
    while (*p != '\n') p++;
    e.src = MAX_VID;
    e.dst = MAX_VID;
    return p - orig;
  }
  r = fma_common::TextParserUtils::ParseInt64(p, end, t);
  e.src = t;
  p += r;
  while (p != end && (*p == ' ' || *p == '\t')) p++;
  r = fma_common::TextParserUtils::ParseInt64(p, end, t);
  e.dst = t;
  p += r;
  double w = 1;
  //r = fma_common::TextParserUtils::ParseDigit(p, end, w);
  e.edge_data = w;
  if (e.src == e.dst) e.edge_data /= 2;
  //p += r;
  return p - orig;
}

bool filter_edge(EdgeUnit<double> & e) {
  return e.src != MAX_VID && e.dst != MAX_VID;
}

size_t parse_label(const char * p, const char * end, VertexUnit<VertexId> & v) {
  const char * orig = p;
  int64_t t = 0;
  p += fma_common::TextParserUtils::ParseInt64(p, end, t);
  v.vertex = t;
  while (p != end && (*p == ' ' || *p == '\t')) p++;
  p += fma_common::TextParserUtils::ParseInt64(p, end, t);
  v.vertex_data = t;
  return p - orig;
}

int main(int argc, char ** argv) {
  // FILE * fin = fopen(argv[1], "r");
  // char * line = new char[1024];
  // size_t read_length;
  // size_t line_length;
  
  // std::unordered_map<long int, long int> label;
  // long int node_num = 0;

  // while((read_length = getline(&line, &line_length, fin)) != -1) {
  //   node_num++;
  //   if (line[0] == '#') continue;
  //   long int node, node_label;
  //   assert(sscanf(line, "%ld %ld", &node, &node_label) == 2);
  //   auto it = label.find(node_label);
  //   if (it == label.end()) {
  //     label[node_label] = 1;
  //   } else {
  //     it->second += 1;
  //   }
  // }

  // long int max_label = 0;
  // for (auto ele : label) {
  //   if (ele.second > max_label) {
  //     max_label = ele.second;
  //   }
  // }
  
  fma_common::Configuration config;
  std::string input_dir = "";
  std::string label_dir = "";
  VertexId num_vertices = 0;

  config.Add(input_dir, "input_dir", true)
    .Comment("Input graph directory, only needed when calculating modularity");
  config.Add(label_dir, "label_dir", false)
    .Comment("Label directory to load");
  config.Add(num_vertices, "num_vertices", false)
    .Comment("number of vertices");
  
  config.Parse(argc, argv);
  config.ExitAfterHelp();
  config.Finalize();

  Graph<double> graph;
  if (input_dir == "") {
    EdgeUnit<double> * edge_array = new EdgeUnit<double>[2 << 20];
    for (VertexId v_i = 0; v_i < (2 << 20); v_i++) {
      edge_array[v_i].src = 0;
      edge_array[v_i].dst = v_i;
      edge_array[v_i].edge_data = 1.0;
    }
    VertexId edges = 2 << 20;
    graph.load_from_array(*edge_array, num_vertices, edges);
  } else {
    graph.load_txt_undirected(input_dir, num_vertices, parse_edge, filter_edge);
  }

  VertexId * label = graph.alloc_vertex_array<VertexId>();
  graph.fill_vertex_array(label, num_vertices);
  
  graph.load_vertex_array_txt<VertexId>(label, label_dir, parse_label);

  VertexId node_num = 0;
  VertexId * comm = graph.alloc_vertex_array<VertexId>();
  graph.fill_vertex_array(comm, (VertexId)0);

  for (VertexId v = 0; v < num_vertices; v++) {
    if (label[v] == num_vertices) continue;
    node_num++;
    comm[label[v]]++;
  }

  VertexId max_comm = 0;
  VertexId comm_num = 0;
  for (VertexId v = 0; v < num_vertices; v++) {
    if (comm[v] > max_comm) max_comm = comm[v];
    if (comm[v] > 0) comm_num++;
  }

  VertexId threshold = num_vertices / 10;
  Bitmap * active = graph.alloc_vertex_bitmap();
  active->fill();
  VertexId chors_num = graph.stream_vertices<VertexId>(
    [&] (VertexId v) {
      if (comm[label[v]] > threshold && label[v] != v && comm[v] > 0) {
        printf("%ld\t", v);
        return 1;
      } else {
        return 0;
      }
    },
    active
  );
  printf("\nchors_num = %ld\n", chors_num);

 if (input_dir != "") {
    double * k = graph.alloc_vertex_array<double>();
    graph.fill_vertex_array(k, 0.0);
    double * e_tot = graph.alloc_vertex_array<double>();
    graph.fill_vertex_array(e_tot, 0.0);
    
    double m = graph.stream_vertices<double> (
      [&] (VertexId v) {
        for (auto e : graph.out_edges(v)) {
          k[v] += e.edge_data;
        }
        write_add(&e_tot[label[v]], k[v]);
        return k[v];
      },
      active
    ) / 2;
    //LOG() << "m = " << m;
    printf("m = %lf\n", m);

    double Q = graph.stream_vertices<double> (
      [&] (VertexId v) {
        double q = 0.0;
        for (auto e : graph.out_edges(v)) {
          VertexId nbr = e.neighbour;
          if (label[v] == label[nbr]) q += e.edge_data;
        }
        q -= 1.0 * k[v] * e_tot[label[v]] / (2 * m);
        return q;
      },
      active
    ) / (2.0 * m);
    //LOG() << "Q = " << Q;
    printf("Q = %lf\n", Q);
 }

  printf("node number is %ld\n", node_num);
  printf("community number is %ld\n", comm_num);
  printf("largest community size is %ld\n", max_comm);
  printf("largest community has percentage of %lf%%\n", 100.0 * max_comm / node_num);
}