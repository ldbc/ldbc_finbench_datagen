/* Copyright (c) 2022 AntGroup. All Rights Reserved. */

#include <string.h>

#include <iostream>
#include <unordered_map>

#include "algo.h"
#include "olap/olap_on_disk.h"

using namespace lgraph_api;
using namespace lgraph_api::olap;
using json = nlohmann::json;

class MyConfig : public ConfigBase<Empty> {
   public:
    std::string name = std::string("profiler");
    bool run_degree = true;

    void AddParameter(fma_common::Configuration &config) {
        ConfigBase<Empty>::AddParameter(config);
        config.Add(run_degree, "degree", true)
            .Comment("Boolean. T to run degree profile and F to run wcc/ de");
    }

    void Print() {
        ConfigBase<Empty>::Print();
        std::cout << "  name: " << name << std::endl;
        std::cout << "  run degree: " << run_degree << std::endl;
    }

    MyConfig(int &argc, char **&argv) : ConfigBase<Empty>(argc, argv) {
        fma_common::Configuration config;
        AddParameter(config);
        config.ExitAfterHelp(true);
        config.ParseAndFinalize(argc, argv);
        Print();
    }
};

void print_deg_dist(std::vector<size_t> &deg, size_t size,
                    std::string filename) {
    std::sort(deg.begin(), deg.end());
    std::vector<std::pair<size_t, size_t>> dist;
    {
        size_t i = 0;
        size_t curr = deg[i];
        size_t count = 1;
        while (++i < size) {
            if (deg[i] == curr) {
                count++;
            } else {
                dist.emplace_back(curr, count);
                count = 1;
                curr = deg[i];
            }
        }
        dist.emplace_back(curr, count);
    }
    {
        std::ofstream logfile;
        logfile.open(filename, std::ios::out | std::ios::trunc);
        for (auto v : dist) {
            logfile << v.first << " " << v.second << std::endl;
        }
        logfile.close();
    }
}

void print_high_deg(std::vector<size_t> input_deg,
                    std::vector<size_t> input_deg_reverse, size_t size,
                    std::string filename) {
    std::ofstream logfile;
    logfile.open(filename, std::ios::out | std::ios::trunc);

    const int PARTS = 100;
    std::vector<std::pair<size_t, size_t>> deg;
    deg.resize(size);
    for (size_t i = 0; i < size; ++i) {
        deg[i] = std::make_pair(input_deg[i], i);
    }
    std::sort(deg.begin(), deg.end(),
              std::greater<std::pair<size_t, size_t>>());
    logfile << "DEGREE TOP " << PARTS;
    for (auto it = deg.begin(); it != deg.end() && it != deg.begin() + PARTS;
         ++it) {
        logfile << " (" << it->first << ","
                << (int)input_deg_reverse[it->second] << ")";
    }
    logfile << std::endl;

    logfile << "DEGREE PEC " << PARTS;
    for (int i = 0; i <= PARTS; ++i) {
        logfile << " " << (deg.begin() + deg.size() / PARTS * i)->first;
    }
    logfile << std::endl;
    logfile.close();
}

void count_edges(OlapOnDisk<Empty> &graph, std::string &output_dir) {
    std::ofstream logfile;
    logfile.open(output_dir + "/edges.txt", std::ios::out | std::ios::trunc);

    size_t nv = graph.NumVertices();
    size_t ne = graph.NumEdges();
    std::cout << "V " << nv << ", E " << ne << ", E/V " << ne * 1.0 / nv
              << std::endl;
    logfile << "V " << nv << ", E " << ne << ", E/V " << ne * 1.0 / nv
            << std::endl;
    auto active = graph.AllocVertexSubset();
    active.Fill();
    auto unique_edges = graph.ProcessVertexActive<double>(
        [&](size_t vtx) {
            std::set<size_t> s;
            s.clear();
            for (auto edge : graph.OutEdges(vtx)) {
                s.insert(edge.neighbour);
            }
            return s.size();
        },
        active);
    std::cout << "Unique edges: " << unique_edges << " / " << ne
              << ", Multiplicity: " << ne * 1.0 / unique_edges << std::endl;
    logfile << "Unique edges: " << unique_edges << " / " << ne
            << ", Multiplicity: " << ne * 1.0 / unique_edges << std::endl;
    logfile.close();
}

void calcu_degree(OlapOnDisk<Empty> &graph, std::string &output_dir) {
    auto active = graph.AllocVertexSubset();
    active.Fill();
    std::vector<size_t> input_deg;
    graph.ProcessVertexActive<double>(
        [&](size_t vtx) {
            input_deg.push_back(graph.InDegree(vtx));
            return input_deg.size();
        },
        active);

    std::vector<size_t> out_deg;
    graph.ProcessVertexActive<double>(
        [&](size_t vtx) {
            out_deg.push_back(graph.OutDegree(vtx));
            return out_deg.size();
        },
        active);
    print_deg_dist(out_deg, graph.NumVertices(),
                   output_dir + "/out_degree_dist.txt");
    print_deg_dist(input_deg, graph.NumVertices(),
                   output_dir + "/in_degree_dist.txt");
    print_high_deg(out_deg, input_deg, graph.NumVertices(),
                   output_dir + "/out-in.txt");
    print_high_deg(input_deg, out_deg, graph.NumVertices(),
                   output_dir + "/in-out.txt");
}

int main(int argc, char **argv) {
    MyConfig config(argc, argv);
    OlapOnDisk<Empty> graph;

    std::string logfile = config.output_dir + "/profile.log";
    FILE *fout = fopen(logfile.c_str(), "w");
    assert(fout != nullptr);

    double load_cost, core_cost;
    double start_time = get_time();
    if (config.run_degree) {
        printf("Run degree profiling.\n");
        fprintf(fout, "Run degree profiling.\n");

        graph.Load(config, DUAL_DIRECTION);  // load as raw
        load_cost = get_time() - start_time;
        printf("load_cost = %.2lf(s)\n", load_cost);
        fprintf(fout, "load_cost = %.2lf(s)\n", load_cost);

        start_time = get_time();
        count_edges(graph, config.output_dir);   // count edges
        calcu_degree(graph, config.output_dir);  // profile degree
        core_cost = get_time() - start_time;
    } else {
        printf("Run diameter and wcc profiling.\n");
        fprintf(fout, "Run diameter and wcc profiling.\n");

        graph.Load(config, MAKE_SYMMETRIC);  // load as symmetric to support wcc
        load_cost = get_time() - start_time;
        printf("load_cost = %.2lf(s)\n", load_cost);
        fprintf(fout, "load_cost = %.2lf(s)\n", load_cost);

        auto label = graph.AllocVertexArray<size_t>();
        WCCCore(graph, label);  // profile wcc
        std::set<size_t> roots = {64};
        DECore(graph, roots);  // profile de
        core_cost = get_time() - start_time;
    }
    printf("exec_cost = %.2lf(s)\n", core_cost);
    fprintf(fout, "exec_cost = %.2lf(s)\n", core_cost);
    printf("total_cost = %.2lf(s)\n", core_cost + load_cost);
    fprintf(fout, "total_cost = %.2lf(s)\n", core_cost + load_cost);

    return 0;
}