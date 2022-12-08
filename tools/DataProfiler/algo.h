/* Copyright (c) 2022 AntGroup. All Rights Reserved. */

#pragma once

#include <unordered_set>
#include "lgraph/olap_base.h"

using namespace lgraph_api;
using namespace lgraph_api::olap;

/**
 * @brief    Compute the Dimension Estimation algorithm.
 *
 * @param[in]    graph    The graph to compute on.
 * @param[in]    roots    The root vertex id to start de from.
 *
 * @return    return dimension of graph.
 */
size_t DECore(OlapBase<Empty>& graph, std::set<size_t>& roots);

/**
 * \brief   Compute the weakly connected components.
 *
 * \param               graph   The graph to compute on, should be an *undirected* graph.
 * \param   [in,out]    label   the ParallelVector to store wcc_label.
 */
void WCCCore(OlapBase<Empty>& graph, ParallelVector<size_t>& label);
