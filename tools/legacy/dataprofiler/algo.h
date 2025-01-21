/**
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
