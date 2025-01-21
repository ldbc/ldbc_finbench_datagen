====
    Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
====

cmake_minimum_required(VERSION 3.2)
project(Profiler C CXX)

set(TUGRAPH_HOME /home/qsp/project/tugraph-db/)

add_executable(
  profiler_standalone
  profiler.cpp
  de_core.cpp
  wcc_core.cpp
  ${TUGRAPH_HOME}/src/lgraph_api/olap_base.cpp
  ${TUGRAPH_HOME}/src/lgraph_api/lgraph_utils.cpp
  ${TUGRAPH_HOME}/src/lgraph_api/olap_profile.cpp)
target_link_libraries(profiler_standalone -static-libstdc++ libstdc++fs.a
                      libgomp.a pthread dl)
target_include_directories(
  profiler_standalone PUBLIC ${TUGRAPH_HOME}/deps/fma-common/
                             ${TUGRAPH_HOME}/include ${TUGRAPH_HOME}/src)
