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
