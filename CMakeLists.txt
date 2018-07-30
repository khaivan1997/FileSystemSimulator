cmake_minimum_required(VERSION 2.8.8)

project(SimGridExampleProject) # TODO: give a real name to your project here

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")
find_package(SimGrid REQUIRED)
include_directories(${SimGrid_INCLUDE_DIR})

add_executable(my_simulator s4u-app-pingpong.cpp)
target_link_libraries(my_simulator ${SimGrid_LIBRARY})
