cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(FileSystemSimulator) # TODO: give a real name to your project here

# Search for SimGrid
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")
find_package(SimGrid 3.27 REQUIRED) # This template requires SimGrid v3.27 because we use templated get()

include_directories(${SimGrid_INCLUDE_DIR})

include_directories(src)

# Glob all source files from src directory
file(GLOB_RECURSE SRC_FILES "src/*.cpp" "src/*.c")

add_executable(FileSystemSimulator main.cpp ${SRC_FILES})
target_link_libraries(FileSystemSimulator ${SimGrid_LIBRARY})
