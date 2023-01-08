cmake_minimum_required(VERSION 3.10)

project(my_project)

# Abseil requires C++14
set(CMAKE_CXX_STANDARD 20)

# Import Abseil's CMake targets
find_package(absl REQUIRED)

add_executable(a hello_world.cc)

# Declare dependency on the absl::strings library
target_link_libraries(a absl::strings)
target_link_libraries(a absl::time)
target_link_libraries(a absl::log)