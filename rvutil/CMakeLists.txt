# CMakeList.txt : CMake project for rvutil, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)
set(CMAKE_CXX_STANDARD 17)

# Add source to this project's executable.
add_executable (rvutil "main.cpp" "rvutil.hpp" "pbo.hpp")

# TODO: Add tests and install targets if needed.
