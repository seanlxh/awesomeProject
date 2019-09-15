cmake_minimum_required(VERSION 3.12)
project(miniecho)

set(CMAKE_CXX_STANDARD 11)

add_executable(miniecho main.cpp tiny.cpp csapp.h csapp.cpp)