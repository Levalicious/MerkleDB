cmake_minimum_required(VERSION 3.14)
project(MerkleDB C)

set(CMAKE_C_STANDARD 11)

set(CMAKE_C_FLAGS "-O3")

add_executable(MerkleDB main.c blake2/blake2-impl.h blake2/blake2.h blake2/blake2b.c blake2/blake2bp.c merkledb.c merkledb.h)