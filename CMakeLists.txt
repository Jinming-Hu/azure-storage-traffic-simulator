cmake_minimum_required(VERSION 3.11)
set(CMAKE_CXX_STANDARD 14)

project(azure-sdk-for-cpp-perf)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake-modules")

find_package(Git REQUIRED)
include(FetchContent)
include(automate-vcpkg)

vcpkg_bootstrap()
vcpkg_install_packages(azure-storage-blobs-cpp)

set(SIMULATOR_SOURCES
    src/network_simulator.cc
)

add_executable(simulator ${SIMULATOR_SOURCES})

if(WIN32)
  target_compile_definitions(simulator PRIVATE NOMINMAX)
endif()

if(MSVC)
  target_compile_options(simulator PUBLIC /W4 /WX /MP)
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL
                                                "GNU")
  target_compile_options(simulator PUBLIC -Wall -Wextra -Werror -pedantic)
  target_compile_options(simulator PRIVATE -O3)
endif()

find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
target_link_libraries(
  simulator
  Azure::azure-storage-blobs)