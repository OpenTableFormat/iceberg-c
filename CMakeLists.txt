cmake_minimum_required(VERSION 3.12)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(APPLE)
  set(CMAKE_CXX_COMPILER clang++)
  set(CMAKE_C_COMPILER clang)
  set(CMAKE_ASM_COMPILER clang)
endif()

project(
  iceberg-c
  VERSION 0.0.1
  DESCRIPTION "A C/C++ library of Apache Iceberg"
  HOMEPAGE_URL "https://github.com/OpenTableFormat/iceberg-c"
  LANGUAGES CXX C ASM)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules/")

# CCache
option(WITH_CCACHE "Build with ccache.")
if(WITH_CCACHE)
  if(CMAKE_C_COMPILER_LAUNCHER OR CMAKE_CXX_COMPILER_LAUNCHER)
    message(WARNING "Compiler launcher already set. stop configuring ccache")
  else()
    find_program(CCACHE_EXECUTABLE ccache)
    if(NOT CCACHE_EXECUTABLE)
      message(FATAL_ERROR "Can't find ccache. Is it installed?")
    endif()
    message(STATUS "Building with ccache: ${CCACHE_EXECUTABLE}, CCACHE_DIR=$ENV{CCACHE_DIR}")
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE_EXECUTABLE})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_EXECUTABLE})
  endif()
endif(WITH_CCACHE)

# Snappy compression library required by arrow
find_package(snappy REQUIRED)

# Boost
option(WITH_SYSTEM_BOOST "require and build with system Boost" OFF)
set(BOOST_COMPONENTS
    atomic
    chrono
    thread
    system
    regex
    random
    program_options
    date_time
    iostreams
    context
    coroutine)
set(BOOST_HEADER_COMPONENTS container)

if(WITH_SYSTEM_BOOST)
  set(Boost_USE_STATIC_LIBS ON)
  find_package(
    Boost
    COMPONENTS ${BOOST_COMPONENTS}
    REQUIRED)
else()
  include(ProcessorCount)
  ProcessorCount(DEFAULT_BOOST_J)
  if(DEFAULT_BOOST_J EQUAL 0)
    set(DEFAULT_BOOST_J 1)
  endif()
  set(BOOST_J
      ${DEFAULT_BOOST_J}
      CACHE STRING "max jobs for Boost build") # override w/-DBOOST_J=<n>
  set(Boost_USE_STATIC_LIBS ON)
  include(BuildBoost)
  build_boost(
    1.79
    COMPONENTS
    ${BOOST_COMPONENTS}
    ${BOOST_HEADER_COMPONENTS})
endif()
include_directories(BEFORE SYSTEM ${Boost_INCLUDE_DIRS})

option(ICEBERG_WITH_BACKTRACE "Build with backtrace support" ON)
option(ENABLE_TESTING "Enable the tests" ON)
option(WITH_SYSTEM_UTF8PROC "Use system-provided utf8proc" OFF)

add_subdirectory(third-party)
add_subdirectory(src)

if(ENABLE_TESTING)
  enable_testing()
  add_subdirectory(test)
endif()
