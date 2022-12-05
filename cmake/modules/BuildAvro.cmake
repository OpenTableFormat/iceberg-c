function(build_avro)
  # only build static version
  list(APPEND avro_CMAKE_ARGS -DBUILD_SHARED_LIBS=OFF)

  list(APPEND avro_CMAKE_ARGS -DBUILD_POSITION_INDEPENDENT_LIB=ON)

  # cmake doesn't properly handle arguments containing ";", such as
  # CMAKE_PREFIX_PATH, for which reason we'll have to use some other separator.
  string(
    REPLACE ";"
            "!"
            CMAKE_PREFIX_PATH_ALT_SEP
            "${CMAKE_PREFIX_PATH}")
  list(APPEND avro_CMAKE_ARGS -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH_ALT_SEP})
  if(CMAKE_TOOLCHAIN_FILE)
    list(APPEND avro_CMAKE_ARGS -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE})
  endif()

  list(APPEND avro_CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER})
  list(APPEND avro_CMAKE_ARGS -DCMAKE_AR=${CMAKE_AR})
  list(APPEND avro_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})

  if(NOT WITH_SYSTEM_BOOST)
    # make sure boost submodule builds first, so arrow can find its byproducts
    list(APPEND avro_DEPENDS Boost)
  endif()

  set(avro_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/avro/lang/c++")
  set(avro_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/avro/lang/c++")

  set(avro_INSTALL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/avro/install")
  list(APPEND avro_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${avro_INSTALL_PREFIX})

  set(avro_INSTALL_LIBDIR "lib") # favroe lib so we don't have to guess between lib/lib64
  list(APPEND avro_CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR=${avro_INSTALL_LIBDIR})
  set(avro_LIBRARY_DIR "${avro_INSTALL_PREFIX}/${avro_INSTALL_LIBDIR}")

  set(avro_LIBRARY "${avro_LIBRARY_DIR}/libavrocpp.a")

  set(avro_INCLUDE_DIR "${avro_INSTALL_PREFIX}/include")

  # this include directory won't exist until the install step, but the
  # imported target needs it early for INTERFACE_INCLUDE_DIRECTORIES
  file(MAKE_DIRECTORY "${avro_INCLUDE_DIR}")

  set(avro_BYPRODUCTS ${avro_LIBRARY})

  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    # try to inherit command line arguments passed by parent "make" job
    set(make_cmd $(MAKE))
  else()
    set(make_cmd ${CMAKE_COMMAND} --build <BINARY_DIR>)
  endif()

  # we use an external project and copy the sources to bin directory to ensure
  # that object files are built outside of the source tree.
  include(ExternalProject)
  ExternalProject_Add(
    avro_ext
    SOURCE_DIR "${avro_SOURCE_DIR}"
    CMAKE_ARGS ${avro_CMAKE_ARGS}
    BINARY_DIR "${avro_BINARY_DIR}"
    BUILD_COMMAND "${make_cmd}"
    BUILD_BYPRODUCTS "${avro_BYPRODUCTS}"
    INSTALL_DIR "${avro_INSTALL_PREFIX}"
    DEPENDS "${avro_DEPENDS}"
    LIST_SEPARATOR !)

  add_library(avro::avro STATIC IMPORTED)
  add_dependencies(avro::avro avro_ext)
  set_target_properties(
    avro::avro
    PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${avro_INCLUDE_DIR}"
               INTERFACE_LINK_LIBRARIES "${avro_INTERFACE_LINK_LIBRARIES}"
               IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
               IMPORTED_LOCATION "${avro_LIBRARY}")
endfunction()
