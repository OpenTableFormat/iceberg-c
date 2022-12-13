function(build_orc)
  # only build static version
  list(APPEND orc_CMAKE_ARGS -DBUILD_SHARED_LIBS=OFF)

  # disable java build
  list(APPEND orc_CMAKE_ARGS -DBUILD_JAVA=OFF)

  # disable CPP_TESTS
  list(APPEND orc_CMAKE_ARGS -DBUILD_CPP_TESTS=OFF)

  # diable BUILD_TOOLS
  list(APPEND orc_CMAKE_ARGS -DBUILD_TOOLS=OFF)

  list(APPEND orc_CMAKE_ARGS -DBUILD_POSITION_INDEPENDENT_LIB=ON)

  # cmake doesn't properly handle arguments containing ";", such as
  # CMAKE_PREFIX_PATH, for which reason we'll have to use some other separator.
  string(
    REPLACE ";"
            "!"
            CMAKE_PREFIX_PATH_ALT_SEP
            "${CMAKE_PREFIX_PATH}")
  list(APPEND orc_CMAKE_ARGS -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH_ALT_SEP})
  if(CMAKE_TOOLCHAIN_FILE)
    list(APPEND orc_CMAKE_ARGS -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE})
  endif()

  list(APPEND orc_CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER})
  list(APPEND orc_CMAKE_ARGS -DCMAKE_AR=${CMAKE_AR})
  list(APPEND orc_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})

  set(orc_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/orc")
  set(orc_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/orc")

  set(orc_INSTALL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/orc/install")
  list(APPEND orc_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${orc_INSTALL_PREFIX})

  set(orc_INSTALL_LIBDIR "lib") # force lib so we don't have to guess between lib/lib64
  list(APPEND orc_CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR=${orc_INSTALL_LIBDIR})
  set(orc_LIBRARY_DIR "${orc_INSTALL_PREFIX}/${orc_INSTALL_LIBDIR}")

  set(orc_LIBRARY "${orc_LIBRARY_DIR}/liborc.a")

  set(orc_INCLUDE_DIR "${orc_INSTALL_PREFIX}/include")

  # this include directory won't exist until the install step, but the
  # imported target needs it early for INTERFACE_INCLUDE_DIRECTORIES
  file(MAKE_DIRECTORY "${orc_INCLUDE_DIR}")

  set(orc_BYPRODUCTS ${orc_LIBRARY})

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
    orc_ext
    SOURCE_DIR "${orc_SOURCE_DIR}"
    CMAKE_ARGS ${orc_CMAKE_ARGS}
    BINARY_DIR "${orc_BINARY_DIR}"
    BUILD_COMMAND "${make_cmd}"
    BUILD_BYPRODUCTS "${orc_BYPRODUCTS}"
    INSTALL_DIR "${orc_INSTALL_PREFIX}"
    DEPENDS "${orc_DEPENDS}"
    LIST_SEPARATOR !)

  add_library(orc::orc STATIC IMPORTED)
  add_dependencies(orc::orc orc_ext)
  set_target_properties(
    orc::orc
    PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${orc_INCLUDE_DIR}"
               INTERFACE_LINK_LIBRARIES "${orc_INTERFACE_LINK_LIBRARIES}"
               IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
               IMPORTED_LOCATION "${orc_LIBRARY}")
endfunction()
