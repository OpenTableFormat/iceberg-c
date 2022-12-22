add_library(iceberg_header INTERFACE)
target_include_directories(iceberg_header INTERFACE include)

add_library(iceberg_objs OBJECT)
target_sources(
  iceberg_objs
  PRIVATE status.cc
          result.cc
          field.cc
          type.cc
          schema.cc
          transform.cc
          partitioning.cc
          snapshot.cc
          table.cc
          io/file_io.cc
          io/local_file_io.cc
          util/logging.cc
          util/string_builder.cc
          util/murmur_hash3.cc)
target_link_libraries(iceberg_objs PRIVATE iceberg_header)

add_library(iceberg STATIC)
target_link_libraries(iceberg PRIVATE iceberg_objs)
target_include_directories(iceberg INTERFACE $<TARGET_PROPERTY:iceberg_header,INTERFACE_INCLUDE_DIRECTORIES>)

add_library(Iceberg::Iceberg ALIAS iceberg)
