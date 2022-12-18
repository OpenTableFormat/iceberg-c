add_executable(local_file_io_test local_file_io_test.cc)
target_link_libraries(local_file_io_test PRIVATE Iceberg::Iceberg GTest::GTest GTest::Main)
add_test(NAME local_file_io_test COMMAND local_file_io_test)
