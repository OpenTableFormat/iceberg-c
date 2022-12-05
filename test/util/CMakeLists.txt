add_executable(logging_test logging_test.cc)
target_link_libraries(logging_test PRIVATE Iceberg::Iceberg GTest::GTest GTest::Main)
add_test(NAME logging_test COMMAND logging_test)

add_executable(checked_cast_test checked_cast_test.cc)
target_link_libraries(checked_cast_test PRIVATE Iceberg::Iceberg GTest::GTest GTest::Main)
add_test(NAME checked_cast_test COMMAND checked_cast_test)
