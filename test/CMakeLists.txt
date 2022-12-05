add_executable(status_test status_test.cc)
target_link_libraries(status_test PRIVATE Iceberg::Iceberg GTest::GTest GTest::Main)
add_test(NAME status_test COMMAND status_test)

add_executable(result_test result_test.cc)
target_link_libraries(result_test PRIVATE Iceberg::Iceberg GTest::GTest GTest::Main)
add_test(NAME result_test COMMAND result_test)

add_executable(field_and_type_test field_and_type_test.cc)
target_link_libraries(field_and_type_test PRIVATE Iceberg::Iceberg GTest::GTest GTest::Main)
add_test(NAME field_and_type_test COMMAND field_and_type_test)

add_subdirectory(util)
