add_executable(localfs_test localfs_test.cc)
target_link_libraries(localfs_test PRIVATE Iceberg::Iceberg GTest::GTest GTest::Main)
add_test(NAME localfs_test COMMAND localfs_test)
