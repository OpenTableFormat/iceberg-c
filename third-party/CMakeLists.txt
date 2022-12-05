include(BuildUtf8proc)
build_utf8proc()

# arrow contains parquet library
include(BuildArrow)
build_arrow()

include(BuildORC)
build_orc()

include(BuildAvro)
build_avro()

if(ENABLE_TESTING)
  add_subdirectory(googletest)
  add_library(GMock::GMock ALIAS gmock)
  add_library(GMock::Main ALIAS gmock_main)
  target_include_directories(gmock INTERFACE $<TARGET_PROPERTY:gtest,INTERFACE_INCLUDE_DIRECTORIES>)
  target_include_directories(gmock_main INTERFACE $<TARGET_PROPERTY:gtest,INTERFACE_INCLUDE_DIRECTORIES>)
  add_library(GTest::GTest ALIAS gtest)
  add_library(GTest::Main ALIAS gtest_main)
endif()
