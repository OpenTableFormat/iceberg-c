#pragma once

namespace iceberg {
#define ICEBERG_GENERATE_FOR_ALL_TYPES(ACTION) \
  ACTION(Boolean);                             \
  ACTION(Integer);                             \
  ACTION(Long);                                \
  ACTION(Float);                               \
  ACTION(Double);                              \
  ACTION(Date);                                \
  ACTION(Time);                                \
  ACTION(Timestamp);                           \
  ACTION(String);                              \
  ACTION(UUID);                                \
  ACTION(Fixed);                               \
  ACTION(Binary);                              \
  ACTION(Decimal);                             \
  ACTION(Struct);                              \
  ACTION(List);                                \
  ACTION(Map);
}  // namespace iceberg