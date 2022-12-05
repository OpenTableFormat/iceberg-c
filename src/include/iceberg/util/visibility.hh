#pragma once

#ifndef ICEBERG_EXPORT
#define ICEBERG_EXPORT __attribute__((visibility("default")))
#endif
#ifndef ICEBERG_NO_EXPORT
#define ICEBERG_NO_EXPORT __attribute__((visibility("hidden")))
#endif
