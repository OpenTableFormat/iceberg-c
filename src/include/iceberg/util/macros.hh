#pragma once

#define ICEBERG_STRINGIFY(x) #x
#define ICEBERG_CONCAT(x, y) x##y

#ifndef ICEBERG_DISALLOW_COPY_AND_ASSIGN
#define ICEBERG_DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&) = delete;              \
  void operator=(const TypeName&) = delete
#endif

#ifndef ICEBERG_DEFAULT_MOVE_AND_ASSIGN
#define ICEBERG_DEFAULT_MOVE_AND_ASSIGN(TypeName) \
  TypeName(TypeName&&) = default;                 \
  TypeName& operator=(TypeName&&) = default
#endif

#define ICEBERG_UNUSED(x) (void)(x)
#define ICEBERG_ARG_UNUSED(x)

//
// GCC can be told that a certain branch is not likely to be token (for
// instance, a CHECK failure), and use that information in static analysis.
// Giving it this information can help it optimize for the common case in
// the absense of better information (i.e. -fprofile-arcs)
#if defined(__GNUC__)
#define ICEBERG_PREDICT_FALSE(x) (__builtin_expect(!!(x), 0))
#define ICEBERG_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#define ICEBERG_NORETURN __attribute__((noreturn))
#define ICERERG_NOINLINE __attribute__((noinline))
#define ICEBERG_PREFETCH(addr) __builtin_prefetch(addr)
#elif defined(__MSC_VER)
#define ICEBERG_PREDICT_FALSE(x) (x)
#define ICEBERG_PREDICT_TRUE(x) (x)
#define ICEBERG_NORETURN __declspec(noreturn)
#define ICERERG_NOINLINE __declspec(noinline)
#define ICEBERG_PREFETCH(addr)
#else
#define ICEBERG_PREDICT_FALSE(x) (x)
#define ICEBERG_PREDICT_TRUE(x) (x)
#define ICEBERG_NORETURN
#define ICERERG_NOINLINE
#define ICEBERG_PREFETCH(addr)
#endif

#if defined(__GNUC__) || defined(__clang__) || defined(_MSC_VER)
#define ICEBERG_RESTRICT __restrict
#else
#define ICEBERG_RESTRICT
#endif