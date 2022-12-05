#include <type_traits>
#include <typeinfo>

#include <gtest/gtest.h>

#include "iceberg/util/checked_cast.hh"

namespace iceberg {
namespace internal {

class Foo {
 public:
  virtual ~Foo() = default;
};

class Bar {};
class FooSub : public Foo {};
template <typename T>
class Baz : public Foo {};

TEST(CheckedCast, TestInvalidSubclassCast) {
  static_assert(std::is_polymorphic<Foo>::value, "Foo is not polymorphic");

  Foo foo;
  FooSub foosub;
  const Foo& foosubref = foosub;
  Baz<double> baz;
  const Foo& bazref = baz;

#ifndef NDEBUG  // debug mode
  // illegal pointer cast
  ASSERT_EQ(nullptr, checked_cast<Bar*>(&foo));

  // illegal reference cast
  ASSERT_THROW(checked_cast<const Bar&>(foosubref), std::bad_cast);

  // legal reference casts
  ASSERT_NO_THROW(checked_cast<const FooSub&>(foosubref));
  ASSERT_NO_THROW(checked_cast<const Baz<double>&>(bazref));
#else  // release mode
  // failure modes for the invalid casts occur at compile time

  // legal pointer cast
  ASSERT_NE(nullptr, checked_cast<const FooSub*>(&foosubref));

  // legal reference casts: this is static_cast in a release build, so ASSERT_NO_THROW
  // doesn't make a whole lot of sense here.
  auto& x = checked_cast<const FooSub&>(foosubref);
  ASSERT_EQ(&foosubref, &x);

  auto& y = checked_cast<const Baz<double>&>(bazref);
  ASSERT_EQ(&bazref, &y);
#endif
}

}  // namespace internal
}  // namespace iceberg