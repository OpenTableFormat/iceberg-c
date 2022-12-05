#include <gtest/gtest.h>

#include "iceberg/field.hh"
#include "iceberg/type.hh"
#include "iceberg/util/macros.hh"

namespace iceberg {

TEST(TestField, Basics) {
  Field f0("f0", integer_());
  Field f0_nn("f0", integer_(), false);

  ASSERT_EQ(f0.name(), "f0");
  ASSERT_EQ(f0.type()->ToString(), integer_()->ToString());

  ASSERT_TRUE(f0.nullable());
  ASSERT_FALSE(f0_nn.nullable());
}

TEST(TestField, ToString) {
  auto f0 = field_("f0", integer_(), false);
  std::string result = f0->ToString();
  std::string expected = "f0: integer not null";
  ASSERT_EQ(expected, result);
}

TEST(TestField, Equals) {
  Field f0("f0", long_());
  Field f0_nn("f0", long_(), false);
  Field f0_other("f0", long_());

  ASSERT_EQ(f0, f0_other);
  ASSERT_NE(f0, f0_nn);
}

#define PRIMITIVE_TEST(KLASS, ENUM, NAME)                 \
  TEST(TestTypes, ICEBERG_CONCAT(TestPrimitive_, ENUM)) { \
    KLASS tp;                                             \
                                                          \
    ASSERT_EQ(tp.id(), Type::ENUM);                       \
    ASSERT_EQ(tp.ToString(), std::string(NAME));          \
  }

PRIMITIVE_TEST(BooleanType, BOOLEAN, "bool");
PRIMITIVE_TEST(IntegerType, INTEGER, "integer");
PRIMITIVE_TEST(LongType, LONG, "long");
PRIMITIVE_TEST(FloatType, FLOAT, "float");
PRIMITIVE_TEST(DoubleType, DOUBLE, "double");
PRIMITIVE_TEST(TimestampType, TIMESTAMP, "timestamp[ns]");
PRIMITIVE_TEST(StringType, STRING, "string");
PRIMITIVE_TEST(BinaryType, BINARY, "binary");

TEST(TestTypes, TestDateType) {
  DateType t1;
  ASSERT_EQ(t1.id(), Type::DATE);
  ASSERT_EQ("date[day]", t1.ToString());
  ASSERT_EQ(32, t1.bit_width());
}

TEST(TestTypes, TestTimeType) {
  TimeType t1;
  ASSERT_EQ(t1.id(), Type::TIME);
  ASSERT_EQ("time[ns]", t1.ToString());
  ASSERT_EQ(64, t1.bit_width());
}

TEST(TestTypes, TestTimestampType) {
  TimestampType t1;
  TimestampType t2("US/Eastern");
  ASSERT_EQ(t1.id(), Type::TIMESTAMP);
  ASSERT_EQ(t2.id(), Type::TIMESTAMP);
  ASSERT_EQ("timestamp[ns]", t1.ToString());
  ASSERT_EQ("timestamp[ns, tz=US/Eastern]", t2.ToString());
  ASSERT_EQ(64, t1.bit_width());
}

TEST(TestTypes, TestUUIDType) {
  UUIDType t1;
  ASSERT_EQ(t1.id(), Type::UUID);
  ASSERT_EQ("uuid", t1.ToString());
  ASSERT_EQ(128, t1.bit_width());
}

TEST(TestTypes, TestFixedType) {
  FixedType t1(10);
  ASSERT_EQ(t1.id(), Type::FIXED);
  ASSERT_EQ("fixed[10]", t1.ToString());
  ASSERT_EQ(80, t1.bit_width());
}

TEST(TestTypes, TestDecimalType) {
  DecimalType t1(8, 4);
  EXPECT_EQ(t1.id(), Type::DECIMAL);
  EXPECT_EQ(t1.ToString(), std::string("decimal(8, 4)"));
  EXPECT_EQ(t1.bit_width(), 128);
}

TEST(TestTypes, TestStructType) {
  auto f0_type = integer_();
  auto f0 = field_("f0", f0_type);

  auto f1_type = string_();
  auto f1 = field_("f1", f1_type);

  auto f2_type = long_();
  auto f2 = field_("f2", f2_type);

  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};

  StructType struct_type(fields);

  ASSERT_TRUE(struct_type.field(0)->Equals(f0));
  ASSERT_TRUE(struct_type.field(1)->Equals(f1));
  ASSERT_TRUE(struct_type.field(2)->Equals(f2));

  ASSERT_EQ(struct_type.ToString(), "struct<f0: integer, f1: string, f2: long>");

  std::shared_ptr<Field> result = struct_type.GetFieldByName("f1");
  ASSERT_EQ(f1, result);

  result = struct_type.GetFieldByName("f2");
  ASSERT_EQ(f2, result);

  result = struct_type.GetFieldByName("not-found");
  ASSERT_EQ(result, nullptr);

  ASSERT_EQ(0, struct_type.GetFieldIndex(f0->name()));
  ASSERT_EQ(1, struct_type.GetFieldIndex(f1->name()));
  ASSERT_EQ(2, struct_type.GetFieldIndex(f2->name()));
}

TEST(TestTypes, TestListType) {
  std::shared_ptr<DataType> vt = std::make_shared<LongType>();

  ListType list_type(vt);
  ASSERT_EQ(list_type.id(), Type::LIST);

  ASSERT_EQ("list<item: long>", list_type.ToString());

  std::shared_ptr<DataType> st = std::make_shared<StringType>();
  std::shared_ptr<DataType> lt = std::make_shared<ListType>(st);
  ASSERT_EQ("list<item: string>", lt->ToString());

  ListType lt2(lt);
  ASSERT_EQ("list<item: list<item: string>>", lt2.ToString());
}

TEST(TestTypes, TestMapType) {
  std::shared_ptr<DataType> kt = std::make_shared<StringType>();
  std::shared_ptr<DataType> it = std::make_shared<IntegerType>();

  MapType map_type(kt, it);
  ASSERT_EQ(map_type.id(), Type::MAP);
  ASSERT_EQ("map<string, integer>", map_type.ToString());

  ASSERT_EQ(map_type.key_type()->id(), kt->id());
  ASSERT_EQ(map_type.item_type()->id(), it->id());
  ASSERT_EQ(map_type.value_type()->id(), Type::STRUCT);
}

}  // namespace iceberg