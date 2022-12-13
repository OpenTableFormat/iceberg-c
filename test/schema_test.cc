#include <gtest/gtest.h>

#include "iceberg/schema.hh"
#include "iceberg/type.hh"

namespace iceberg {

using TestSchema = ::testing::Test;

TEST_F(TestSchema, Basics) {
  auto f0 = field_("f0", integer_());
  auto f1 = field_("f1", long_(), false);
  auto f1_optional = field_("f1", long_());

  auto f2 = field_("f2", long_());

  auto schema = ::iceberg::schema_({f0, f1, f2});

  ASSERT_EQ(3, schema->num_fields());
  ASSERT_EQ(*f0, *schema->field(0));
  ASSERT_EQ(*f1, *schema->field(1));
  ASSERT_EQ(*f2, *schema->field(2));

  auto schema2 = ::iceberg::schema_({f0, f1, f2});

  ASSERT_EQ(*schema, *schema2);

  std::vector<std::shared_ptr<Field>> fields3 = {f0, f1_optional, f2};
  auto schema3 = std::make_shared<Schema>(fields3);

  ASSERT_NE(*schema, *schema3);
}

TEST_F(TestSchema, ToString) {
  auto f0 = field_("f0", integer_());
  auto f1 = field_("f1", long_(), false);
  auto f2 = field_("f2", string_());
  auto f3 = field_("f3", list_(integer_()));

  auto schema = ::iceberg::schema_({f0, f1, f2, f3});
  std::string result = schema->ToString();
  std::string expected = R"(schema_id: 0
struct<f0: integer, f1: long not null, f2: string, f3: list<item: integer>>)";
  ASSERT_EQ(expected, result);
}

TEST_F(TestSchema, GetFieldByName) {
  auto f0 = field_("f0", integer_());
  auto f1 = field_("f1", uuid_(), false);
  auto f2 = field_("f2", binary_());
  auto f3 = field_("f3", list_(date_()));

  auto schema = schema_({f0, f1, f2, f3});

  std::shared_ptr<Field> result;
  result = schema->GetFieldByName("f1");
  ASSERT_EQ(*result, *f1);

  result = schema->GetFieldByName("f3");
  ASSERT_EQ(*result, *f3);

  result = schema->GetFieldByName("not-found");
  ASSERT_EQ(result, nullptr);
}

TEST_F(TestSchema, GetFieldIndex) {
  auto f0 = field_("f0", integer_());
  auto f1 = field_("f1", uuid_(), false);
  auto f2 = field_("f2", binary_());
  auto f3 = field_("f3", list_(date_()));

  auto schema = schema_({f0, f1, f2, f3});

  ASSERT_EQ(0, schema->GetFieldIndex(f0->name()));
  ASSERT_EQ(1, schema->GetFieldIndex(f1->name()));
  ASSERT_EQ(2, schema->GetFieldIndex(f2->name()));
  ASSERT_EQ(3, schema->GetFieldIndex(f3->name()));
  ASSERT_EQ(-1, schema->GetFieldIndex("not-found"));
}

TEST_F(TestSchema, GetFieldDuplicates) {
  auto f0 = field_("f0", integer_());
  auto f1 = field_("f1", uuid_(), false);
  auto f2 = field_("f2", binary_());
  auto f3 = field_("f1", list_(date_()));

  auto schema = schema_({f0, f1, f2, f3});

  ASSERT_EQ(0, schema->GetFieldIndex(f0->name()));
  ASSERT_EQ(-1, schema->GetFieldIndex(f1->name()));  // duplicate
  ASSERT_EQ(2, schema->GetFieldIndex(f2->name()));
  ASSERT_EQ(-1, schema->GetFieldIndex("not-found"));

  ASSERT_EQ(std::vector<int>{0}, schema->GetAllFieldIndices(f0->name()));
  ASSERT_EQ(std::vector<int>({1, 3}), schema->GetAllFieldIndices(f1->name()));

  std::vector<std::shared_ptr<Field>> results;

  results = schema->GetAllFieldsByName(f0->name());
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(*results[0], *f0);

  results = schema->GetAllFieldsByName(f1->name());
  ASSERT_EQ(results.size(), 2);

  results = schema->GetAllFieldsByName("not-found");
  ASSERT_EQ(results.size(), 0);
}

TEST_F(TestSchema, CanReferenceFieldsByNames) {
  auto f0 = field_("f0", integer_());
  auto f1 = field_("f1", uuid_(), false);
  auto f2 = field_("f2", binary_());
  auto f3 = field_("f1", list_(date_()));

  auto schema = schema_({f0, f1, f2, f3});

  ASSERT_TRUE(schema->CanReferenceFieldsByNames({"f0", "f2"}).ok());
  ASSERT_TRUE(schema->CanReferenceFieldsByNames({"f2", "f0"}).ok());
}

}  // namespace iceberg