#include "iceberg/schema.hh"

#include <sstream>

namespace iceberg {

Schema::Schema(std::vector<std::shared_ptr<Field>>& fields)
    : Schema(DEFAULT_SCHEMA_ID, fields) {}

Schema::Schema(int32_t schema_id, std::vector<std::shared_ptr<Field>>& fields)
    : Schema(schema_id, std::make_shared<StructType>(fields)) {}

Schema::Schema(const Schema& schema) : Schema(schema.schema_id_, schema.struct_) {}

bool Schema::Equals(const Schema& other) const {
  if (this == &other) {
    return true;
  }

  // checks field equality
  if (num_fields() != other.num_fields()) {
    return false;
  }

  // field-by-field comparison
  for (int i = 0; i < num_fields(); ++i) {
    if (!field(i)->Equals(*other.field(i).get())) {
      return false;
    }
  }

  return true;
}

bool Schema::Equals(const std::shared_ptr<Schema>& other) const {
  if (other == nullptr) {
    return false;
  }

  return Equals(*other);
}

int Schema::num_fields() const { return struct_->num_fields(); }

int32_t Schema::schema_id() const { return schema_id_; }

const std::shared_ptr<Field>& Schema::field(int i) const { return struct_->field(i); }

const std::vector<std::shared_ptr<Field>>& Schema::fields() const {
  return struct_->fields();
}

std::shared_ptr<Field> Schema::GetFieldByName(const std::string& name) const {
  return struct_->GetFieldByName(name);
}

std::vector<std::shared_ptr<Field>> Schema::GetAllFieldsByName(
    const std::string& name) const {
  return struct_->GetAllFieldsByName(name);
}

int Schema::GetFieldIndex(const std::string& name) const {
  return struct_->GetFieldIndex(name);
}

std::vector<int> Schema::GetAllFieldIndices(const std::string& name) const {
  return struct_->GetAllFieldIndices(name);
}

Status Schema::CanReferenceFieldsByNames(const std::vector<std::string>& names) const {
  for (const auto& name : names) {
    if (GetFieldByName(name) == nullptr) {
      return Status::Invalid("Field named '", name,
                             "' not found or not unique in the schema.");
    }
  }

  return Status::OK();
}

std::string Schema::ToString() const {
  std::stringstream ss;

  ss << "schema_id: " << schema_id_ << std::endl;
  ss << struct_->ToString();

  return ss.str();
}

Result<std::shared_ptr<Schema>> Schema::AddField(
    int i, const std::shared_ptr<Field>& field) const {
  auto res = struct_->AddField(i, field);
  if (res.ok()) {
    return std::make_shared<Schema>(DEFAULT_SCHEMA_ID, res.ValueUnsafe());
  }

  return res.status();
}

Result<std::shared_ptr<Schema>> Schema::SetField(
    int i, const std::shared_ptr<Field>& field) const {
  auto res = struct_->SetField(i, field);
  if (res.ok()) {
    return std::make_shared<Schema>(DEFAULT_SCHEMA_ID, res.ValueUnsafe());
  }

  return res.status();
}

Result<std::shared_ptr<Schema>> Schema::RemoveField(int i) const {
  auto res = struct_->RemoveField(i);
  if (res.ok()) {
    return std::make_shared<Schema>(DEFAULT_SCHEMA_ID, res.ValueUnsafe());
  }

  return res.status();
}

// Factory functions for schemas
std::shared_ptr<Schema> schema_(std::vector<std::shared_ptr<Field>> fields) {
  return std::make_shared<Schema>(fields);
}

std::shared_ptr<Schema> schema_(int32_t schema_id,
                                std::vector<std::shared_ptr<Field>> fields) {
  return std::make_shared<Schema>(schema_id, fields);
}

}  // namespace iceberg