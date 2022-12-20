#include "iceberg/partitioning.hh"

namespace iceberg {
namespace table {

bool PartitionField::Equals(const PartitionField& other) const {
  return source_id_ == other.field_id_ && field_id_ == other.field_id_ &&
         name_ == other.name_;
}

bool PartitionField::Equals(const std::shared_ptr<PartitionField>& other) const {
  if (other == nullptr) {
    return false;
  }

  return Equals(*other);
}

PartitionSpec::PartitionSpec(std::shared_ptr<Schema> schema, int32_t spec_id,
                             std::vector<std::shared_ptr<PartitionField>> fields,
                             int32_t last_assigned_field_id_)
    : schema_(std::move(schema)),
      spec_id_(spec_id),
      fields_(std::move(fields)),
      last_assigned_field_id_(last_assigned_field_id_) {
  for (auto& field : fields_) {
    source_id_to_fields.emplace(field->source_id(), field);
  }
}

std::shared_ptr<StructType> PartitionSpec::partitionType() {
  std::vector<std::shared_ptr<Field>> fields;

  for (auto& pf : fields_) {
    auto f = schema()->field(pf->source_id());
    auto t = pf->getTransform()->getResultType(f->type());
    fields.push_back(f->WithType(t));
  }

  return std::make_shared<StructType>(fields);
}

}  // namespace table
}  // namespace iceberg