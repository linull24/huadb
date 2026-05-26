#include "executors/limit_executor.h"

namespace huadb {

LimitExecutor::LimitExecutor(ExecutorContext &context, std::shared_ptr<const LimitOperator> plan,
                             std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void LimitExecutor::Init() {
  children_[0]->Init();
  current_pos_ = 0;
  offset_ = plan_->limit_offset_.value_or(0);
  uint32_t count = plan_->limit_count_.value_or(UINT32_MAX);
  if (count > UINT32_MAX - offset_) {
    max_pos_ = UINT32_MAX;
  } else {
    max_pos_ = offset_ + count;
  }
}

std::shared_ptr<Record> LimitExecutor::Next() {
  while (current_pos_ < offset_) {
    if (!children_[0]->Next()) {
      return nullptr;
    }
    current_pos_++;
  }

  if (current_pos_ >= max_pos_) {
    return nullptr;
  }

  auto record = children_[0]->Next();
  if (record) {
    current_pos_++;
  }
  return record;
}

}  // namespace huadb
