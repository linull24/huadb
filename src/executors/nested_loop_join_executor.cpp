#include "executors/nested_loop_join_executor.h"

namespace huadb {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext &context,
                                               std::shared_ptr<const NestedLoopJoinOperator> plan,
                                               std::shared_ptr<Executor> left, std::shared_ptr<Executor> right)
    : Executor(context, {std::move(left), std::move(right)}), plan_(std::move(plan)) {}

void NestedLoopJoinExecutor::Init() {
  children_[0]->Init();
  children_[1]->Init();
  current_left_ = nullptr;
  right_index_ = 0;
  right_records_.clear();
  while (auto record = children_[1]->Next()) {
    right_records_.push_back(record);
  }
}

std::shared_ptr<Record> NestedLoopJoinExecutor::Next() {
  // 从 NestedLoopJoinOperator 中获取连接条件
  // 使用 OperatorExpression 的 EvaluateJoin 函数判断是否满足 join 条件
  // 使用 Record 的 Append 函数进行记录的连接
  // LAB 4 BEGIN
  auto match = [this](const auto &left, const auto &right) {
    const auto value = plan_->join_condition_->EvaluateJoin(left, right);
    return !value.IsNull() && value.template GetValue<bool>();
  };
  auto join = [](const auto &left, const auto &right) {
    auto record = std::make_shared<Record>(*left);
    record->Append(*right);
    return record;
  };

  while (true) {
    if (!current_left_) {
      current_left_ = children_[0]->Next();
      right_index_ = 0;
      if (!current_left_) {
        return nullptr;
      }
    }

    while (right_index_ < right_records_.size()) {
      auto right = right_records_[right_index_++];
      if (match(current_left_, right)) {
        return join(current_left_, right);
      }
    }

    current_left_ = nullptr;
  }
}

}  // namespace huadb
