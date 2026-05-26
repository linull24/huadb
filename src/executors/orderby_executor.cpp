#include "executors/orderby_executor.h"

#include <algorithm>

namespace huadb {

OrderByExecutor::OrderByExecutor(ExecutorContext &context, std::shared_ptr<const OrderByOperator> plan,
                                 std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void OrderByExecutor::Init() {
  children_[0]->Init();
  records_.clear();
  index_ = 0;
  sorted_ = false;
}

std::shared_ptr<Record> OrderByExecutor::Next() {
  if (!sorted_) {
    while (auto record = children_[0]->Next()) {
      records_.push_back(record);
    }
    std::sort(records_.begin(), records_.end(), [this](const auto &a, const auto &b) {
      for (const auto &[type, expr] : plan_->order_bys_) {
        auto va = expr->Evaluate(a);
        auto vb = expr->Evaluate(b);
        if (va.Equal(vb)) {
          continue;
        }
        if (type == OrderByType::DESC) {
          return vb.Less(va);
        }
        return va.Less(vb);
      }
      return false;
    });
    sorted_ = true;
  }
  if (index_ >= records_.size()) {
    return nullptr;
  }
  return records_[index_++];
}

}  // namespace huadb
