#include "executors/merge_join_executor.h"

namespace huadb {

MergeJoinExecutor::MergeJoinExecutor(ExecutorContext &context, std::shared_ptr<const MergeJoinOperator> plan,
                                     std::shared_ptr<Executor> left, std::shared_ptr<Executor> right)
    : Executor(context, {std::move(left), std::move(right)}), plan_(std::move(plan)) {}

void MergeJoinExecutor::Init() {
  children_[0]->Init();
  children_[1]->Init();

  current_left_ = children_[0]->Next();
  current_right_ = children_[1]->Next();

  left_records_.clear();
  right_records_.clear();
  left_index_ = right_index_ = 0;
}

std::shared_ptr<Record> MergeJoinExecutor::Next() {
  auto clear = [&] {
    left_records_.clear();
    right_records_.clear();
    left_index_ = right_index_ = 0;
  };

  auto cmp = [&](const auto &l, const auto &r) {
    auto a = plan_->left_key_->Evaluate(l);
    auto b = plan_->right_key_->Evaluate(r);

    if (a.Equal(b)) return 0;
    return a.Less(b) ? -1 : 1;
  };

  auto emit = [&]() -> std::shared_ptr<Record> {
    if (left_index_ >= left_records_.size() ||
        right_index_ >= right_records_.size()) {
      return nullptr;
    }

    auto rec = std::make_shared<Record>(*left_records_[left_index_]);
    rec->Append(*right_records_[right_index_]);

    if (++right_index_ == right_records_.size()) {
      right_index_ = 0;
      ++left_index_;
    }

    return rec;
  };

  auto align = [&] {
    while (current_left_ && current_right_) {
      int o = cmp(current_left_, current_right_);

      if (o < 0) {
        current_left_ = children_[0]->Next();
      } else if (o > 0) {
        current_right_ = children_[1]->Next();
      } else {
        return true;
      }
    }

    return false;
  };

  auto load = [&] {
    auto lk = plan_->left_key_->Evaluate(current_left_);
    auto rk = plan_->right_key_->Evaluate(current_right_);

    for (; current_left_ &&
           lk.Equal(plan_->left_key_->Evaluate(current_left_));
         current_left_ = children_[0]->Next()) {
      left_records_.push_back(current_left_);
    }

    for (; current_right_ &&
           rk.Equal(plan_->right_key_->Evaluate(current_right_));
         current_right_ = children_[1]->Next()) {
      right_records_.push_back(current_right_);
    }

    left_index_ = right_index_ = 0;
  };

  while (true) {
    if (auto rec = emit()) {
      return rec;
    }

    clear();

    if (!align()) {
      return nullptr;
    }

    load();
  }
}

}  // namespace huadb