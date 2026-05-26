#pragma once

#include <vector>

#include "executors/executor.h"
#include "operators/merge_join_operator.h"

namespace huadb {

class MergeJoinExecutor : public Executor {
 public:
  MergeJoinExecutor(ExecutorContext &context, std::shared_ptr<const MergeJoinOperator> plan,
                    std::shared_ptr<Executor> left, std::shared_ptr<Executor> right);
  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const MergeJoinOperator> plan_;
  std::shared_ptr<Record> current_left_;
  std::shared_ptr<Record> current_right_;
  std::vector<std::shared_ptr<Record>> left_records_;
  std::vector<std::shared_ptr<Record>> right_records_;
  size_t left_index_ = 0;
  size_t right_index_ = 0;
};

}  // namespace huadb
