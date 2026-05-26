#pragma once

#include <cstdint>
#include <vector>

#include "executors/executor.h"
#include "operators/orderby_operator.h"

namespace huadb {

class OrderByExecutor : public Executor {
 public:
  OrderByExecutor(ExecutorContext &context, std::shared_ptr<const OrderByOperator> plan,
                  std::shared_ptr<Executor> child);
  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const OrderByOperator> plan_;
  std::vector<std::shared_ptr<Record>> records_;
  size_t index_ = 0;
  bool sorted_ = false;
};

}  // namespace huadb
