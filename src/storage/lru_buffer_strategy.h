#pragma once

#include <vector>

#include "common/constants.h"
#include "storage/buffer_strategy.h"

namespace huadb {

class LRUBufferStrategy : public BufferStrategy {
 public:
  LRUBufferStrategy(size_t capacity = BUFFER_SIZE);

  void Access(size_t frame_no) override;
  size_t Evict() override;

 private:
  struct Node {
    size_t prev = 0;
    size_t next = 0;
    bool in_lru = false;
  };

  void Detach(size_t idx);
  void PushFront(size_t idx);

  std::vector<Node> nodes_;
};

}  // namespace huadb
