#include "storage/lru_buffer_strategy.h"

namespace huadb {

LRUBufferStrategy::LRUBufferStrategy(size_t capacity) {
  // nodes_[0] 是 root
  // nodes_[i + 1] 对应 frame i
  nodes_.resize(capacity + 1);

  nodes_[0].prev = 0;
  nodes_[0].next = 0;
  nodes_[0].in_lru = true;
}

void LRUBufferStrategy::Detach(size_t idx) {
  auto prev = nodes_[idx].prev;
  auto next = nodes_[idx].next;

  nodes_[prev].next = next;
  nodes_[next].prev = prev;

  nodes_[idx].prev = 0;
  nodes_[idx].next = 0;
}

void LRUBufferStrategy::PushFront(size_t idx) {
  auto first = nodes_[0].next;

  nodes_[idx].prev = 0;
  nodes_[idx].next = first;

  nodes_[first].prev = idx;
  nodes_[0].next = idx;

  if (nodes_[0].prev == 0) {
    nodes_[0].prev = idx;
  }

  nodes_[idx].in_lru = true;
}

void LRUBufferStrategy::Access(size_t frame_no) {
  auto idx = frame_no + 1;

  if (idx >= nodes_.size()) {
    throw std::out_of_range("LRUBufferStrategy::Access frame_no out of range");
  }

  if (nodes_[idx].in_lru) {
    Detach(idx);
  }

  PushFront(idx);
}

size_t LRUBufferStrategy::Evict() {
  auto idx = nodes_[0].prev;

  if (idx == 0) {
    throw std::runtime_error("LRUBufferStrategy::Evict empty buffer");
  }

  Detach(idx);
  nodes_[idx].in_lru = false;

  return idx - 1;
}

}  // namespace huadb