#include "transaction/lock_manager.h"

namespace huadb {

bool LockManager::LockTable(xid_t xid, LockType lock_type, oid_t oid) {
  // 对数据表加锁，成功加锁返回 true，如果数据表已被其他事务加锁，且锁的类型不相容，返回 false
  // 如果本事务已经持有该数据表的锁，根据需要升级锁的类型
  // LAB 3 BEGIN
  auto it = table_locks_.find(oid);
  if (it != table_locks_.end()) {
    for (const auto &[other_xid, other_lock] : it->second) {
      if (other_xid != xid && !Compatible(other_lock, lock_type)) {
        return false;
      }
    }
    auto self = it->second.find(xid);
    if (self != it->second.end()) {
      self->second = Upgrade(self->second, lock_type);
    } else {
      it->second[xid] = lock_type;
    }
  } else {
    table_locks_[oid][xid] = lock_type;
  }
  return true;
}

bool LockManager::LockRow(xid_t xid, LockType lock_type, oid_t oid, Rid rid) {
  // 对数据行加锁，成功加锁返回 true，如果数据行已被其他事务加锁，且锁的类型不相容，返回 false
  // 如果本事务已经持有该数据行的锁，根据需要升级锁的类型
  // LAB 3 BEGIN
  auto it = row_locks_.find(rid);
  if (it != row_locks_.end()) {
    for (const auto &[other_xid, other_lock] : it->second) {
      if (other_xid != xid && !Compatible(other_lock, lock_type)) {
        return false;
      }
    }
    auto self = it->second.find(xid);
    if (self != it->second.end()) {
      self->second = Upgrade(self->second, lock_type);
    } else {
      it->second[xid] = lock_type;
    }
  } else {
    row_locks_[rid][xid] = lock_type;
  }
  return true;
}

void LockManager::ReleaseLocks(xid_t xid) {
  // 释放事务 xid 持有的所有锁
  // LAB 3 BEGIN
  for (auto it = table_locks_.begin(); it != table_locks_.end();) {
    it->second.erase(xid);
    if (it->second.empty()) {
      it = table_locks_.erase(it);
    } else {
      ++it;
    }
  }
  for (auto it = row_locks_.begin(); it != row_locks_.end();) {
    it->second.erase(xid);
    if (it->second.empty()) {
      it = row_locks_.erase(it);
    } else {
      ++it;
    }
  }
}

void LockManager::SetDeadLockType(DeadlockType deadlock_type) { deadlock_type_ = deadlock_type; }

bool LockManager::Compatible(LockType type_a, LockType type_b) const {
  // 判断锁是否相容
  // LAB 3 BEGIN
  static const bool matrix[5][5] = {
      //     IS               IX               S                SIX              X
      /*IS*/ {true,            true,            true,            true,            false},
      /*IX*/ {true,            true,            false,           false,           false},
      /*S */ {true,            false,           true,            false,           false},
      /*SIX*/{true,            false,           false,           false,           false},
      /*X */ {false,           false,           false,           false,           false},
  };
  return matrix[static_cast<int>(type_a)][static_cast<int>(type_b)];
}

LockType LockManager::Upgrade(LockType self, LockType other) const {
  // 升级锁类型
  // LAB 3 BEGIN
  static const LockType matrix[5][5] = {
      //       IS              IX              S               SIX             X
      /*IS*/ {LockType::IS,   LockType::SIX,  LockType::S,    LockType::SIX,  LockType::X},
      /*IX*/ {LockType::IX,   LockType::IX,   LockType::SIX,  LockType::SIX,  LockType::X},
      /*S */ {LockType::S,    LockType::SIX,  LockType::S,    LockType::SIX,  LockType::X},
      /*SIX*/{LockType::SIX,  LockType::SIX,  LockType::SIX,  LockType::SIX,  LockType::X},
      /*X */ {LockType::X,    LockType::X,    LockType::X,    LockType::X,    LockType::X},
  };
  return matrix[static_cast<int>(self)][static_cast<int>(other)];
}

}  // namespace huadb
