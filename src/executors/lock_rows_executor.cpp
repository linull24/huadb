#include "executors/lock_rows_executor.h"

namespace huadb {

LockRowsExecutor::LockRowsExecutor(ExecutorContext &context, std::shared_ptr<const LockRowsOperator> plan,
                                   std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void LockRowsExecutor::Init() { children_[0]->Init(); }

std::shared_ptr<Record> LockRowsExecutor::Next() {
  auto record = children_[0]->Next();
  if (record == nullptr) {
    return nullptr;
  }
  // 根据 plan_ 的 lock type 获取正确的锁，加锁失败时抛出异常
  // LAB 3 BEGIN
  LockType lock_type;
  if (plan_->GetLockType() == SelectLockType::UPDATE) {
    lock_type = LockType::X;
    if (!context_.GetLockManager().LockTable(context_.GetXid(), LockType::IX, plan_->GetOid())) {
      throw DbException("Cannot acquire lock");
    }
  } else {
    lock_type = LockType::S;
  }
  if (!context_.GetLockManager().LockRow(context_.GetXid(), lock_type, plan_->GetOid(), record->GetRid())) {
    throw DbException("Cannot acquire lock");
  }
  return record;
}

}  // namespace huadb
