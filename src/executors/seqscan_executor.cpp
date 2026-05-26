#include "executors/seqscan_executor.h"

namespace huadb {

SeqScanExecutor::SeqScanExecutor(ExecutorContext &context, std::shared_ptr<const SeqScanOperator> plan)
    : Executor(context, {}), plan_(std::move(plan)) {}

void SeqScanExecutor::Init() {
  auto table = context_.GetCatalog().GetTable(plan_->GetTableOid());
  scan_ = std::make_unique<TableScan>(context_.GetBufferPool(), table, Rid{table->GetFirstPageId(), 0});
}

std::shared_ptr<Record> SeqScanExecutor::Next() {
  std::unordered_set<xid_t> active_xids;
  // 根据隔离级别，获取活跃事务的 xid（通过 context_ 获取需要的信息）
  // 通过 context_ 获取正确的锁，加锁失败时抛出异常
  // LAB 3 BEGIN
  if (context_.GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
      context_.GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    active_xids = context_.GetTransactionManager().GetSnapshot(context_.GetXid());
  } else {
    active_xids = context_.GetTransactionManager().GetActiveTransactions();
  }
  if (!context_.IsModificationSql() &&
      !context_.GetLockManager().LockTable(context_.GetXid(), LockType::IS, plan_->GetTableOid())) {
    throw DbException("Cannot acquire lock");
  }
  return scan_->GetNextRecord(context_.GetXid(), context_.GetIsolationLevel(), context_.GetCid(), active_xids);
}

}  // namespace huadb
