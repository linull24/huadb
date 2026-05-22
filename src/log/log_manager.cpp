#include "log/log_manager.h"

#include "common/exceptions.h"
#include "log/log_records/log_records.h"

namespace huadb {

namespace {

template <typename LogT>
std::shared_ptr<LogT> AsLog(const std::shared_ptr<LogRecord> &log) {
  return std::dynamic_pointer_cast<LogT>(log);
}

template <typename LogT>
TablePageid LogPage(const std::shared_ptr<LogRecord> &log) {
  auto typed_log = AsLog<LogT>(log);
  return {typed_log->GetOid(), typed_log->GetPageId()};
}

bool TryGetLogPage(const std::shared_ptr<LogRecord> &log, TablePageid &page) {
  switch (log->GetType()) {
    case LogType::INSERT:
      page = LogPage<InsertLog>(log);
      return true;
    case LogType::DELETE:
      page = LogPage<DeleteLog>(log);
      return true;
    case LogType::NEW_PAGE:
      page = LogPage<NewPageLog>(log);
      return true;
    default:
      return false;
  }
}

}  // namespace

LogManager::LogManager(Disk &disk, TransactionManager &transaction_manager, lsn_t next_lsn)
    : disk_(disk), transaction_manager_(transaction_manager), next_lsn_(next_lsn), flushed_lsn_(next_lsn - 1) {}

void LogManager::SetBufferPool(std::shared_ptr<BufferPool> buffer_pool) { buffer_pool_ = std::move(buffer_pool); }

void LogManager::SetCatalog(std::shared_ptr<Catalog> catalog) { catalog_ = std::move(catalog); }

lsn_t LogManager::GetNextLSN() const { return next_lsn_; }

void LogManager::Clear() {
  std::unique_lock lock(log_buffer_mutex_);
  log_buffer_.clear();
}

void LogManager::Flush() { Flush(NULL_LSN); }

void LogManager::SetDirty(oid_t oid, pageid_t page_id, lsn_t lsn) {
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
}

lsn_t LogManager::AppendInsertLog(xid_t xid, oid_t oid, pageid_t page_id, slotid_t slot_id, db_size_t offset,
                                  db_size_t size, char *new_record) {
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendInsertLog)");
  }
  auto log = std::make_shared<InsertLog>(NULL_LSN, xid, att_.at(xid), oid, page_id, slot_id, offset, size, new_record);
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  att_[xid] = lsn;
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
  return lsn;
}

lsn_t LogManager::AppendDeleteLog(xid_t xid, oid_t oid, pageid_t page_id, slotid_t slot_id) {
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendDeleteLog)");
  }
  auto log = std::make_shared<DeleteLog>(NULL_LSN, xid, att_.at(xid), oid, page_id, slot_id);
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  att_[xid] = lsn;
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
  return lsn;
}

lsn_t LogManager::AppendNewPageLog(xid_t xid, oid_t oid, pageid_t prev_page_id, pageid_t page_id) {
  if (xid != DDL_XID && att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendNewPageLog)");
  }
  xid_t log_xid;
  if (xid == DDL_XID) {
    log_xid = NULL_XID;
  } else {
    log_xid = att_.at(xid);
  }
  auto log = std::make_shared<NewPageLog>(NULL_LSN, xid, log_xid, oid, prev_page_id, page_id);
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);

  if (xid != DDL_XID) {
    att_[xid] = lsn;
  }
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
  if (prev_page_id != NULL_PAGE_ID && dpt_.find({oid, prev_page_id}) == dpt_.end()) {
    dpt_[{oid, prev_page_id}] = lsn;
  }
  return lsn;
}

lsn_t LogManager::AppendBeginLog(xid_t xid) {
  if (att_.find(xid) != att_.end()) {
    throw DbException(std::to_string(xid) + " already exists in att");
  }
  auto log = std::make_shared<BeginLog>(NULL_LSN, xid, NULL_LSN);
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  att_[xid] = lsn;
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  return lsn;
}

lsn_t LogManager::AppendCommitLog(xid_t xid) {
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendCommitLog)");
  }
  auto log = std::make_shared<CommitLog>(NULL_LSN, xid, att_.at(xid));
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  Flush(lsn);
  att_.erase(xid);
  return lsn;
}

lsn_t LogManager::AppendRollbackLog(xid_t xid) {
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendRollbackLog)");
  }
  auto log = std::make_shared<RollbackLog>(NULL_LSN, xid, att_.at(xid));
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  Flush(lsn);
  att_.erase(xid);
  return lsn;
}

lsn_t LogManager::Checkpoint(bool async) {
  auto begin_checkpoint_log = std::make_shared<BeginCheckpointLog>(NULL_LSN, NULL_XID, NULL_LSN);
  lsn_t begin_lsn = next_lsn_.fetch_add(begin_checkpoint_log->GetSize(), std::memory_order_relaxed);
  begin_checkpoint_log->SetLSN(begin_lsn);
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(begin_checkpoint_log));
  }

  auto end_checkpoint_log = std::make_shared<EndCheckpointLog>(NULL_LSN, NULL_XID, NULL_LSN, att_, dpt_);
  lsn_t end_lsn = next_lsn_.fetch_add(end_checkpoint_log->GetSize(), std::memory_order_relaxed);
  end_checkpoint_log->SetLSN(end_lsn);
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(end_checkpoint_log));
  }
  Flush(end_lsn);
  std::ofstream out(MASTER_RECORD_NAME);
  out << begin_lsn;
  return end_lsn;
}

void LogManager::FlushPage(oid_t table_oid, pageid_t page_id, lsn_t page_lsn) {
  Flush(page_lsn);
  dpt_.erase({table_oid, page_id});
}

void LogManager::Rollback(xid_t xid) {
  // 在 att_ 中查找事务 xid 的最后一条日志的 lsn
  // 依次获取 lsn 的 prev_lsn_，直到 NULL_LSN
  // 根据 lsn 和 flushed_lsn_ 的大小关系，判断日志在 buffer 中还是在磁盘中
  // 若日志在 buffer 中，通过 log_buffer_ 获取日志
  // 若日志在磁盘中，通过 disk_ 读取日志，count 参数可设置为 MAX_LOG_SIZE
  // 通过 LogRecord::DeserializeFrom 函数解析日志
  // 调用日志的 Undo 函数
  // LAB 2 BEGIN

  auto lsn = att_[xid];
  lsn_t undo_next_lsn = NULL_LSN;

  while (lsn != NULL_LSN) {
    std::shared_ptr<LogRecord> log;

    if (lsn > flushed_lsn_) {
      std::unique_lock lock(log_buffer_mutex_);
      for (const auto &record : log_buffer_) {
        if (record->GetLSN() == lsn) {
          log = record;
          break;
        }
      }
    } else {
      auto data = std::make_unique<char[]>(MAX_LOG_SIZE);
      disk_.ReadLog(lsn, MAX_LOG_SIZE, data.get());
      log = LogRecord::DeserializeFrom(lsn, data.get());
    }

    log->Undo(*buffer_pool_, *catalog_, *this, undo_next_lsn);

    undo_next_lsn = lsn;
    lsn = log->GetPrevLSN();
  }
}

void LogManager::Recover() {
  Analyze();
  Redo();
  Undo();
}

void LogManager::IncrementRedoCount() { redo_count_++; }

uint32_t LogManager::GetRedoCount() const { return redo_count_; }

void LogManager::Flush(lsn_t lsn) {
  size_t max_log_size = 0;
  lsn_t max_lsn = NULL_LSN;
  {
    std::unique_lock lock(log_buffer_mutex_);
    for (auto iterator = log_buffer_.cbegin(); iterator != log_buffer_.cend();) {
      const auto &log_record = *iterator;
      // 如果 lsn 为 NULL_LSN，表示 log_buffer_ 中所有日志都需要刷盘
      if (lsn != NULL_LSN && log_record->GetLSN() > lsn) {
        iterator++;
        continue;
      }
      auto log_size = log_record->GetSize();
      auto log = std::make_unique<char[]>(log_size);
      log_record->SerializeTo(log.get());
      disk_.WriteLog(log_record->GetLSN(), log_size, log.get());
      if (max_lsn == NULL_LSN || log_record->GetLSN() > max_lsn) {
        max_lsn = log_record->GetLSN();
        max_log_size = log_size;
      }
      iterator = log_buffer_.erase(iterator);
    }
  }
  // 如果 max_lsn 为 NULL_LSN，表示没有日志刷盘
  // 如果 flushed_lsn_ 为 NULL_LSN，表示还没有日志刷过盘
  if (max_lsn != NULL_LSN && (flushed_lsn_ == NULL_LSN || max_lsn > flushed_lsn_)) {
    flushed_lsn_ = max_lsn;
    lsn_t next_lsn = FIRST_LSN;
    if (disk_.FileExists(NEXT_LSN_NAME)) {
      std::ifstream in(NEXT_LSN_NAME);
      in >> next_lsn;
    }
    if (flushed_lsn_ + max_log_size > next_lsn) {
      std::ofstream out(NEXT_LSN_NAME);
      out << (flushed_lsn_ + max_log_size);
    }
  }
}

void LogManager::Analyze() {
  // 恢复 Master Record 等元信息
  // 恢复故障时正在使用的数据库
  if (disk_.FileExists(NEXT_LSN_NAME)) {
    std::ifstream in(NEXT_LSN_NAME);
    lsn_t next_lsn;
    in >> next_lsn;
    next_lsn_ = next_lsn;
  } else {
    next_lsn_ = FIRST_LSN;
  }
  flushed_lsn_ = next_lsn_ - 1;
  lsn_t checkpoint_lsn = 0;

  if (disk_.FileExists(MASTER_RECORD_NAME)) {
    std::ifstream in(MASTER_RECORD_NAME);
    in >> checkpoint_lsn;
  }
  // 根据 Checkpoint 日志恢复脏页表、活跃事务表等元信息
  // 必要时调用 transaction_manager_.SetNextXid 来恢复事务 id
  // LAB 2 BEGIN
  att_.clear();
  dpt_.clear();

  auto read_log_at = [this](lsn_t lsn) {
    auto data = std::make_unique<char[]>(MAX_LOG_SIZE);
    disk_.ReadLog(lsn, MAX_LOG_SIZE, data.get());
    return LogRecord::DeserializeFrom(lsn, data.get());
  };
  auto update_active_transaction = [this](xid_t xid, lsn_t lsn) {
    if (xid != NULL_XID) {
      att_[xid] = lsn;
      transaction_manager_.SetNextXid(xid + 1);
    }
  };
  auto add_dirty_page = [this](oid_t oid, pageid_t page_id, lsn_t lsn) {
    if (page_id != NULL_PAGE_ID) {
      dpt_.try_emplace({oid, page_id}, lsn);
    }
  };
  auto add_log_dirty_pages = [&add_dirty_page](const std::shared_ptr<LogRecord> &log, lsn_t lsn) {
    TablePageid page;
    if (!TryGetLogPage(log, page)) {
      return;
    }
    add_dirty_page(page.table_oid_, page.page_id_, lsn);
    if (log->GetType() == LogType::NEW_PAGE) {
      auto new_page_log = AsLog<NewPageLog>(log);
      add_dirty_page(page.table_oid_, new_page_log->GetPrevPageId(), lsn);
    }
  };

  lsn_t lsn = checkpoint_lsn == 0 ? FIRST_LSN : checkpoint_lsn;
  while (lsn < next_lsn_) {
    auto log = read_log_at(lsn);
    auto xid = log->GetXid();

    switch (log->GetType()) {
      case LogType::BEGIN:
        update_active_transaction(xid, lsn);
        break;
      case LogType::COMMIT:
      case LogType::ROLLBACK:
        if (xid != NULL_XID) {
          transaction_manager_.SetNextXid(xid + 1);
        }
        att_.erase(xid);
        break;
      case LogType::INSERT:
      case LogType::DELETE:
      case LogType::NEW_PAGE:
        update_active_transaction(xid, lsn);
        add_log_dirty_pages(log, lsn);
        break;
      case LogType::END_CHECKPOINT: {
        auto end_checkpoint_log = AsLog<EndCheckpointLog>(log);
        att_ = end_checkpoint_log->GetATT();
        dpt_ = end_checkpoint_log->GetDPT();
        break;
      }
      case LogType::BEGIN_CHECKPOINT:
        break;
    }

    lsn += log->GetSize();
  }
}

void LogManager::Redo() {
  // 正序读取日志，调用日志记录的 Redo 函数
  // LAB 2 BEGIN
  if (dpt_.empty()) {
    return;
  }

  auto read_log_at = [this](lsn_t lsn) {
    auto data = std::make_unique<char[]>(MAX_LOG_SIZE);
    disk_.ReadLog(lsn, MAX_LOG_SIZE, data.get());
    return LogRecord::DeserializeFrom(lsn, data.get());
  };
  auto is_dirty_page_log = [this](oid_t oid, pageid_t page_id, lsn_t lsn) {
    auto it = dpt_.find({oid, page_id});
    return it != dpt_.end() && it->second <= lsn;
  };
  auto should_redo_log = [&is_dirty_page_log](const std::shared_ptr<LogRecord> &log, lsn_t lsn) {
    TablePageid page;
    if (!TryGetLogPage(log, page)) {
      return false;
    }
    return is_dirty_page_log(page.table_oid_, page.page_id_, lsn);
  };

  lsn_t lsn = next_lsn_;
  for (const auto &[_, rec_lsn] : dpt_) {
    if (rec_lsn < lsn) {
      lsn = rec_lsn;
    }
  }

  while (lsn < next_lsn_) {
    auto log = read_log_at(lsn);
    if (should_redo_log(log, lsn)) {
      log->Redo(*buffer_pool_, *catalog_, *this);
    }

    lsn += log->GetSize();
  }
}

void LogManager::Undo() {
  // 根据活跃事务表，将所有活跃事务回滚
  // LAB 2 BEGIN
  auto active_transactions = att_;
  for (const auto &[xid, _] : active_transactions) {
    Rollback(xid);
    att_.erase(xid);
  }
}

}  // namespace huadb
