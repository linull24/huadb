#include "table/table_scan.h"

#include "table/table_page.h"

namespace huadb {

TableScan::TableScan(BufferPool &buffer_pool, std::shared_ptr<Table> table, Rid rid)
    : buffer_pool_(buffer_pool), table_(std::move(table)), rid_(rid) {}

std::shared_ptr<Record> TableScan::GetNextRecord(xid_t xid, IsolationLevel isolation_level, cid_t cid,
                                                 const std::unordered_set<xid_t> &active_xids) {
  // 根据事务隔离级别及活跃事务集合，判断记录是否可见
  // LAB 3 BEGIN

  // 每次调用读取一条记录
  // 读取时更新 rid_ 变量，避免重复读取
  // 扫描结束时，返回空指针
  // 注意处理扫描空表的情况（rid_.page_id_ 为 NULL_PAGE_ID）
  // LAB 1 BEGIN

  struct Step {
    bool has_value;
    std::shared_ptr<Record> record;
    Rid next_rid;
  };
  auto make_rid = [] (pageid_t page_id, int slot_id) -> Rid {
    Rid rid;
    rid.page_id_ = page_id;
    rid.slot_id_ = slot_id;
    return rid;
  };
  auto next_non_empty_page = [] (BufferPool &buffer_pool,
                                const std::shared_ptr<Table> &table,
                                pageid_t page_id,
                                const auto &make_rid) -> Rid {
    while (page_id != NULL_PAGE_ID) {
      TablePage table_page(
        buffer_pool.GetPage(table->GetDbOid(), table->GetOid(), page_id)
      );
      if (table_page.GetRecordCount() > 0) {
        return make_rid(page_id, 0);
      }
      page_id = table_page.GetNextPageId();
    }
    return make_rid(NULL_PAGE_ID, 0);
  };
  auto scan_one = [] (BufferPool &buffer_pool,
                      const std::shared_ptr<Table> &table,
                      Rid rid,
                      const auto &make_rid,
                      const auto &next_non_empty_page) -> Step {
    if (rid.page_id_ == NULL_PAGE_ID) {
      return Step{false, nullptr, make_rid(NULL_PAGE_ID, 0)};
    }
    TablePage table_page(
      buffer_pool.GetPage(table->GetDbOid(), table->GetOid(), rid.page_id_)
    );
    auto record = table_page.GetRecord(rid, table->GetColumnList());
    Rid next_rid;
    if (rid.slot_id_ + 1 < table_page.GetRecordCount()) {
      next_rid = rid;
      ++next_rid.slot_id_;
    } else {
      next_rid = next_non_empty_page(
        buffer_pool,
        table,
        table_page.GetNextPageId(),
        make_rid
      );
    }
    return Step{true, record, next_rid};
  };
  auto check_visible = [](const std::shared_ptr<Record> &record, xid_t xid, cid_t cid,
                           const std::unordered_set<xid_t> &active_xids, IsolationLevel isolation_level) {
    // deleted
    if (record->GetXmax() != NULL_XID) {
      if (xid == NULL_XID) {
        return false;
      }
      if (record->GetXmax() == xid) {
        return false;
      }
      if (active_xids.find(record->GetXmax()) != active_xids.end()) {
        // still active
      } else if (isolation_level != IsolationLevel::READ_COMMITTED && record->GetXmax() > xid) {
        // RR/SERIALIZABLE
      } else {
        return false;
      }
    }
    if (xid == NULL_XID) {
      return true;
    }
    // halloween 
    if (record->GetXmin() == xid && record->GetCid() == cid) {
      return false;
    }
    // other 
    if (record->GetXmin() != xid && record->GetXmin() != NULL_XID) {
      if (active_xids.find(record->GetXmin()) != active_xids.end()) {
        return false;
      }
      if (isolation_level != IsolationLevel::READ_COMMITTED && record->GetXmin() > xid) {
        return false;
      }
    }
    return true;
  };
  while (true) {
    auto step = scan_one(
      buffer_pool_,
      table_,
      rid_,
      make_rid,
      next_non_empty_page
    );
    if (!step.has_value) {
      rid_ = make_rid(NULL_PAGE_ID, 0);
      return nullptr;
    }
    rid_ = step.next_rid;
    if (!check_visible(step.record, xid, cid, active_xids, isolation_level)) {
      continue;
    }
    return step.record;
  }
}

}  // namespace huadb
