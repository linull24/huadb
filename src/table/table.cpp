#include "table/table.h"

#include "table/table_page.h"

namespace huadb {

Table::Table(BufferPool &buffer_pool, LogManager &log_manager, oid_t oid, oid_t db_oid, ColumnList column_list,
             bool new_table, bool is_empty)
    : buffer_pool_(buffer_pool),
      log_manager_(log_manager),
      oid_(oid),
      db_oid_(db_oid),
      column_list_(std::move(column_list)) {
  if (new_table || is_empty) {
    first_page_id_ = NULL_PAGE_ID;
  } else {
    first_page_id_ = 0;
  }
}

Rid Table::InsertRecord(std::shared_ptr<Record> record, xid_t xid, cid_t cid, bool write_log) {
  if (record->GetSize() > MAX_RECORD_SIZE) {
    throw DbException("Record size too large: " + std::to_string(record->GetSize()));
  }

  // 当 write_log 参数为 true 时开启写日志功能
  // 在插入记录时增加写 InsertLog 过程
  // 在创建新的页面时增加写 NewPageLog 过程
  // 设置页面的 page lsn
  // LAB 2 BEGIN

  // 使用 buffer_pool_ 获取页面
  // 使用 TablePage 类操作记录页面
  // 遍历表的页面，判断页面是否有足够的空间插入记录，如果没有则通过 buffer_pool_ 创建新页面
  // 如果 first_page_id_ 为 NULL_PAGE_ID，说明表还没有页面，需要创建新页面
  // 创建新页面时需设置前一个页面的 next_page_id，并将新页面初始化
  // 找到空间足够的页面后，通过 TablePage 插入记录
  // 返回插入记录的 rid
  // LAB 1 BEGIN
  struct InsertTarget {
    pageid_t page_id;
    std::unique_ptr<TablePage> table_page;
  };
  auto get_table_page = [this](pageid_t page_id) {
    return std::make_unique<TablePage>(buffer_pool_.GetPage(db_oid_, oid_, page_id));
  };
  auto new_table_page = [this](pageid_t page_id) {
    auto table_page = std::make_unique<TablePage>(buffer_pool_.NewPage(db_oid_, oid_, page_id));
    table_page->Init();
    return table_page;
  };
  auto append_new_page_log = [this, xid, write_log](pageid_t prev_page_id, pageid_t page_id, TablePage *prev_table_page,
                                                    TablePage &table_page) {
    if (!write_log) {
      return;
    }
    auto lsn = log_manager_.AppendNewPageLog(xid, oid_, prev_page_id, page_id);
    table_page.SetPageLSN(lsn);
    if (prev_table_page != nullptr) {
      prev_table_page->SetPageLSN(lsn);
    }
  };
  auto append_insert_log = [this, xid, write_log](const std::shared_ptr<Record> &record, pageid_t page_id,
                                                  slotid_t slot_id, TablePage &table_page) {
    if (!write_log) {
      return;
    }
    auto raw_record = std::make_unique<char[]>(record->GetSize());
    record->SerializeTo(raw_record.get());
    auto lsn = log_manager_.AppendInsertLog(xid, oid_, page_id, slot_id, table_page.GetUpper(), record->GetSize(),
                                            raw_record.get());
    table_page.SetPageLSN(lsn);
  };
  auto find_insert_target = [this, record_size = record->GetSize(), &get_table_page, &new_table_page,
                             &append_new_page_log] {
    if (first_page_id_ == NULL_PAGE_ID) {
      pageid_t page_id = 0;
      auto table_page = new_table_page(page_id);
      first_page_id_ = page_id;
      append_new_page_log(NULL_PAGE_ID, page_id, nullptr, *table_page);
      return InsertTarget{page_id, std::move(table_page)};
    }

    auto page_id = first_page_id_;
    auto table_page = get_table_page(page_id);
    while (table_page->GetFreeSpaceSize() < record_size && table_page->GetNextPageId() != NULL_PAGE_ID) {
      page_id = table_page->GetNextPageId();
      table_page = get_table_page(page_id);
    }

    if (table_page->GetFreeSpaceSize() >= record_size) {
      return InsertTarget{page_id, std::move(table_page)};
    }

    auto prev_page_id = page_id;
    auto new_page_id = prev_page_id + 1;
    table_page->SetNextPageId(new_page_id);

    auto new_page = new_table_page(new_page_id);
    append_new_page_log(prev_page_id, new_page_id, table_page.get(), *new_page);
    return InsertTarget{new_page_id, std::move(new_page)};
  };
  auto insert_into_target = [xid, cid, &append_insert_log](const std::shared_ptr<Record> &record,
                                                           InsertTarget &target) {
    auto slot_id = target.table_page->InsertRecord(record, xid, cid);
    Rid rid{target.page_id, slot_id};
    record->SetRid(rid);
    append_insert_log(record, target.page_id, slot_id, *target.table_page);
    return rid;
  };
  auto target = find_insert_target();
  return insert_into_target(record, target);
}

void Table::DeleteRecord(const Rid &rid, xid_t xid, bool write_log) {
  // 增加写 DeleteLog 过程
  // 设置页面的 page lsn
  // LAB 2 BEGIN

  // 使用 TablePage 操作页面
  // LAB 1 BEGIN
  auto append_delete_log = [this, xid, write_log](const Rid &rid, TablePage &table_page) {
    if (!write_log) {
      return;
    }
    auto lsn = log_manager_.AppendDeleteLog(xid, oid_, rid.page_id_, rid.slot_id_);
    table_page.SetPageLSN(lsn);
  };
  TablePage table_page(buffer_pool_.GetPage(db_oid_, oid_, rid.page_id_));
  append_delete_log(rid, table_page);
  table_page.DeleteRecord(rid.slot_id_, xid);
}

Rid Table::UpdateRecord(const Rid &rid, xid_t xid, cid_t cid, std::shared_ptr<Record> record, bool write_log) {
  DeleteRecord(rid, xid, write_log);
  return InsertRecord(record, xid, cid, write_log);
}

void Table::UpdateRecordInPlace(const Record &record) {
  auto rid = record.GetRid();
  auto table_page = std::make_unique<TablePage>(buffer_pool_.GetPage(db_oid_, oid_, rid.page_id_));
  table_page->UpdateRecordInPlace(record, rid.slot_id_);
}

pageid_t Table::GetFirstPageId() const { return first_page_id_; }

oid_t Table::GetOid() const { return oid_; }

oid_t Table::GetDbOid() const { return db_oid_; }

const ColumnList &Table::GetColumnList() const { return column_list_; }

}  // namespace huadb
