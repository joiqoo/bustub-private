//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  if (free_list_.empty() && replacer_->Size() == 0) {
    page_id = nullptr;
    return nullptr;
  }
  *page_id = AllocatePage();
  frame_id_t frame_id = INVALID_PAGE_ID;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&frame_id);
    if (pages_[frame_id].IsDirty()) {
      FlushPgImpInternal(pages_[frame_id].GetPageId());
    }
    page_table_->Remove(pages_[frame_id].GetPageId());
  }
  page_table_->Insert(*page_id, frame_id);
  // reset the memory and metadata for the new page
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  // pin the frame
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id = INVALID_PAGE_ID;  // -1
  if (!page_table_->Find(page_id, frame_id)) {
    //    std::cout << "Fetch: pageID not found!!!" << std::endl;
    if (free_list_.empty() && replacer_->Size() == 0) {
      return nullptr;
    }
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      replacer_->Evict(&frame_id);
      if (pages_[frame_id].IsDirty()) {
        FlushPgImpInternal(pages_[frame_id].GetPageId());
      }
      page_table_->Remove(pages_[frame_id].GetPageId());
    }
    page_table_->Insert(page_id, frame_id);
    // reset the memory and metadata for the page
    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].is_dirty_ = false;
    // replace the old page in the frame
    char *page_data = pages_[frame_id].GetData();
    disk_manager_->ReadPage(page_id, page_data);
  }
  // pin the frame
  //  page_table_->Find(page_id, frame_id); // added to test
  pages_[frame_id].pin_count_ += 1;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id = INVALID_PAGE_ID;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].GetPinCount() == 0) {
    return false;
  }
  pages_[frame_id].pin_count_ -= 1;
  /**
   * 当有多个线程使用同一个页面时，UnpinPgImp(page_id_t page_id, bool is_dirty) 函数会被每个线程调用一次，
   * 这时如果一个进程修改数据后先调用，另一个进程没有修改数据后调用，如果不做条件判断直接赋值 is_dirty 会发生什么情况？
   *
   * 第一个进程将 page 的 is_dirty 置为 true，第二个又置为 false，本来应该脏的页面现在不脏了。
   * 所以我们只能将 is_dirty 属性由 false 变为 true，而不能由 true 变为 false。
   * */
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  return FlushPgImpInternal(page_id);
}
auto BufferPoolManagerInstance::FlushPgImpInternal(page_id_t page_id) -> bool {
  frame_id_t frame_id = INVALID_PAGE_ID;
  if (!page_table_->Find(page_id, frame_id)) {
    //    std::cout << "FALSE" << std::endl;
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; ++i) {
    FlushPgImp(pages_[i].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id = INVALID_PAGE_ID;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.emplace_back(frame_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
