//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "iostream"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  *frame_id = INVALID_PAGE_ID;  // -1
  for (auto revit = history_list_.rbegin(); revit != history_list_.rend(); ++revit) {
    if (evictable_map_[revit->first]) {
      *frame_id = revit->first;
      history_list_.erase(location_[*frame_id]);
      count_map_.erase(*frame_id);
      location_.erase(*frame_id);
      evictable_map_.erase(*frame_id);
      return true;
    }
  }

  //  int curr = static_cast<int>(current_timestamp_);
  //  cache_list_.sort([&curr](const std::pair<frame_id_t, timestamps> &lhs, const std::pair<frame_id_t, timestamps>
  //  &rhs) {
  //    return (curr - lhs.second.front()) < (curr - rhs.second.front());
  //  });

  //    for (auto revit = cache_list_.rbegin(); revit != cache_list_.rend(); ++revit) {
  //    std::cout << revit->second.size() <<std::endl;
  //      for (auto it = revit->second.begin(); it != revit->second.end(); ++it) {
  //      std::cout << "frameID: " << revit->first << " first: " << *it << std::endl;
  //    }
  //      std::cout << "===================" <<std::endl;
  //    }

  //  size_t ts = current_timestamp_;
  //    for (auto revit = cache_list_.rbegin(); revit != cache_list_.rend(); ++revit) {
  //      if (evictable_map_[revit->first] && revit->second.front()<ts) {
  //        *frame_id = revit->first;
  //        ts = revit->second.front();
  //      }
  //    }
  //    if (*frame_id != INVALID_PAGE_ID) {
  //      cache_list_.erase(location_[*frame_id]);
  //      count_map_.erase(*frame_id);
  //      location_.erase(*frame_id);
  //      evictable_map_.erase(*frame_id);
  //      return true;
  //    }
  //    return false;

  cache_list_.sort([](const std::pair<frame_id_t, timestamps> &lhs, const std::pair<frame_id_t, timestamps> &rhs) {
    return lhs.second.front() > rhs.second.front();
  });
  for (auto revit = cache_list_.rbegin(); revit != cache_list_.rend(); ++revit) {
    if (evictable_map_[revit->first]) {
      *frame_id = revit->first;
      cache_list_.erase(location_[*frame_id]);
      count_map_.erase(*frame_id);
      location_.erase(*frame_id);
      evictable_map_.erase(*frame_id);
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  //  std::lock_guard<std::mutex> guard(latch_);
  //  if (frame_id > static_cast<int>(replacer_size_)) {
  //    throw std::runtime_error("RecordAccess(): frame_id out of range!");
  //  }
  //  count_map_[frame_id]++;
  //  if (count_map_[frame_id] == k_) {
  //    if (location_.count(frame_id) != 0) {
  //      auto old_it = location_[frame_id];
  //      cache_list_.push_front(*old_it);
  //      location_[frame_id] = cache_list_.begin();
  //      auto it = location_[frame_id];
  //      timestamps &ts_list = it->second;
  //      ts_list.push_back(current_timestamp_);
  //      history_list_.erase(old_it);
  //
  //      cache_list_.sort([](const std::pair<frame_id_t, timestamps> &lhs, const std::pair<frame_id_t, timestamps>
  // &rhs) { /        return lhs.second.front() > rhs.second.front(); /      }); /    } else { / std::pair<frame_id_t,
  // timestamps> new_timestamps{frame_id, timestamps{current_timestamp_}}; / cache_list_.push_front(new_timestamps);
  //      location_[frame_id] = cache_list_.begin();
  //
  //      evictable_map_[frame_id] = true;
  //    }
  //
  //  } else if (count_map_[frame_id] > k_) {
  //    auto it = location_[frame_id];
  //    timestamps &ts_list = it->second;
  //    ts_list.erase(ts_list.begin());
  //    ts_list.push_back(current_timestamp_);
  //    //    ts_list.front().second = current_timestamp_;
  //    //    int curr = static_cast<int>(current_timestamp_);
  //    //    cache_list_.sort([&curr](const std::pair<frame_id_t, timestamps>& lhs, const std::pair<frame_id_t,
  //    //    timestamps>& rhs) {
  //    //      return (curr-lhs.second.front()) < (curr-rhs.second.front());
  //    //    });
  //
  //    //    int curr = static_cast<int>(current_timestamp_);
  //    cache_list_.sort([](const std::pair<frame_id_t, timestamps> &lhs, const std::pair<frame_id_t, timestamps> &rhs)
  //{ /      return lhs.second.front() > rhs.second.front(); /    });
  //
  //  } else {
  //    if (count_map_[frame_id] == 1) {
  //      std::pair<frame_id_t, timestamps> new_timestamps{frame_id, timestamps{current_timestamp_}};
  //      history_list_.push_front(new_timestamps);
  //      location_[frame_id] = history_list_.begin();
  //      evictable_map_[frame_id] = true;
  //    } else {
  //      auto it = location_[frame_id];
  //      it->second.push_back(current_timestamp_);
  //    }
  //  }
  //  current_timestamp_++;

  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::runtime_error("RecordAccess(): frame_id out of range!");
  }
  count_map_[frame_id]++;
  if (count_map_[frame_id] > k_) {
    auto it = location_[frame_id];
    timestamps &ts_list = it->second;
    ts_list.erase(ts_list.begin());
    ts_list.push_back(current_timestamp_);
    //    cache_list_.sort([](const std::pair<frame_id_t, timestamps> &lhs, const std::pair<frame_id_t, timestamps>
    //    &rhs)
    //{ /      return lhs.second.front() > rhs.second.front(); /    });
    current_timestamp_++;
    return;
  }

  if (count_map_[frame_id] == 1) {
    std::pair<frame_id_t, timestamps> new_timestamps{frame_id, timestamps{}};
    history_list_.push_front(new_timestamps);
    location_[frame_id] = history_list_.begin();
    evictable_map_[frame_id] = true;
  }
  auto it = location_[frame_id];
  it->second.push_back(current_timestamp_);

  if (count_map_[frame_id] == k_) {
    auto old_it = location_[frame_id];
    cache_list_.push_front(*old_it);
    location_[frame_id] = cache_list_.begin();
    history_list_.erase(old_it);

    //      cache_list_.sort([](const std::pair<frame_id_t, timestamps> &lhs, const std::pair<frame_id_t, timestamps>
    // &rhs) { /        return lhs.second.front() > rhs.second.front(); /      });
  }

  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::runtime_error("SetEvictable(): frame_id out of range!");
  }
  //    if (count_map_[frame_id] == 0) {
  //   // "count_map_[frame_id] == 0" will not only do the judgement, but also initiate a key-value(default) for
  //   frame_id
  //    std::cout << "hhhhhhhhhhh" << std::endl;
  //      return;
  //    }
  if (count_map_.count(frame_id) == 0) {
    return;
  }
  //  if (location_.count(frame_id)==0) {
  //    return;
  //  }
  evictable_map_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (count_map_.count(frame_id) == 0) {
    return;
  }
  if (!evictable_map_[frame_id]) {
    throw std::runtime_error("Tried to remove non-evictable frame!");
  }

  if (count_map_[frame_id] >= k_) {
    cache_list_.erase((location_[frame_id]));
  } else {
    history_list_.erase(location_[frame_id]);
  }
  evictable_map_.erase(frame_id);
  count_map_.erase(frame_id);
  location_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  size_t sz = 0;
  for (auto it : evictable_map_) {
    if (it.second) {
      sz++;
    }
  }
  return sz;
}

}  // namespace bustub
