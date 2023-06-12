//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size_, global_depth_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::ComputeIndex(const K &key, int depth) -> size_t {
  int mask = (1 << depth) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  latch_.lock();
  size_t ind = IndexOf(key);
  if (ind >= dir_.size()) {
    latch_.unlock();
    return false;
  }
  std::shared_ptr<Bucket> bucket = dir_[ind];
  bool res = bucket->Find(key, value);
  latch_.unlock();
  return res;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  latch_.lock();
  size_t ind = IndexOf(key);
  if (ind >= dir_.size()) {
    latch_.unlock();
    return false;
  }
  std::shared_ptr<Bucket> bucket = dir_[ind];
  bool res = bucket->Remove(key);
  latch_.unlock();
  return res;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  latch_.lock();
  while (true) {
    size_t ind = IndexOf(key);
    //    std::cout << "Ind:  " << ind << std::endl;
    //    std::cout << "key:  " << key << std::endl;

    bool insert_success = dir_[ind]->Insert(key, value);
    if (insert_success) {
      break;
    }

    RedistributeBucket(dir_[ind]);
  }
  //  std::cout << "===========================" << std::endl;

  latch_.unlock();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  //    std::cout << "in Redistribute" << std::endl;
  int local_depth = bucket->GetDepth();
  std::list<std::pair<K, V>> &kv_pairs = bucket->GetItems();

  local_depth += 1;
  size_t dir_sz = dir_.size();
  if (local_depth > GetGlobalDepthInternal()) {
    //    int mask = (1 << global_depth_) - 1;
    global_depth_ += 1;
    //    dir_.resize(dir_sz * 2);

    //    for (size_t i = dir_sz; i < dir_sz * 2; ++i) {
    //      dir_[i] = dir_[(i & mask)];
    //    }

    for (size_t i = 0; i < dir_sz; ++i) {
      dir_.push_back(dir_[i]);
    }
  }

  // approach #1:
  std::shared_ptr<Bucket> new_bucket = std::make_shared<Bucket>(bucket_size_, local_depth);
  size_t pre_ind = ComputeIndex(kv_pairs.begin()->first, local_depth - 1);
  for (auto it = kv_pairs.begin(); it != kv_pairs.end();) {
    size_t ind = ComputeIndex(it->first, local_depth);
    //      std::cout << " KEY: " << it->first << " Ind: "<< ind << "||" ;

    if (ind != pre_ind) {
      new_bucket->Insert(it->first, it->second);
      //        std::cout << " REMOVE: " << it->first << std::endl ;
      it = kv_pairs.erase(it);
    } else {
      it++;
    }
  }
  for (size_t i = 0; i < dir_.size(); ++i) {
    if ((i & ((1 << (local_depth - 1)) - 1)) == pre_ind && (i & ((1 << local_depth) - 1)) != pre_ind) {
      dir_[i] = new_bucket;
    }
  }
  num_buckets_++;
  bucket->IncrementDepth();

  //  // approach #2:
  //  for (auto it = kv_pairs.begin(); it != kv_pairs.end();) {
  //    size_t ind = ComputeIndex(it->first, local_depth);
  //    if (dir_[ind]->GetDepth() == local_depth) {
  //      dir_[ind]->Insert(it->first, it->second);
  //    } else {
  //      dir_[ind] = std::make_shared<Bucket>(bucket_size_, local_depth);
  //      dir_[ind]->Insert(it->first, it->second);
  //    }
  //    it = kv_pairs.erase(it);
  //  }
  //  num_buckets_++;
  //
  ////  for (size_t i = 0; i < dir_.size(); ++i) {
  ////    if (dir_[i] == bucket) {
  ////      bucket->IncrementDepth();
  ////      return;
  ////    }
  ////  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &kv_pair : list_) {
    if (kv_pair.first == key) {
      value = kv_pair.second;
      return true;
    }
  }
  value = V();
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }
  if (!IsFull()) {
    list_.push_back(std::make_pair(key, value));
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
