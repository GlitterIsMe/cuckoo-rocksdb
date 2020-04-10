#include "cuckoo_filter.h"

namespace rocksdb {
    CuckooBucket::CuckooBucket(CuckooSlot *pmem_slot, uint64_t slot_size, bool is_create) :
            pmem_slots_(pmem_slot), slot_size_(slot_size) {
        if (is_create) {
            for (size_t i = 0; i < slot_size_; i++) {
                pmem_slots_[i].status_ = CuckooSlot::AVAILIBLE;
            }
        }
    }

    CuckooFilter::CuckooFilter(PersistentArena *pmem_arena, uint64_t level, uint64_t &block_num) {
        pmem_arena_ = pmem_arena;

        filter_addr_ =
                pmem_arena_->AllocateBlock(level, block_num) + sizeof(AllocatedBlockListNode);
        assert(filter_addr_ != nullptr);

        uint64_t max_filter_size = BLOCK_SIZE - sizeof(AllocatedBlockListNode);
        uint64_t max_slot_num = max_filter_size / sizeof(CuckooSlot);
        bucket_size_ = max_slot_num / SLOT_PER_BUCKET;
        uint64_t slot_num = bucket_size_ * SLOT_PER_BUCKET;
#ifdef PMEM_CUCKOO_DEBUG
        printf("[CuckooFilter]sizeof(AllocatedBlockListNode)=%ld, sizeof(CuckooSlot)=%ld",
                sizeof(AllocatedBlockListNode), sizeof(CuckooSlot));
        printf("[CuckooFilter]bucket_size_: %ld, slot_num: %ld\n", bucket_size_, slot_num);
#endif
        pmem_buckets_ = new CuckooBucket *[bucket_size_];
        char *tmp = filter_addr_;
        for (size_t i = 0; i < bucket_size_; i++) {
            pmem_buckets_[i] = new CuckooBucket((CuckooSlot *) tmp, SLOT_PER_BUCKET);
            tmp += (sizeof(CuckooSlot) * SLOT_PER_BUCKET);
        }
    }

    CuckooFilter::CuckooFilter(PersistentArena *pmem_arena, uint64_t block_num) {
        pmem_arena_ = pmem_arena;

        filter_addr_ = pmem_arena_->GetBlockWithBlockNum(block_num) + sizeof(AllocatedBlockListNode);

        uint64_t max_filter_size = BLOCK_SIZE - sizeof(AllocatedBlockListNode);
        uint64_t max_slot_num = max_filter_size / sizeof(CuckooSlot);
        bucket_size_ = max_slot_num / SLOT_PER_BUCKET;
        uint64_t slot_num = bucket_size_ * SLOT_PER_BUCKET;
#ifdef PMEM_CUCKOO_DEBUG
        printf("[CuckooFilter]sizeof(AllocatedBlockListNode)=%ld, sizeof(CuckooSlot)=%ld",
                sizeof(AllocatedBlockListNode), sizeof(CuckooSlot));
        printf("[CuckooFilter]bucket_size_: %ld, slot_num: %ld\n", bucket_size_, slot_num);
#endif
        pmem_buckets_ = new CuckooBucket *[bucket_size_];
        char *tmp = filter_addr_;
        for (size_t i = 0; i < bucket_size_; i++) {
            pmem_buckets_[i] = new CuckooBucket((CuckooSlot *) tmp, SLOT_PER_BUCKET, false);
            tmp += (sizeof(CuckooSlot) * SLOT_PER_BUCKET);
        }
    }

    CuckooFilter::~CuckooFilter() {
        for (size_t i = 0; i < bucket_size_; i++) {
            delete pmem_buckets_[i];
        }
        delete pmem_buckets_;
    }

// BKDRHash Impl
    uint64_t CuckooFilter::CuckooHash1(const char *str, size_t size) {
        uint64_t seed = 131;
        uint64_t hash = 0;

        for (size_t i = 0; i < size; i++) {
            hash = hash * seed + str[i];
        }

        return hash % bucket_size_;
    }

// APHash Impl
    uint64_t CuckooFilter::CuckooHash2(const char *str, size_t size) {
        uint64_t hash = 0;

        for (size_t i = 0; i < size; i++) {
            if ((i & 1) == 0) {
                hash ^= ((hash << 7) ^ str[i] ^ (hash >> 3));
            } else {
                hash ^= (~((hash << 11) ^ str[i] ^ (hash >> 5)));
            }
        }

        return hash % bucket_size_;
    }

    int CuckooFilter::CuckooCollide(uint64_t *tags) {
        CuckooBucket *bucket = pmem_buckets_[tags[0]];
        uint64_t victim_tags[2] = {tags[0], bucket->pmem_slots_[0].tag_};
        bucket->pmem_slots_[0].tag_ = tags[1];
        bucket->pmem_slots_[0].status_ = CuckooSlot::OCCUPIED;

        // 为受害者寻找新的 slot
        int indicator = 1;
        int which_slot = 0;
        int collide_num = 0;

        while (true) {
            bucket = pmem_buckets_[victim_tags[indicator]];
            for (size_t i = 0; i < bucket->slot_size_; i++) {
                CuckooSlot::STATUS slot_status = bucket->pmem_slots_[i].status_;
                if (slot_status == CuckooSlot::AVAILIBLE ||
                    slot_status == CuckooSlot::DELETED) {
                    bucket->pmem_slots_[i].status_ = CuckooSlot::OCCUPIED;
                    bucket->pmem_slots_[i].tag_ = victim_tags[indicator ^ 1];
                    return 0;
                }
            }

            // 判断处理冲突次数是否达到上限
            if ((++collide_num) > MAX_COLLIDE_NUM) {
                if (which_slot >= SLOT_PER_BUCKET) {
                    // 因为 pmem allocator 的限制，固定大小的一个block就是一个filter
                    // 所以目前rehash是无法实现的
                    // 今后考虑模拟实现一个伙伴算法，并提供一个虚拟的＂虚实地址转换＂
                    return 1;
                } else {
                    collide_num = 0;
                    which_slot++;
                }
            }
            // 强行占据一个slot，更新新的受害者
            uint64_t tmp_tag = victim_tags[indicator ^ 1];
            victim_tags[indicator ^ 1] = bucket->pmem_slots_[which_slot].tag_;
            bucket->pmem_slots_[which_slot].tag_ = tmp_tag;
            indicator ^= 1;
        }
    }

    void CuckooFilter::CuckooPutKey(const char *str, size_t size) {
        uint64_t tag1 = CuckooHash1(str, size);
        uint64_t tag2 = CuckooHash2(str, size);
        if (tag1 == tag2) {
            tag2 = (tag2 + 1) % bucket_size_;
        }
        uint64_t tags[2] = {tag1, tag2};

        cuckoo_mutex.lock();
        // 先查找 tag1，再查找 tag2
        bool tag_found = false;
        for (size_t tag_idx = 0; tag_idx < 2; tag_idx++) {
            CuckooBucket *tag_bucket = pmem_buckets_[tags[tag_idx]];
            for (size_t i = 0; i < tag_bucket->slot_size_; i++) {
                CuckooSlot::STATUS slot_status = tag_bucket->pmem_slots_[i].status_;
                if (slot_status == CuckooSlot::AVAILIBLE ||
                    slot_status == CuckooSlot::DELETED) {
                    tag_bucket->pmem_slots_[i].status_ = CuckooSlot::OCCUPIED;
                    tag_bucket->pmem_slots_[i].tag_ = (tag_idx == 0) ?
                                                      tag2 : tag1;
                    tag_found = true;
                    // TODO: slot 还需要保存这个key属于该 level 的哪一个 group
                    break;
                }
            }
            if (tag_found)
                break;
        }

        if (!tag_found) {
            // 都没有找到空位，碰撞处理
            int need_rehash = CuckooCollide(tags);
            assert(need_rehash == 0);
        }
        cuckoo_mutex.unlock();
        return;
    }

    void CuckooFilter::CuckooDeleteKey(const char *str, size_t size) {
        uint64_t tag1 = CuckooHash1(str, size);
        uint64_t tag2 = CuckooHash2(str, size);
        if (tag1 == tag2) {
            tag2 = (tag2 + 1) % bucket_size_;
        }
        // uint64_t tags[2] = {tag1, tag2};
        cuckoo_mutex.lock();
        // 两种情况
        // 1. tag1 确定 bucket
        CuckooBucket *bucket = pmem_buckets_[tag1];
        for (size_t i = 0; i < bucket->slot_size_; i++) {
            if (bucket->pmem_slots_[i].tag_ == tag2) {
                if (bucket->pmem_slots_[i].status_ ==
                    CuckooSlot::OCCUPIED) {
                    bucket->pmem_slots_[i].status_ = CuckooSlot::DELETED;
                    return;
                }
            }
        }

        // 2. tag2 确定 bucket
        bucket = pmem_buckets_[tag2];
        for (size_t i = 0; i < bucket->slot_size_; i++) {
            if (bucket->pmem_slots_[i].tag_ == tag1) {
                if (bucket->pmem_slots_[i].status_ ==
                    CuckooSlot::OCCUPIED) {
                    bucket->pmem_slots_[i].status_ = CuckooSlot::DELETED;
                    return;
                }
            }
        }
        cuckoo_mutex.unlock();
        return;
    }

    bool CuckooFilter::CuckooKeyExists(const char *str, size_t size) {
        uint64_t tag1 = CuckooHash1(str, size);
        uint64_t tag2 = CuckooHash2(str, size);
        if (tag1 == tag2) {
            tag2 = (tag2 + 1) % bucket_size_;
        }
        // uint64_t tags[2] = {tag1, tag2};
#ifdef PMEM_CUCKOO_DEBUG
        printf("[CuckooKeyExists] tag1=%ld, tag2=%ld\n", tag1, tag2);
#endif
        cuckoo_mutex.lock();
        // 两种情况
        // 1. tag1 确定 bucket
        CuckooBucket *bucket = pmem_buckets_[tag1];

        for (size_t i = 0; i < bucket->slot_size_; i++) {
            if (bucket->pmem_slots_[i].tag_ == tag2) {
                if (bucket->pmem_slots_[i].status_ ==
                    CuckooSlot::OCCUPIED) {
                    return true;
                }
            }
        }

        // 2. tag2 确定 bucket
        bucket = pmem_buckets_[tag2];
        for (size_t i = 0; i < bucket->slot_size_; i++) {
            if (bucket->pmem_slots_[i].tag_ == tag1) {
                if (bucket->pmem_slots_[i].status_ ==
                    CuckooSlot::OCCUPIED) {
                    return true;
                }
            }
        }
        cuckoo_mutex.unlock();
        return false;
    }
}