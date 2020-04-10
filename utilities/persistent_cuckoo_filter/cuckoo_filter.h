#pragma once

#include "persistent_arena.h"

#define SLOT_PER_BUCKET 4
#define MAX_COLLIDE_NUM 512

namespace rocksdb {
    struct CuckooSlot {
        enum STATUS {
            AVAILIBLE,
            OCCUPIED,
            DELETED,
        };
        uint64_t tag_;     // 保存 Cuckoo Hash 的第二个 Hash 值
        STATUS status_;
    };

    class CuckooBucket {
        friend class CuckooFilter;

    public:
        CuckooBucket(CuckooSlot *pmem_slot, uint64_t slot_size = SLOT_PER_BUCKET, bool is_create = true);

    private:
        CuckooSlot *pmem_slots_;
        uint64_t slot_size_;
    };

    class CuckooFilter {
    public:
        // 用于创建一个新的 CuckooFilter
        CuckooFilter(PersistentArena *pmem_arena, uint64_t level, uint64_t &block_num);

        // 用于恢复一个 CuckooFilter
        CuckooFilter(PersistentArena *pmem_arena, uint64_t block_num);

        ~CuckooFilter();

        uint64_t CuckooHash1(const char *str, size_t size);

        uint64_t CuckooHash2(const char *str, size_t size);

        void CuckooPutKey(const char *str, size_t size);

        void CuckooDeleteKey(const char *str, size_t size);

        bool CuckooKeyExists(const char *str, size_t size);

    private:
        std::mutex cuckoo_mutex;

        int CuckooCollide(uint64_t *tags);

        PersistentArena *pmem_arena_;
        char *filter_addr_;
        uint64_t bucket_size_;
        CuckooBucket **pmem_buckets_;
    };
}