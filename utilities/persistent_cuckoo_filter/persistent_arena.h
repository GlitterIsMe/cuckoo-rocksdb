#ifndef PERSISTENT_CUCKOO_FILTER_ARENA
#define PERSISTENT_CUCKOO_FILTER_ARENA

#include <string>
#include <libpmem.h>
#include <unistd.h>
#include <stdio.h>
#include <cassert>
#include <mutex>
#include "pmem_format.h"

#define LEVEL_NUM 10

namespace rocksdb {
    class PersistentArena {
    public:
        PersistentArena(std::string &path, uint64_t pmem_size = PMEM_SIZE);

        PersistentArena(const PersistentArena &) = delete;

        PersistentArena &operator=(const PersistentArena &) = delete;

        ~PersistentArena();

        size_t GetMappedSize() { return mapped_len_; }

        char *AllocateBlock(uint64_t level, uint64_t &block_num);

        void DisposeBlock(uint64_t block_num);

        void Sync();

        char *GetBlockWithBlockNum(uint64_t block_num);

    private:
        std::mutex alloc_dispose_mutex_;
        char *pmem_raw_;             // PM mmap后在内存中的首地址
        int64_t *first_free_block_;  // 首个空闲的block编号
        // RocksDB 默认的 level 层数为7,这里设置为10,以防万一
        int64_t *first_filter_block_in_level_;
        size_t mapped_len_;
        int is_pmem_;
        uint64_t block_num_;
    };
}

#endif