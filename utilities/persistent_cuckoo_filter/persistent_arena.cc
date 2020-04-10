#include "persistent_arena.h"

namespace rocksdb {
    PersistentArena::PersistentArena(std::string &path, uint64_t pmem_size) {
        // 将 pmem_size 按照 BLOCK_SIZE 进行对齐
        pmem_size = ((pmem_size + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;

        char *pmemaddr = nullptr;
        size_t mapped_size;
        int is_pmem;

        bool file_is_exists = access(path.c_str(), F_OK) ? false : true;
#ifdef PMEM_CUCKOO_DEBUG
        printf("[PersistentArena] pmem_size: %ld, file_is_exists: %d\n",pmem_size, file_is_exists);
#endif
        pmemaddr = (char *) pmem_map_file(path.c_str(), pmem_size,
                                          PMEM_FILE_CREATE, 0666, &mapped_size, &is_pmem);
        assert(pmemaddr != NULL);

        pmem_raw_ = pmemaddr;
        mapped_len_ = mapped_size;
        assert(mapped_len_ == pmem_size);
        is_pmem_ = is_pmem;
        block_num_ = pmem_size / BLOCK_SIZE;
        if (!file_is_exists) {
            // 创建
            uint64_t block_num = block_num_;
#ifdef PMEM_CUCKOO_DEBUG
            printf("[PersistentArena] block_num: %ld\n",block_num);
#endif
            for (size_t i = 0; i < block_num - 1; i++) {
                *((int64_t *) (pmem_raw_ + i * BLOCK_SIZE)) = (i + 1);
            }

            *((int64_t *) (pmem_raw_ + (block_num - 1) * BLOCK_SIZE)) =
                    NO_MORE_FREE_BLOCK;

            int64_t *filter_block_hdr_list_ptr =
                    (int64_t *) (pmem_raw_ + BLOCK_NEXT_FREE_BLOCK_SIZE);
            for (size_t i = 0; i < LEVEL_NUM; i++) {
                filter_block_hdr_list_ptr[i] = NO_MORE_NEXT_VALID_BLOCK;
            }
        }
        first_free_block_ = (int64_t *) pmem_raw_;
        first_filter_block_in_level_ =
                (int64_t *) (pmem_raw_ + BLOCK_NEXT_FREE_BLOCK_SIZE);
    }

    PersistentArena::~PersistentArena() {
        Sync();
        pmem_unmap(pmem_raw_, mapped_len_);
#ifdef PMEM_CUCKOO_DEBUG
        printf("[~PersistentArena]\n");
#endif
    }

    char *PersistentArena::AllocateBlock(uint64_t level, uint64_t &block_num) {
        assert(level < LEVEL_NUM);

        alloc_dispose_mutex_.lock();

        if (*first_free_block_ == NO_MORE_FREE_BLOCK)
            return nullptr;

        int64_t free_block_num = *first_free_block_;
#ifdef PMEM_CUCKOO_DEBUG
        printf("[PersistentArena::AllocateBlock] free_block_num=%ld\n", free_block_num);
#endif
        AllocatedBlockListNode *node =
                (AllocatedBlockListNode *) (pmem_raw_ + free_block_num * BLOCK_SIZE);
        node->level_ = level;
        *first_free_block_ = node->next_block_;
#ifdef PMEM_CUCKOO_DEBUG
        printf("[PersistentArena::AllocateBlock] current first_block_num=%ld\n", *first_free_block_);
#endif
        node->next_block_ = first_filter_block_in_level_[level];
#ifdef PMEM_CUCKOO_DEBUG
        printf("[PersistentArena::AllocateBlock] next_block=%ld\n", node->next_block_);
#endif
        if (node->next_block_ != NO_MORE_NEXT_VALID_BLOCK) {
            AllocatedBlockListNode *next_node =
                    (AllocatedBlockListNode *) (pmem_raw_ +
                                                first_filter_block_in_level_[level] * BLOCK_SIZE);
            next_node->pre_block_ = free_block_num;
        }
        node->pre_block_ = 0;
        first_filter_block_in_level_[level] = free_block_num;
        block_num = free_block_num;

        alloc_dispose_mutex_.unlock();

        return pmem_raw_ + free_block_num * BLOCK_SIZE;
    }

    void PersistentArena::DisposeBlock(uint64_t block_num) {
        alloc_dispose_mutex_.lock();

        AllocatedBlockListNode *node =
                (AllocatedBlockListNode *) (pmem_raw_ + block_num * BLOCK_SIZE);
        AllocatedBlockListNode *pre_node = node->pre_block_ == 0 ? nullptr :
                                           (AllocatedBlockListNode *) (pmem_raw_ + node->pre_block_ * BLOCK_SIZE);
        AllocatedBlockListNode *next_node = node->next_block_ == NO_MORE_NEXT_VALID_BLOCK ? nullptr :
                                            (AllocatedBlockListNode *) (pmem_raw_ + node->next_block_ * BLOCK_SIZE);

        if (pre_node) {
            pre_node->next_block_ = node->next_block_;
        } else {
            first_filter_block_in_level_[node->level_] = node->next_block_;
        }

        if (next_node) {
            next_node->pre_block_ = node->pre_block_;
        }

        node->next_block_ = *first_free_block_;
        *first_free_block_ = block_num;

        alloc_dispose_mutex_.unlock();
    }

    void PersistentArena::Sync() {
        if (is_pmem_) {
            pmem_persist(pmem_raw_, mapped_len_);
        } else {
            pmem_msync(pmem_raw_, mapped_len_);
        }
    }

    char *PersistentArena::GetBlockWithBlockNum(uint64_t block_num) {
        assert(block_num < block_num_ && block_num > 0);
        return pmem_raw_ + block_num * BLOCK_SIZE;
    }
}