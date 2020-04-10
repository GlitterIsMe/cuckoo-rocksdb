#pragma once

#include <stdint.h>
/*
*   +----------------+
*   |    Block 0     |
*   +----------------+
*           | 
*           V
*   +----------------------------+ 
*   |        首个空闲block号     |      
*   +----------------------------+
*   | Group CuckooFilter listHdr |
*   | L0 第一个Filter所在Block号 |
*   | L1 第一个Filter所在Block号 |
*   |             .....          |
*   +----------------------------+
*
*
*   +----------------+
*   |  Block i(i!=0) |
*   +----------------+
*           | 
*           V
*   +----------------------------+
*   |    下一个空闲block号       |   非空闲block保存同level的下一个block号
*   +----------------------------+
*   |  该层的前一个GroupFilter   |
*   |     所在的Block号          |
*   +----------------------------+
*   |    属于哪一层（非空闲使用）|
*   +----------------------------+
*   |   smallest_key size        |
*   +----------------------------+
*   |   smallest_key             |
*   +----------------------------+
*   |   largest_key size         |
*   +----------------------------+
*   |   largest_key              |
*   +----------------------------+
*   |   CuckooFilter             |
*   +----------------------------+
*/

#define BLOCK_NEXT_FREE_BLOCK_SIZE (sizeof(int64_t))
#define NO_MORE_FREE_BLOCK -1
#define NO_MORE_NEXT_VALID_BLOCK -2
#define BLOCK_SIZE (1024*1024)            // 暂定一个 Cuckoo Filter 占用 1MB
#define PMEM_SIZE (1024*1024*1024)         // 本地测试开辟的 PM 的大小 128MB  

namespace rocksdb {
    struct AllocatedBlockListNode {
        int64_t next_block_;
        int64_t pre_block_;
        int level_;
    };
}