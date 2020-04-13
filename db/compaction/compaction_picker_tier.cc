#include <string>
#include <utility>
#include <algorithm>
#include <vector>

#include "db/compaction/compaction_picker_tier.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"

#define TIERED_DEBUG

namespace ROCKSDB_NAMESPACE {

namespace {
    // 定义 Tier 结构中的 Vertical Group
    struct VerticalGroup 
    {
        std::vector<FileMetaData*> group_files_;  // Group 中包含的文件元数据
        InternalKey smallest;                     // 整个 group 中的文件的最小 InternalKey
        InternalKey largest;                      // 整个 group 中的文件的最大 InternalKey
        uint64_t group_file_size_;                // 整个 group 中的文件总大小   
    };

    // 定义 VerticalGroup 结构的排序索引结构
    struct GroupSize
    {
        int group_index_;                         // 指示 VerticalGroup 在 TierCompactionBuilder::start_level_groups_ 的索引位置
        uint64_t group_file_size_;                // 整个 group 中的文件总大小 

        GroupSize(int group_index, uint64_t group_file_size) :
            group_index_(group_index), group_file_size_(group_file_size) {}
    };

    // 定义 compaction_picker_builder
    class TierCompactionBuilder {
    public:
        TierCompactionBuilder(const std::string& cf_name,
                              VersionStorageInfo* vstorage,
                              CompactionPicker* compaction_picker,
                              LogBuffer* log_buffer,
                              const MutableCFOptions& mutable_cf_options,
                              const ImmutableCFOptions& ioptions)
            : cf_name_(cf_name),
            vstorage_(vstorage),
            compaction_picker_(compaction_picker),
            log_buffer_(log_buffer),
            mutable_cf_options_(mutable_cf_options),
            ioptions_(ioptions) {}

        // Pick and return a compaction.
        Compaction* PickCompaction();

        bool PickFileToCompact();

        bool FileMetaDataPtrCmp(FileMetaData* ptr1, FileMetaData* ptr2);

        void GetStartLevelGroup();

        uint32_t GetPathId(
                    const ImmutableCFOptions& ioptions,
                    const MutableCFOptions& mutable_cf_options, int level);

        Compaction* GetCompaction();

        // vertical grouping 的 compaction 和 leveled 有相似之处
        // 所以 level compaction 中的部分成员这里也是需要的
        const std::string& cf_name_;                      // columnfamily name
        VersionStorageInfo* vstorage_;                    // 关于 version 的一些重要的统计信息和统计方法
        CompactionPicker* compaction_picker_;             // TierCompactionPicker Ptr
        LogBuffer* log_buffer_;
        int start_level_ = -1;                            // compaction 的引发层
        int output_level_ = -1;                           // compaction 的目的层
        int parent_index_ = -1;
        int base_index_ = -1;
        double start_level_score_ = 0;                     // 引发层的 compaction score
        bool is_manual_ = false;                           // 是否是一个手动调用的 compaction
        CompactionInputFiles start_level_inputs_;          // 引发层 level 以及需要 compaction 的 File 的元信息
        std::vector<CompactionInputFiles> compaction_inputs_;
        std::vector<CompactionInputFiles> compaction_for_tier_;
        CompactionInputFiles output_level_inputs_;         // 类似于 start_level_inputs
        std::vector<FileMetaData*> grandparents_;
        CompactionReason compaction_reason_ = CompactionReason::kUnknown;

        const MutableCFOptions& mutable_cf_options_;       // 一些配置信息
        const ImmutableCFOptions& ioptions_;

        std::vector<VerticalGroup> start_level_groups_;    // start_level_ 层的所有 group
        std::vector<GroupSize> start_level_groups_size_pri;// 对 start_level_groups_ 进行关于 size 的排序

        uint64_t input_level_group_filter_block_num_;
    };

    Compaction* TierCompactionBuilder::PickCompaction() 
    {
// #ifndef TIERED_DEBUG
//         for (int i = 0; i < compaction_picker_->NumberLevels(); i++) {
//             fprintf(stdout, "############################\n");
//             fprintf(stdout, "level: %d, score: %lf\n", 
//                     vstorage_->CompactionScoreLevel(i), vstorage_->CompactionScore(i));
//             fprintf(stdout, "############################\n");
//         }
// #endif
        // 遍历全部 level (除了最高层)
        // 根据 versionStorageInfo compaction score 裁决哪一层需要进行 compaction
        // vstorage_->CompactionScore 返回第 i 大的 compaction score
        // vstorage_->CompactionScoreLevel 返回具有第 i 大 compaction score 的 level
        for (int i = 0; i < compaction_picker_->NumberLevels() - 1; i++) {
            start_level_score_ = vstorage_->CompactionScore(i);
            start_level_ = vstorage_->CompactionScoreLevel(i);

            if (start_level_score_ >= 1) {
                // 只有当 score >= 1 时才有可能被选中
                // 确认 output_level_
                output_level_ =  start_level_ + 1;

                // 从 start_level_ 中获取需要进行 compact 的文件
                if (PickFileToCompact()) {
                    if (start_level_ == 0) {
                        // L0 score = `num L0 files` / `level0_file_num_compaction_trigger`
                        compaction_reason_ = CompactionReason::kLevelL0FilesNum;
                    } else {
                        // L1+ score = `Level files size` / `MaxBytesForLevel`
                        compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
                    }
                    break;
                }
            }
        }

        if (start_level_inputs_.empty()) {
            return nullptr;
        }

        compaction_picker_->GetGrandparents(vstorage_, start_level_inputs_,
                                        output_level_inputs_, &grandparents_);
        compaction_inputs_.push_back(start_level_inputs_);
        compaction_for_tier_.push_back(output_level_inputs_);

    // if (start_level_ >= 1) {
#ifndef TIERED_DEBUG
       fprintf(stdout, "[Tiered Compaction] ---------------current file meta-----------------\n");
       for (int i = 0; i < compaction_picker_->NumberLevels(); i++) {
           const std::vector<FileMetaData*>& level_files =  vstorage_->LevelFiles(i);
           fprintf(stdout, "Level %d : ", i);
           for (size_t j = 0; j < level_files.size(); j++) {
               fprintf(stdout, "[ %s --> %s  : %ld ], ", level_files[j]->smallest.user_key().ToString().c_str(),
                        level_files[j]->largest.user_key().ToString().c_str(), level_files[j]->pmem_block_num);
           }
           fprintf(stdout, "\n");
       }
       fprintf(stdout, "[Tiered Compaction] -------------------------------------------------\n\n");     
#endif 

#ifndef TIERED_DEBUG
        assert(compaction_inputs_.size() == 1);
        assert(compaction_for_tier_.size() == 1);
        fprintf(stdout, "[Tiered Compaction] ---------------Picked File-----------------\n");
        fprintf(stdout, "Pick files from level %d : ", start_level_inputs_.level);
        for (size_t i = 0; i < start_level_inputs_.files.size(); i++) {
            fprintf(stdout, "[ %s --> %s  : %ld ], ", 
                        start_level_inputs_.files[i]->smallest.user_key().ToString().c_str(),
                        start_level_inputs_.files[i]->largest.user_key().ToString().c_str(), 
                        start_level_inputs_.files[i]->pmem_block_num);
        }
        fprintf(stdout, "\n[Tiered Compaction] --------------------------------------------\n\n");

        fprintf(stdout, "[Tiered Compaction] ---------------Overlapped Files in OutputLevel-----------------\n");
        fprintf(stdout, "Output level %d : ", output_level_inputs_.level);
        for (size_t i = 0; i < output_level_inputs_.files.size(); i++) {
            fprintf(stdout, "[ %s --> %s  : %ld ], ", 
                        output_level_inputs_.files[i]->smallest.user_key().ToString().c_str(),
                        output_level_inputs_.files[i]->largest.user_key().ToString().c_str(), 
                        output_level_inputs_.files[i]->pmem_block_num);
        }
        fprintf(stdout, "\n[Tiered Compaction] --------------------------------------------\n\n");
#endif
    // }
        Compaction* c = GetCompaction();
        return c;
    }

    bool TierCompactionBuilder::PickFileToCompact()
    {
        // 对于 Tier Compaction 来说应该是不需要的
        // if (start_level_ == 0 &&
        //     !compaction_picker_->level0_compactions_in_progress()->empty()) {
        //     // TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
        //     return false;
        // }

        start_level_inputs_.clear();
        assert(start_level_ >= 0);
        if (start_level_ == 0) {
            InternalKey smallest, largest;
            bool is_init =true;
            const std::vector<FileMetaData*>& level0_files = vstorage_->LevelFiles(0);
            for (size_t i = 0; i < level0_files.size(); i++) {
                // 跳过正在从上层刷下来的文件
                if (level0_files[i]->being_compacted) {
                    continue;
                }
                start_level_inputs_.files.push_back(level0_files[i]);
                if (is_init) {
                    smallest = level0_files[i]->smallest;
                    largest = level0_files[i]->largest;
                    is_init = false;
                } else {
                    if (ioptions_.internal_comparator.Compare(
                            smallest, level0_files[i]->smallest) > 0) {
                        smallest = level0_files[i]->smallest;
                    }
                    if (ioptions_.internal_comparator.Compare(
                            largest, level0_files[i]->largest) < 0) {
                        largest = level0_files[i]->largest;
                    }
                }
            }
            start_level_inputs_.level = 0;
            if (start_level_inputs_.files.empty())
                return false;

            output_level_inputs_.level = 1;
            vstorage_->GetOverlappingInputs(1, &smallest, &largest,
                                        &output_level_inputs_.files);
            input_level_group_filter_block_num_ = 0;
            
            InternalKeyComparator comparator = ioptions_.internal_comparator;
            std::sort(output_level_inputs_.files.begin(),
                    output_level_inputs_.files.end(),
                    [comparator](FileMetaData* ptr1, FileMetaData* ptr2)
                    {
                        int cmp_res = comparator.Compare(ptr1->smallest, ptr2->smallest);
                        if (!cmp_res)
                            return comparator.Compare(ptr1->largest, ptr2->largest) > 0;
                        else
                            return cmp_res < 0;
                    });
            return true;
        }
        // 获取 start_level_ 层的 vertical group 信息
        // 注意信息中已经筛去了 being_compacted 的 File
        GetStartLevelGroup();

        if (start_level_groups_.empty())
            return false;

    if (start_level_ >= 1) {
#ifndef TIERED_DEBUG
        fprintf(stdout, "[Tiered Compaction] ---------------Group Info-----------------\n");
        fprintf(stdout, " level %d \n", start_level_);
        for (size_t i = 0; i < start_level_groups_.size(); i++) {
            fprintf(stdout, "Group %ld : ", i);
            VerticalGroup vgroup = start_level_groups_[i];
            for (size_t j = 0; j < vgroup.group_files_.size(); j++) {
                fprintf(stdout, "[ %s --> %s : %ld ], ", 
                    vgroup.group_files_[j]->smallest.user_key().ToString().c_str(),
                    vgroup.group_files_[j]->largest.user_key().ToString().c_str(),
                    vgroup.group_files_[j]->pmem_block_num);
            }
            fprintf(stdout, " smallest : %s , largest : %s \n",
                    vgroup.smallest.user_key().ToString().c_str(),
                    vgroup.largest.user_key().ToString().c_str());
        }
        fprintf(stdout, "[Tiered Compaction] --------------------------------------------\n\n");
#endif
    }

        // 对 start_level_groups_size_pri 关于 group size 从大到小进行排序
        std::sort(start_level_groups_size_pri.begin(), start_level_groups_size_pri.end(),
                  [](const GroupSize& first, const GroupSize& second) -> bool {
                      return first.group_file_size_ > second.group_file_size_;
                  });

        // 选择文件大小最大的 group
        start_level_inputs_.level = start_level_;
        int max_size_group_index = start_level_groups_size_pri[0].group_index_;
        const std::vector<FileMetaData*>& max_size_group_files = 
            start_level_groups_[max_size_group_index].group_files_;
        for (size_t i = 0; i < max_size_group_files.size(); i++) {
            start_level_inputs_.files.push_back(max_size_group_files[i]);
            // 读流程
            input_level_group_filter_block_num_ = max_size_group_files[i]->pmem_block_num;
        }

        // 获得 output_level_ 上的重叠文件
        InternalKey smallest, largest;
        smallest = start_level_groups_[max_size_group_index].smallest;
        largest = start_level_groups_[max_size_group_index].largest;
        output_level_inputs_.level = output_level_;
        vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs_.files);
        
        InternalKeyComparator comparator = ioptions_.internal_comparator;
        std::sort(output_level_inputs_.files.begin(),
                  output_level_inputs_.files.end(),
                  [comparator](FileMetaData* ptr1, FileMetaData* ptr2)
                {
                    int cmp_res = comparator.Compare(ptr1->smallest, ptr2->smallest);
                    if (!cmp_res)
                        return comparator.Compare(ptr1->largest, ptr2->largest) > 0;
                    else
                        return cmp_res < 0;
                });
        return true;
    }

    // bool TierCompactionBuilder::FileMetaDataPtrCmp(FileMetaData* ptr1, FileMetaData* ptr2)
    // {
    //     int cmp_res = ioptions_.internal_comparator.Compare(ptr1->smallest, ptr2->smallest);
    //     if (!cmp_res)
    //         return ioptions_.internal_comparator.Compare(ptr1->largest, ptr2->largest) > 0;
    //     else
    //         return cmp_res < 0;
    // }

    // 获取 start_level_ 层的 vertical group 信息
    void TierCompactionBuilder::GetStartLevelGroup()
    {
        start_level_groups_.clear();
        start_level_groups_size_pri.clear();
        // 获得 level 层的 File 元数据
        std::vector<FileMetaData*> level_files(vstorage_->LevelFiles(start_level_));
  
        InternalKeyComparator comparator = ioptions_.internal_comparator;
        // 对元数据按照 smallest InternalKey 从小到大进行排序
        std::sort(level_files.begin(), level_files.end(), 
                [comparator](FileMetaData* ptr1, FileMetaData* ptr2)
                {
                    int cmp_res = comparator.Compare(ptr1->smallest, ptr2->smallest);
                    if (!cmp_res)
                        return comparator.Compare(ptr1->largest, ptr2->largest) > 0;
                    else
                        return cmp_res < 0;
                });

        // 确保 start_level_ 存在有文件
        if (!level_files.empty()) {
            VerticalGroup vgroup;
            vgroup.group_files_.clear();
            assert(vgroup.group_files_.empty());
            vgroup.group_files_.push_back(level_files[0]);
            vgroup.smallest = level_files[0]->smallest;
            vgroup.largest = level_files[0]->largest;
            vgroup.group_file_size_ = level_files[0]->compensated_file_size;
            for (size_t i = 1; i < level_files.size(); i++) {
                // 跳过正在从上层刷下来的文件
                if (level_files[i]->being_compacted) {
                    continue;
                }
                // 判断此文件是否落在当前 group 范围内
                // 注意：需要判断的是 UserKey
                if (ioptions_.internal_comparator.CompareWithUserKey(
                            vgroup.smallest, level_files[i]->smallest) <= 0 &&
                    ioptions_.internal_comparator.CompareWithUserKey(
                            vgroup.largest, level_files[i]->smallest) >= 0) {
                    vgroup.group_files_.push_back(level_files[i]);
                    vgroup.group_file_size_ += level_files[i]->compensated_file_size;
                    if (ioptions_.internal_comparator.Compare(
                            vgroup.largest, level_files[i]->largest) < 0) {
                        // 扩展 largest 边界
                        vgroup.largest = level_files[i]->largest;
                    }
                } else {
                    // 不在当前 group 范围内, 保存旧信息
                    start_level_groups_.push_back(vgroup);
                    start_level_groups_size_pri.emplace_back(start_level_groups_.size() - 1,
                                                             vgroup.group_file_size_);
                    // 创建新的 group 信息
                    vgroup.group_files_.clear();
                    vgroup.group_files_.push_back(level_files[i]);
                    vgroup.smallest = level_files[i]->smallest;
                    vgroup.largest = level_files[i]->largest;
                    vgroup.group_file_size_ = level_files[i]->compensated_file_size;
                }
            }
            start_level_groups_.push_back(vgroup);
            start_level_groups_size_pri.emplace_back(start_level_groups_.size() - 1,
                                                     vgroup.group_file_size_);
        }
    }

    uint32_t TierCompactionBuilder::GetPathId(
        const ImmutableCFOptions& ioptions,
        const MutableCFOptions& mutable_cf_options, int level) {
    uint32_t p = 0;
    assert(!ioptions.cf_paths.empty());

    // size remaining in the most recent path
    uint64_t current_path_size = ioptions.cf_paths[0].target_size;

    uint64_t level_size;
    int cur_level = 0;

    // max_bytes_for_level_base denotes L1 size.
    // We estimate L0 size to be the same as L1.
    level_size = mutable_cf_options.max_bytes_for_level_base;

    // Last path is the fallback
    while (p < ioptions.cf_paths.size() - 1) {
        if (level_size <= current_path_size) {
        if (cur_level == level) {
            // Does desired level fit in this path?
            return p;
        } else {
            current_path_size -= level_size;
            if (cur_level > 0) {
            if (ioptions.level_compaction_dynamic_level_bytes) {
                // Currently, level_compaction_dynamic_level_bytes is ignored when
                // multiple db paths are specified. https://github.com/facebook/
                // rocksdb/blob/master/db/column_family.cc.
                // Still, adding this check to avoid accidentally using
                // max_bytes_for_level_multiplier_additional
                level_size = static_cast<uint64_t>(
                    level_size * mutable_cf_options.max_bytes_for_level_multiplier);
            } else {
                level_size = static_cast<uint64_t>(
                    level_size * mutable_cf_options.max_bytes_for_level_multiplier *
                    mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
            }
            }
            cur_level++;
            continue;
        }
        }
        p++;
        current_path_size = ioptions.cf_paths[p].target_size;
    }
    return p;
    }
    
    Compaction* TierCompactionBuilder::GetCompaction() 
    {
        auto c = new Compaction(
            vstorage_, ioptions_, mutable_cf_options_, std::move(compaction_inputs_),
            output_level_,
            MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                                ioptions_.compaction_style, vstorage_->base_level(),
                                ioptions_.level_compaction_dynamic_level_bytes),
            mutable_cf_options_.max_compaction_bytes,
            GetPathId(ioptions_, mutable_cf_options_, output_level_),
            GetCompressionType(ioptions_, vstorage_, mutable_cf_options_,
                                output_level_, vstorage_->base_level()),
            GetCompressionOptions(ioptions_, vstorage_, output_level_),
            // GenSubcompactionBoundaries 中对 Tier 的情况进行了特殊处理
            // Tier 的 subcompaction 数和 max_subcompactions 无关
            // 所以这里传0也可以
            /* max_subcompactions */ 0, std::move(grandparents_), is_manual_,
            start_level_score_, false /* deletion_compaction */, compaction_reason_,
            std::move(compaction_for_tier_),
            // 传入 input 层的 group filter 的 block_num
            input_level_group_filter_block_num_);

        // If it's level 0 compaction, make sure we don't execute any other level 0
        // compactions in parallel
        compaction_picker_->RegisterCompaction(c);

        // Creating a compaction influences the compaction score because the score
        // takes running compactions into account (by skipping files that are already
        // being compacted). Since we just changed compaction score, we recalculate it
        // here
        vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
        return c;
    }

}  // anoymous namespace for TierCompactionBuilder

    Compaction* TierCompactionPicker::PickCompaction(
            const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
            VersionStorageInfo* vstorage, LogBuffer* log_buffer,
            SequenceNumber earliest_memtable_seqno) {
        TierCompactionBuilder builder(cf_name, vstorage, this, log_buffer,
                                        mutable_cf_options, ioptions_);
        return builder.PickCompaction();
    }

    bool TierCompactionPicker::NeedsCompaction(
            const VersionStorageInfo* vstorage) const {
        // if (!vstorage->ExpiredTtlFiles().empty()) {
        //     return true;
        // }
        // if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
        //     return true;
        // }
        // if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
        //     return true;
        // }
        // if (!vstorage->FilesMarkedForCompaction().empty()) {
        //     return true;
        // }
        for (int i = 0; i <= vstorage->MaxInputLevel(); i++) {
            if (vstorage->CompactionScore(i) >= 1) {
                return true;
            }
        }
        return false;
    }

}  // namespace rocksdb