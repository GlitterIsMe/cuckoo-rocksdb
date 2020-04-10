#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace std;
using namespace rocksdb;

const std::string PATH = "./rocksdb_data";

class IntegerComparator : public Comparator 
{
public:
    const char* Name() const override {
        return "IntegerComparator";
    }
    
    int Compare(const Slice& a, const Slice& b) const override {
        uint64_t x = stol(a.ToString());
        uint64_t y = stol(b.ToString());
        return (x == y) ? 0 : ((x < y) ? -1 : 1);
    }

    void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
        
    }
    
    void FindShortSuccessor(std::string* key) const override {

    }
};

IntegerComparator integerComparator;

Status PutNumericKeyValue(DB* db, uint64_t n)
{
    char str[20];
    snprintf(str, 20, "%ld", n);
    Slice key(str);
    Slice value(str);
    return db->Put(WriteOptions(), key, value);
}

Status PutNumericKeyValueAddOne(DB* db, uint64_t n)
{
    char str[20];
    snprintf(str, 20, "%ld", n);
    char str2[20];
    snprintf(str2, 20, "%ld", n + 1);
    Slice key(str);
    Slice value(str2);
    return db->Put(WriteOptions(), key, value);
}

int main()
{
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 1<<10;
    options.persistent_file_path_ = "./pmem";
    options.is_tiered = true;
    options.compaction_style = kCompactionStyleTier;
    options.comparator = &integerComparator;
    // options.max_bytes_for_level_base = 1<<13;

    Status status = DB::Open(options, PATH, &db);
    assert(status.ok());
    
    // 1 --> 15000
    for (uint64_t i = 4000; i <= 19000; i++) {
        if (i % 1000 == 0) {
            printf("finish %ld put ops\n", i);
        }
        status = PutNumericKeyValue(db, i);
        assert(status.ok());
    }

    printf("\n\n#######################################\n");
    printf("Overlap test!!!!!!!!!!!!!!!!!!!!\n");
    printf("#######################################\n\n");
    // 4000 --> 19000 
    for (uint64_t i = 1; i <= 15000; i++) {
        if (i % 1000 == 0) {
            printf("finish %ld put ops\n", i);
        }
        status = PutNumericKeyValueAddOne(db, i);
        assert(status.ok());
    }

    for (uint64_t i = 1; i <= 8000; i++) {
        if (i % 1000 == 0) {
            printf("finish %ld put ops\n", i);
        }
        status = PutNumericKeyValueAddOne(db, i);
        assert(status.ok());
    }

    ReadOptions ro = ReadOptions();
    ro.ignore_range_deletions = true;
    printf("\n\n#######################################\n");
    printf("Get test!!!!!!!!!!!!!!!!!!!!\n");
    printf("#######################################\n\n");
    string get_value;
    status = db->Get(ro, "4567", &get_value);
    if(status.ok()){
        printf("get %s\n", get_value.c_str());
    }else{
        printf("get failed\n"); 
    }

    delete db;
}
