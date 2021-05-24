/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <folly/lang/Bits.h>
#include "kvstore/RocksEngine.h"

namespace nebula {
namespace kvstore {

const int32_t kDefaultVIdLen = 8;

TEST(RocksEngineTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_SimpleTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->put("key", "val"));
    std::string val;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get("key", &val));
    EXPECT_EQ("val", val);
}


TEST(RocksEngineTest, RangeTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_RangeTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    std::vector<KV> data;
    for (int32_t i = 10; i < 20;  i++) {
        data.emplace_back(std::string(reinterpret_cast<const char*>(&i), sizeof(int32_t)),
                          folly::stringPrintf("val_%d", i));
    }
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));

    auto checkRange = [&](int32_t start,
                          int32_t end,
                          int32_t expectedFrom,
                          int32_t expectedTotal) {
        VLOG(1) << "start " << start
                << ", end " << end
                << ", expectedFrom " << expectedFrom
                << ", expectedTotal " << expectedTotal;
        std::string s(reinterpret_cast<const char*>(&start), sizeof(int32_t));
        std::string e(reinterpret_cast<const char*>(&end), sizeof(int32_t));
        std::unique_ptr<KVIterator> iter;
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->range(s, e, &iter));
        int num = 0;
        while (iter->valid()) {
            num++;
            auto key = *reinterpret_cast<const int32_t*>(iter->key().data());
            auto val = iter->val();
            EXPECT_EQ(expectedFrom, key);
            EXPECT_EQ(folly::stringPrintf("val_%d", expectedFrom), val);
            expectedFrom++;
            iter->next();
        }
        EXPECT_EQ(expectedTotal, num);
    };

    checkRange(10, 20, 10, 10);
    checkRange(1, 50, 10, 10);
    checkRange(15, 18, 15, 3);
    checkRange(15, 23, 15, 5);
    checkRange(1, 15, 10, 5);
}


TEST(RocksEngineTest, PrefixTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_PrefixTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    LOG(INFO) << "Write data in batch and scan them...";
    std::vector<KV> data;
    for (int32_t i = 0; i < 10;  i++) {
        data.emplace_back(folly::stringPrintf("a_%d", i),
                          folly::stringPrintf("val_%d", i));
    }
    for (int32_t i = 10; i < 15;  i++) {
        data.emplace_back(folly::stringPrintf("b_%d", i),
                          folly::stringPrintf("val_%d", i));
    }
    for (int32_t i = 20; i < 40;  i++) {
        data.emplace_back(folly::stringPrintf("c_%d", i),
                          folly::stringPrintf("val_%d", i));
    }
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));

    auto checkPrefix = [&](const std::string& prefix,
                           int32_t expectedFrom,
                           int32_t expectedTotal) {
        VLOG(1) << "prefix " << prefix
                << ", expectedFrom " << expectedFrom
                << ", expectedTotal " << expectedTotal;

        std::unique_ptr<KVIterator> iter;
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->prefix(prefix, &iter));
        int num = 0;
        while (iter->valid()) {
            num++;
            auto key = iter->key();
            auto val = iter->val();
            EXPECT_EQ(folly::stringPrintf("%s_%d", prefix.c_str(), expectedFrom), key);
            EXPECT_EQ(folly::stringPrintf("val_%d", expectedFrom), val);
            expectedFrom++;
            iter->next();
        }
        EXPECT_EQ(expectedTotal, num);
    };
    checkPrefix("a", 0, 10);
    checkPrefix("b", 10, 5);
    checkPrefix("c", 20, 20);
}


TEST(RocksEngineTest, RemoveTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_RemoveTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->put("key", "val"));
    std::string val;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get("key", &val));
    EXPECT_EQ("val", val);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->remove("key"));
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, engine->get("key", &val));
}


TEST(RocksEngineTest, RemoveRangeTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_RemoveRangeTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    for (int32_t i = 0; i < 100; i++) {
        std::string key(reinterpret_cast<const char*>(&i), sizeof(int32_t));
        std::string value(folly::stringPrintf("%d_val", i));
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->put(key, value));
        std::string val;
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get(key, &val));
        EXPECT_EQ(value, val);
    }
    {
        int32_t s = 0, e = 50;
        EXPECT_EQ(
            nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->removeRange(
                std::string(reinterpret_cast<const char*>(&s), sizeof(int32_t)),
                std::string(reinterpret_cast<const char*>(&e), sizeof(int32_t))));
    }
    {
        int32_t s = 0, e = 100;
        std::unique_ptr<KVIterator> iter;
        std::string start(reinterpret_cast<const char*>(&s), sizeof(int32_t));
        std::string end(reinterpret_cast<const char*>(&e), sizeof(int32_t));
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->range(start, end, &iter));
        int num = 0;
        int expectedFrom = 50;
        while (iter->valid()) {
            num++;
            auto key = *reinterpret_cast<const int32_t*>(iter->key().data());
            auto val = iter->val();
            EXPECT_EQ(expectedFrom, key);
            EXPECT_EQ(folly::stringPrintf("%d_val", expectedFrom), val);
            expectedFrom++;
            iter->next();
        }
        EXPECT_EQ(50, num);
    }
}


TEST(RocksEngineTest, OptionTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_OptionTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->setOption("disable_auto_compactions", "true"));
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM,
              engine->setOption("disable_auto_compactions_", "true"));
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM,
              engine->setOption("disable_auto_compactions", "bad_value"));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->setDBOption("max_background_compactions", "2"));
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM,
              engine->setDBOption("max_background_compactions_", "2"));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->setDBOption("max_background_compactions", "2_"));
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM,
              engine->setDBOption("max_background_compactions", "bad_value"));
}


TEST(RocksEngineTest, CompactTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_CompactTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    std::vector<KV> data;
    for (int32_t i = 2; i < 8;  i++) {
        data.emplace_back(folly::stringPrintf("key_%d", i),
                          folly::stringPrintf("value_%d", i));
    }
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->compact());
}

TEST(RocksEngineTest, IngestTest) {
    rocksdb::Options options;
    rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options);
    fs::TempDir rootPath("/tmp/rocksdb_engine_IngestTest.XXXXXX");
    auto file = folly::stringPrintf("%s/%s", rootPath.path(), "data.sst");
    auto stauts = writer.Open(file);
    ASSERT_TRUE(stauts.ok());

    stauts = writer.Put("key", "value");
    ASSERT_TRUE(stauts.ok());
    stauts = writer.Put("key_empty", "");
    ASSERT_TRUE(stauts.ok());
    writer.Finish();

    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    std::vector<std::string> files = {file};
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->ingest(files));

    std::string result;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get("key", &result));
    EXPECT_EQ("value", result);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get("key_empty", &result));
    EXPECT_EQ("", result);
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, engine->get("key_not_exist", &result));
}

TEST(RocksEngineTest, BackupRestoreTable) {
    rocksdb::Options options;
    rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options);
    fs::TempDir rootPath("/tmp/rocksdb_engine_backuptable.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());

    std::vector<KV> data;
    for (int32_t i = 0; i < 10; i++) {
        data.emplace_back(folly::stringPrintf("part_%d", i),
                          folly::stringPrintf("val_%d", i));
        data.emplace_back(folly::stringPrintf("tags_%d", i),
                          folly::stringPrintf("val_%d", i));
    }
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));

    std::vector<std::string> sst_files;
    std::string partPrefix = "part_";
    std::string tagsPrefix = "tags_";
    auto parts = engine->backupTable("backup_test", partPrefix, nullptr);
    EXPECT_TRUE(ok(parts));
    sst_files.emplace_back(value(parts));
    auto tags = engine->backupTable("backup_test", tagsPrefix, [](const folly::StringPiece& key) {
        auto i = key.subpiece(5, key.size());
        if (folly::to<int>(i) % 2 == 0) {
            return true;
        }
        return false;
    });
    EXPECT_TRUE(ok(tags));
    sst_files.emplace_back(value(tags));

    fs::TempDir restoreRootPath("/tmp/rocksdb_engine_restoretable.XXXXXX");
    auto restore_engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, restoreRootPath.path());
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, restore_engine->ingest(sst_files));

    std::unique_ptr<KVIterator> iter;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, restore_engine->prefix(partPrefix, &iter));
    int index = 0;
    while (iter->valid()) {
        auto key = iter->key();
        auto val = iter->val();
        EXPECT_EQ(folly::stringPrintf("%s%d", partPrefix.c_str(), index), key);
        EXPECT_EQ(folly::stringPrintf("val_%d", index), val);
        iter->next();
        index++;
    }
    EXPECT_EQ(index, 10);

    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, restore_engine->prefix(tagsPrefix, &iter));
    index = 1;
    int num = 0;
    while (iter->valid()) {
        auto key = iter->key();
        auto val = iter->val();
        EXPECT_EQ(folly::stringPrintf("%s%d", tagsPrefix.c_str(), index), key);
        EXPECT_EQ(folly::stringPrintf("val_%d", index), val);
        iter->next();
        index += 2;
        num++;
    }
    EXPECT_EQ(num, 5);
}

TEST(RocksEngineTest, TailingTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_TailingTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    for (int i = 0; i < 10; i++) {
        auto key = folly::stringPrintf("key-1-%d", i);
        auto val = folly::stringPrintf("old-val%d", i);
        engine->put(key, val);
    }

    std::thread write([db = engine.get()] {
        // sleep a while to wait db iterator initialized
        sleep(1);
        for (int i = 0; i < 10; i++) {
            auto key = folly::stringPrintf("key-2-%d", i);
            auto val = folly::stringPrintf("new-val%d", i);
            db->put(key, val);
        }
    });

    {
        std::string prefix = "key";
        rocksdb::ReadOptions options;
        options.prefix_same_as_start = true;
        options.tailing = true;
        rocksdb::Iterator* iter = engine->db_->NewIterator(options);
        if (iter) {
            iter->Seek(rocksdb::Slice(prefix));
        }

        // wait write thread has finished
        sleep(3);

        std::unique_ptr<KVIterator> storageIter;
        storageIter.reset(new RocksPrefixIter(iter, prefix));

        int32_t count = 0;
        while (storageIter->valid()) {
            VLOG(1) << storageIter->key() << " " << storageIter->val();
            storageIter->next();
            count++;
        }
        EXPECT_EQ(count, 20);
    }
    write.join();
}

TEST(RocksEngineTest, SnapshotTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_SnapshotTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    for (int i = 0; i < 10; i++) {
        auto key = folly::stringPrintf("key-%d", i);
        auto val = folly::stringPrintf("old-%d", i);
        engine->put(key, val);
    }

    const auto* snapshot = engine->db_->GetSnapshot();
    std::string prefix = "key";

    for (int i = 0; i < 5; i++) {
        auto key = folly::stringPrintf("key-%d", i);
        auto val = folly::stringPrintf("new-%d", i);
        engine->put(key, val);
    }
    {
        // read from snapshot
        rocksdb::ReadOptions options;
        options.snapshot = snapshot;
        auto* iter = engine->db_->NewIterator(options);
        if (iter) {
            iter->Seek(rocksdb::Slice(prefix));
        }
        std::unique_ptr<KVIterator> storageIter;
        storageIter.reset(new RocksPrefixIter(iter, prefix));

        int32_t count = 0;
        while (storageIter->valid()) {
            auto key = storageIter->key();
            auto val = storageIter->val();
            EXPECT_EQ(folly::stringPrintf("key-%d", count), key);
            EXPECT_EQ(folly::stringPrintf("old-%d", count), val);
            storageIter->next();
            count++;
        }
        EXPECT_EQ(count, 10);
    }
    {
        // read from latest
        rocksdb::ReadOptions options;
        auto* iter = engine->db_->NewIterator(options);
        if (iter) {
            iter->Seek(rocksdb::Slice(prefix));
        }
        std::unique_ptr<KVIterator> storageIter;
        storageIter.reset(new RocksPrefixIter(iter, prefix));

        int32_t count = 0;
        while (storageIter->valid()) {
            auto key = storageIter->key();
            auto val = storageIter->val();
            EXPECT_EQ(folly::stringPrintf("key-%d", count), key);
            if (count < 5) {
                EXPECT_EQ(folly::stringPrintf("new-%d", count), val);
            } else {
                EXPECT_EQ(folly::stringPrintf("old-%d", count), val);
            }
            storageIter->next();
            count++;
        }
        EXPECT_EQ(count, 10);
    }
    {
        // read new data after snapshot
        rocksdb::ReadOptions options;
        options.prefix_same_as_start = true;
        // do not include the seq num of snapshot
        options.iter_start_seqnum = snapshot->GetSequenceNumber() + 1;
        auto* iter = engine->db_->NewIterator(options);
        if (iter) {
            iter->Seek(rocksdb::Slice(prefix));
        }

        int32_t count = 0;
        while (iter->Valid()) {
            // when iter_start_seqnum is specified, internal key is returned
            rocksdb::FullKey fKey;
            rocksdb::ParseFullKey(iter->key(), &fKey);
            EXPECT_EQ(folly::stringPrintf("key-%d", count), fKey.user_key.ToString());
            EXPECT_EQ(folly::stringPrintf("old-%d", count), iter->value().ToString());
            iter->Next();
            count++;
        }
        EXPECT_EQ(count, 5);
    }

    engine->db_->ReleaseSnapshot(snapshot);
}

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

