/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_KVENGINE_H_
#define KVSTORE_KVENGINE_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "kvstore/Common.h"
#include "kvstore/KVIterator.h"
#include "common/base/ErrorOr.h"

namespace nebula {
namespace kvstore {

class WriteBatch {
public:
    virtual ~WriteBatch() = default;

    virtual ErrorCode
    put(folly::StringPiece key, folly::StringPiece value) = 0;

    virtual ErrorCode remove(folly::StringPiece key) = 0;

    // Remove all keys in the range [start, end)
    virtual ErrorCode
    removeRange(folly::StringPiece start, folly::StringPiece end) = 0;
};


class KVEngine {
public:
    explicit KVEngine(GraphSpaceID spaceId)
                : spaceId_(spaceId) {}

    virtual ~KVEngine() = default;

    virtual void stop() = 0;

    // Retrieve the root path for the data
    // If the store is persistent, a valid path will be returned
    // Otherwise, nullptr will be returned
    virtual const char* getDataRoot() const = 0;

    virtual std::unique_ptr<WriteBatch> startBatchWrite() = 0;

    virtual ErrorCode
    commitBatchWrite(std::unique_ptr<WriteBatch> batch,
                     bool disableWAL = true,
                     bool sync = false) = 0;

    // Read a single key
    virtual ErrorCode get(const std::string& key, std::string* value) = 0;

    // Read a list of keys, if key[i] does not exist, the i-th value in return value
    // would be Status::KeyNotFound
    virtual std::vector<Status> multiGet(const std::vector<std::string>& keys,
                                         std::vector<std::string>* values) = 0;

    // Get all results in range [start, end)
    virtual ErrorCode
    range(const std::string& start,
          const std::string& end,
          std::unique_ptr<KVIterator>* iter) = 0;

    // Get all results with 'prefix' str as prefix.
    virtual ErrorCode
    prefix(const std::string& prefix, std::unique_ptr<KVIterator>* iter) = 0;

    // Get all results with 'prefix' str as prefix starting form 'start'
    virtual ErrorCode
    rangeWithPrefix(const std::string& start,
                    const std::string& prefix,
                    std::unique_ptr<KVIterator>* iter) = 0;

    // Write a single record
    virtual ErrorCode put(std::string key, std::string value) = 0;

    // Write a batch of records
    virtual ErrorCode multiPut(std::vector<KV> keyValues) = 0;

    // Remove a single key
    virtual ErrorCode remove(const std::string& key) = 0;

    // Remove a batch of keys
    virtual ErrorCode multiRemove(std::vector<std::string> keys) = 0;

    // Remove range [start, end)
    virtual ErrorCode
    removeRange(const std::string& start, const std::string& end) = 0;

    // Add partId into current storage engine.
    virtual void addPart(PartitionID partId) = 0;

    // Remove partId from current storage engine.
    virtual void removePart(PartitionID partId) = 0;

    // Return all partIds current storage engine holds.
    virtual std::vector<PartitionID> allParts() = 0;

    // Return total parts num
    virtual int32_t totalPartsNum() = 0;

    // Ingest sst files
    virtual ErrorCode ingest(const std::vector<std::string>& files,
                                           bool verifyFileChecksum = false) = 0;

    // Set Config Option
    virtual ErrorCode
    setOption(const std::string& configKey, const std::string& configValue) = 0;

    // Set DB Config Option
    virtual ErrorCode
    setDBOption(const std::string& configKey,
                const std::string& configValue) = 0;

    virtual ErrorCode compact() = 0;

    virtual ErrorCode flush() = 0;

    virtual ErrorCode createCheckpoint(const std::string& name) = 0;

    // For meta
    virtual ErrorOr<ErrorCode, std::string>
    backupTable(const std::string& path,
                const std::string& tablePrefix,
                std::function<bool(const folly::StringPiece& key)> filter) = 0;

protected:
    GraphSpaceID spaceId_;
};

}   // namespace kvstore
}   // namespace nebula

#endif   // KVSTORE_KVENGINE_H_
