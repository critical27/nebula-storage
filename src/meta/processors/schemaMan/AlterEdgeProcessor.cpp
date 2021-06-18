/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/AlterEdgeProcessor.h"
#include "meta/processors/schemaMan/SchemaUtil.h"

namespace nebula {
namespace meta {

void AlterEdgeProcessor::process(const cpp2::AlterEdgeReq& req) {
    GraphSpaceID spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);
    auto edgeName = req.get_edge_name();

    folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    auto ret = getEdgeType(spaceId, edgeName);
    if (!nebula::ok(ret)) {
        auto retCode = nebula::error(ret);
        LOG(ERROR) << "Failed to get edge " << edgeName << " error "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto edgeType = nebula::value(ret);

    // Check the edge belongs to the space
    auto edgePrefix = MetaServiceUtils::schemaEdgePrefix(spaceId, edgeType);
    auto retPre = doPrefix(edgePrefix);
    if (!nebula::ok(retPre)) {
        auto retCode = nebula::error(retPre);
        LOG(ERROR) << "Edge Prefix failed, edgename: " << edgeName
                   << ", spaceId " << spaceId << " error "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto iter = nebula::value(retPre).get();
    if (!iter->valid()) {
        LOG(ERROR) << "Edge could not be found, spaceId " << spaceId
                   << ", edgename: " << edgeName;
        handleErrorCode(ErrorCode::E_STORAGE_KVSTORE_KEY_NOT_FOUND);
        onFinished();
        return;
    }

    // Get lasted version of edge
    auto version = MetaServiceUtils::parseEdgeVersion(iter->key()) + 1;
    auto schema = MetaServiceUtils::parseSchema(iter->val());
    auto columns = schema.get_columns();
    auto prop = schema.get_schema_prop();

    // Update schema column
    auto& edgeItems = req.get_edge_items();

    auto iRet = getIndexes(spaceId, edgeType);
    if (!nebula::ok(iRet)) {
        handleErrorCode(nebula::error(iRet));
        onFinished();
        return;
    }

    auto indexes = std::move(nebula::value(iRet));
    auto existIndex = !indexes.empty();
    if (existIndex) {
        auto iStatus = indexCheck(indexes, edgeItems);
        if (iStatus != ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Alter edge error, index conflict : "
                       << apache::thrift::util::enumNameSafe(iStatus);
            handleErrorCode(iStatus);
            onFinished();
            return;
        }
    }

    auto& alterSchemaProp = req.get_schema_prop();
    if (existIndex) {
        int64_t duration = 0;
        if (alterSchemaProp.get_ttl_duration()) {
            duration = *alterSchemaProp.get_ttl_duration();
        }
        std::string col;
        if (alterSchemaProp.get_ttl_col()) {
            col = *alterSchemaProp.get_ttl_col();
        }
        if (!col.empty() && duration > 0) {
            LOG(ERROR) << "Alter edge error, index and ttl conflict";
            handleErrorCode(ErrorCode::E_META_SCHEMA_CHANGE_FORBIDDEN_WHEN_HAS_TTL);
            onFinished();
            return;
        }
    }

    // check fulltext index
    auto ftIdxRet = getFTIndex(spaceId, edgeType);
    if (nebula::ok(ftIdxRet)) {
        auto fti = std::move(nebula::value(ftIdxRet));
        auto ftStatus = ftIndexCheck(fti.get_fields(), edgeItems);
        if (ftStatus != ErrorCode::SUCCEEDED) {
            handleErrorCode(ftStatus);
            onFinished();
            return;
        }
    } else if (nebula::error(ftIdxRet) != ErrorCode::E_META_FULLTEXT_INDEX_NOT_FOUND) {
        handleErrorCode(nebula::error(ftIdxRet));
        onFinished();
        return;
    }

    for (auto& edgeItem : edgeItems) {
        auto &cols = edgeItem.get_schema().get_columns();
        for (auto& col : cols) {
            auto retCode = MetaServiceUtils::alterColumnDefs(columns,
                                                             prop,
                                                             col,
                                                             *edgeItem.op_ref(),
                                                             true);
            if (retCode != ErrorCode::SUCCEEDED) {
                LOG(ERROR) << "Alter edge column error "
                           << apache::thrift::util::enumNameSafe(retCode);
                handleErrorCode(retCode);
                onFinished();
                return;
            }
        }
    }

    if (!SchemaUtil::checkType(columns)) {
        handleErrorCode(ErrorCode::E_META_SCHEMA_INVALID_DEFAULT_VALUE);
        onFinished();
        return;
    }

    // Update schema property if edge not index
    auto retCode = MetaServiceUtils::alterSchemaProp(columns,
                                                     prop,
                                                     alterSchemaProp,
                                                     existIndex,
                                                     true);
    if (retCode != ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Alter edge property error "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    schema.set_schema_prop(std::move(prop));
    schema.set_columns(std::move(columns));

    std::vector<kvstore::KV> data;
    LOG(INFO) << "Alter edge " << edgeName << ", edgeType " << edgeType;
    data.emplace_back(MetaServiceUtils::schemaEdgeKey(spaceId, edgeType, version),
                      MetaServiceUtils::schemaVal(edgeName, schema));
    resp_.set_id(to(edgeType, EntryType::EDGE));
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula
