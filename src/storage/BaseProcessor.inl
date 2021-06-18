/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

template <typename RESP>
void BaseProcessor<RESP>::handleAsync(GraphSpaceID spaceId,
                                      PartitionID partId,
                                      ErrorCode code) {
    VLOG(3) << "partId:" << partId << ", code: " << static_cast<int32_t>(code);

    bool finished = false;
    {
        std::lock_guard<std::mutex> lg(this->lock_);
        handleErrorCode(code, spaceId, partId);
        this->callingNum_--;
        if (this->callingNum_ == 0) {
            finished = true;
        }
    }

    if (finished) {
        this->onFinished();
    }
}

template <typename RESP>
meta::cpp2::ColumnDef
BaseProcessor<RESP>::columnDef(std::string name, meta::cpp2::PropertyType type) {
    nebula::meta::cpp2::ColumnDef column;
    column.set_name(std::move(name));
    column.set_type(type);
    return column;
}

template <typename RESP>
void BaseProcessor<RESP>::pushResultCode(ErrorCode code,
                                         PartitionID partId,
                                         HostAddr leader) {
    if (code != ErrorCode::SUCCEEDED) {
        cpp2::PartitionResult thriftRet;
        thriftRet.set_code(code);
        thriftRet.set_part_id(partId);
        if (leader != HostAddr("", 0)) {
            thriftRet.set_leader(leader);
        }
        codes_.emplace_back(std::move(thriftRet));
    }
}

template <typename RESP>
void BaseProcessor<RESP>::handleErrorCode(ErrorCode code,
                                          GraphSpaceID spaceId,
                                          PartitionID partId) {
    if (code != ErrorCode::SUCCEEDED) {
        if (code == ErrorCode::E_LEADER_CHANGED) {
            handleLeaderChanged(spaceId, partId);
        } else {
            pushResultCode(code, partId);
        }
    }
}

template <typename RESP>
void BaseProcessor<RESP>::handleLeaderChanged(GraphSpaceID spaceId,
                                              PartitionID partId) {
    auto addrRet = env_->kvstore_->partLeader(spaceId, partId);
    if (ok(addrRet)) {
        auto leader = value(std::move(addrRet));
        this->pushResultCode(ErrorCode::E_LEADER_CHANGED, partId, leader);
    } else {
        LOG(ERROR) << "Fail to get part leader, spaceId: " << spaceId
                   << ", partId: " << partId << ", ResultCode: "
                   << static_cast<int32_t>(error(addrRet));
        this->pushResultCode(error(addrRet), partId);
    }
}

template <typename RESP>
void BaseProcessor<RESP>::doPut(GraphSpaceID spaceId,
                                PartitionID partId,
                                std::vector<kvstore::KV>&& data) {
    this->env_->kvstore_->asyncMultiPut(
        spaceId, partId, std::move(data), [spaceId, partId, this](ErrorCode code) {
            handleAsync(spaceId, partId, code);
    });
}

template <typename RESP>
ErrorCode
BaseProcessor<RESP>::doSyncPut(GraphSpaceID spaceId,
                               PartitionID partId,
                               std::vector<kvstore::KV>&& data) {
    folly::Baton<true, std::atomic> baton;
    auto ret = ErrorCode::SUCCEEDED;
    env_->kvstore_->asyncMultiPut(spaceId,
                                  partId,
                                  std::move(data),
                                  [&ret, &baton] (ErrorCode code) {
        if (ErrorCode::SUCCEEDED != code) {
            ret = code;
        }
        baton.post();
    });
    baton.wait();
    return ret;
}

template <typename RESP>
void BaseProcessor<RESP>::doRemove(GraphSpaceID spaceId,
                                   PartitionID partId,
                                   std::vector<std::string>&& keys) {
    this->env_->kvstore_->asyncMultiRemove(
        spaceId, partId, std::move(keys), [spaceId, partId, this](ErrorCode code) {
        handleAsync(spaceId, partId, code);
    });
}

template <typename RESP>
void BaseProcessor<RESP>::doRemoveRange(GraphSpaceID spaceId,
                                        PartitionID partId,
                                        const std::string& start,
                                        const std::string& end) {
    this->env_->kvstore_->asyncRemoveRange(
        spaceId, partId, start, end, [spaceId, partId, this](ErrorCode code) {
            handleAsync(spaceId, partId, code);
        });
}

template <typename RESP>
ErrorOr<ErrorCode, std::string>
BaseProcessor<RESP>::encodeRowVal(const meta::NebulaSchemaProvider* schema,
                                  const std::vector<std::string>& propNames,
                                  const std::vector<Value>& props) {
    RowWriterV2 rowWrite(schema);
    // If req.prop_names is not empty, use the property name in req.prop_names
    // Otherwise, use property name in schema
    if (!propNames.empty()) {
        for (size_t i = 0; i < propNames.size(); i++) {
            wRet = rowWrite.setValue(propNames[i], props[i]);
            if (wRet != ErrorCode::SUCCEEDED) {
                return wRet;
            }
        }
    } else {
        for (size_t i = 0; i < props.size(); i++) {
            wRet = rowWrite.setValue(i, props[i]);
            if (wRet != ErrorCode::SUCCEEDED) {
                return wRet;
            }
        }
    }

    wRet = rowWrite.finish();
    if (wRet != ErrorCode::SUCCEEDED) {
        return wRet;
    }

    return std::move(rowWrite).moveEncodedStr();
}

}  // namespace storage
}  // namespace nebula
