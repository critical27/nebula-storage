/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_UPDATENODE_H_
#define STORAGE_EXEC_UPDATENODE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "storage/context/StorageExpressionContext.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/FilterNode.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

// Only use for update vertex
// Update records, write to kvstore
class UpdateTagNode : public RelNode<VertexID> {
public:
    UpdateTagNode(PlanContext* planCtx,
                  TagContext* tagContext,
                  std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes,
                  std::vector<storage::cpp2::UpdatedProp>& updatedProps,
                  FilterNode<VertexID>* filterNode,
                  bool insertable,
                  StorageExpressionContext* expCtx = nullptr)
        : planContext_(planCtx)
        , tagContext_(tagContext)
        , indexes_(indexes)
        , updatedProps_(updatedProps)
        , filterNode_(filterNode)
        , insertable_(insertable)
        , expCtx_(expCtx) {
            tagId_ = planContext_->tagId_;
        }

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        CHECK_NOTNULL(planContext_->env_->kvstore_);

        folly::Baton<true, std::atomic> baton;
        auto ret = kvstore::ResultCode::SUCCEEDED;
        planContext_->env_->kvstore_->asyncAtomicOp(planContext_->spaceId_, partId,
            [&partId, &vId, this] ()
            -> folly::Optional<std::string> {
                this->exeResult_ = RelNode::execute(partId, vId);

                if (this->exeResult_ == kvstore::ResultCode::SUCCEEDED) {
                    if (this->planContext_->resultStat_ == ResultStatus::ILLEGAL_DATA) {
                        this->exeResult_ = kvstore::ResultCode::ERR_INVALID_DATA;
                        return folly::none;
                    } else if (this->planContext_->resultStat_ ==  ResultStatus::FILTER_OUT) {
                        this->exeResult_ = kvstore::ResultCode::ERR_RESULT_FILTERED;
                        return folly::none;
                    }

                    this->reader_ = filterNode_->reader();
                    // reset StorageExpressionContext reader_ to nullptr
                    this->expCtx_->reset();

                    if (!this->reader_ && this->insertable_) {
                        this->exeResult_ = this->insertTagProps(partId, vId);
                    } else if (this->reader_) {
                        this->key_ = filterNode_->key().str();
                        this->exeResult_ = this->collTagProp();
                    } else {
                        this->exeResult_ = kvstore::ResultCode::ERR_KEY_NOT_FOUND;
                    }

                    if (this->exeResult_ != kvstore::ResultCode::SUCCEEDED) {
                        return folly::none;
                    }
                    return this->updateAndWriteBack(partId, vId);
                } else {
                    // if tagnode/edgenode error
                    return folly::none;
                }
            },
            [&ret, &baton, this] (kvstore::ResultCode code) {
                if (code == kvstore::ResultCode::ERR_ATOMIC_OP_FAILED &&
                    this->exeResult_ != kvstore::ResultCode::SUCCEEDED) {
                    ret = this->exeResult_;
                } else {
                    ret = code;
                }
                baton.post();
            });
        baton.wait();

        return ret;
    }

    kvstore::ResultCode getLatestTagSchemaAndName() {
        auto schemaIter = tagContext_->schemas_.find(tagId_);
        if (schemaIter == tagContext_->schemas_.end() ||
            schemaIter->second.empty()) {
            return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
        }
        schema_ = schemaIter->second.back().get();
        if (!schema_) {
            LOG(ERROR) << "Get nullptr schema";
            return kvstore::ResultCode::ERR_UNKNOWN;
        }

        auto iter = tagContext_->tagNames_.find(tagId_);
        if (iter == tagContext_->tagNames_.end()) {
            VLOG(1) << "Can't find spaceId " << planContext_->spaceId_
                    << " tagId " << tagId_;
            return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
        }
        tagName_ = iter->second;
        return kvstore::ResultCode::SUCCEEDED;
    }

    // Insert props row,
    // For insert, condition is always true,
    // Props must have default value or nullable
    kvstore::ResultCode insertTagProps(PartitionID partId, const VertexID& vId) {
        planContext_->insert_ = true;
        auto ret = getLatestTagSchemaAndName();
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        // props must have default value or nullable
        // all fields values of this edge puts props_
        for (auto index = 0UL; index < schema_->getNumFields(); index++) {
            auto field = schema_->field(index);
            if (!field) {
                VLOG(1) << "Fail to read prop";
                return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
            }
            if (field->hasDefault()) {
                props_[field->name()] = field->defaultValue();
            } else if (field->nullable()) {
                props_[field->name()] = NullType::__NULL__;
            } else {
                return kvstore::ResultCode::ERR_INVALID_FIELD_VALUE;
            }
        }

        for (auto &e : props_) {
            expCtx_->setTagProp(tagName_, e.first, e.second);
        }

        // build key, value is emtpy
        auto version =
            std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
        // Switch version to big-endian, make sure the key is in ordered.
        version = folly::Endian::big(version);
        key_ = NebulaKeyUtils::vertexKey(planContext_->vIdLen_,
                                         partId, vId, tagId_, version);
        rowWriter_ = std::make_unique<RowWriterV2>(schema_);

        return kvstore::ResultCode::SUCCEEDED;
    }

    // collect tag prop
    kvstore::ResultCode collTagProp() {
        auto ret = getLatestTagSchemaAndName();
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        for (auto index = 0UL; index < schema_->getNumFields(); index++) {
            auto propName = std::string(schema_->getFieldName(index));
            VLOG(1) << "Collect prop " << propName << ", type " << tagId_;

            // read prop value, If the RowReader contains this field,
            // read from the rowreader, otherwise read the default value
            // or null value from the latest schema
            auto retVal = QueryUtils::readValue(reader_, propName, schema_);
            if (!retVal.ok()) {
                VLOG(1) << "Bad value for tag: " << tagId_
                        << ", prop " << propName;
                return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
            }
            props_[propName] = std::move(retVal.value());
        }

        for (auto &e : props_) {
            expCtx_->setTagProp(tagName_, e.first, e.second);
        }

        // After alter tag, the schema get from meta and the schema in RowReader
        // may be inconsistent, so the following method cannot be used
        // this->rowWriter_ = std::make_unique<RowWriterV2>(schema.get(), reader->getData());
        rowWriter_ = std::make_unique<RowWriterV2>(schema_);
        val_ = reader_->getData();
        return kvstore::ResultCode::SUCCEEDED;
    }

    folly::Optional<std::string>
    updateAndWriteBack(const PartitionID partId, const VertexID vId) {
        for (auto& updateProp : updatedProps_) {
            auto propName = updateProp.get_name();
            auto updateExp = Expression::decode(updateProp.get_value());
            if (!updateExp) {
                LOG(ERROR) << "Update expression decode failed " << updateProp.get_value();
                return folly::none;
            }
            auto updateVal = updateExp->eval(*expCtx_);
            // update prop value to props_
            props_[propName] = updateVal;
            // update expression context
            expCtx_->setTagProp(tagName_, propName, std::move(updateVal));
        }

        for (auto& e : props_) {
            auto wRet = rowWriter_->setValue(e.first, e.second);
            if (wRet != WriteResult::SUCCEEDED) {
                LOG(ERROR) << "Add field faild ";
                return folly::none;
            }
        }

        std::unique_ptr<kvstore::BatchHolder> batchHolder
            = std::make_unique<kvstore::BatchHolder>();

        auto wRet = rowWriter_->finish();
        if (wRet != WriteResult::SUCCEEDED) {
            LOG(ERROR) << "Add field faild ";
            return folly::none;
        }

        auto nVal = rowWriter_->moveEncodedStr();

        // update index if exists
        // Note: when insert_ is true, either there is no origin data or TTL expired
        // when there is no origin data, there is no the old index.
        // when TTL exists, there is no index.
        // when insert_ is true, not old index, val_ is empty.
        if (!indexes_.empty()) {
            std::unique_ptr<RowReader> nReader;
            for (auto& index : indexes_) {
                if (tagId_ == index->get_schema_id().get_tag_id()) {
                    // step 1, delete old version index if exists.
                    if (!val_.empty()) {
                        if (!reader_) {
                            LOG(ERROR) << "Bad format row";
                            return folly::none;
                        }
                        auto oi = indexKey(partId, vId, reader_, index);
                        if (!oi.empty()) {
                            batchHolder->remove(std::move(oi));
                        }
                    }

                    // step 2, insert new vertex index
                    if (!nReader) {
                        nReader = RowReader::getTagPropReader(planContext_->env_->schemaMan_,
                                                              planContext_->spaceId_,
                                                              tagId_,
                                                              nVal);
                    }
                    if (!nReader) {
                        LOG(ERROR) << "Bad format row";
                        return folly::none;
                    }
                    auto ni = indexKey(partId, vId, nReader.get(), index);
                    if (!ni.empty()) {
                        batchHolder->put(std::move(ni), "");
                    }
                }
            }
        }
        // step 3, insert new vertex data
        batchHolder->put(std::move(key_), std::move(nVal));
        return encodeBatchValue(batchHolder->getBatch());
    }

    std::string indexKey(PartitionID partId,
                         VertexID vId,
                         RowReader* reader,
                         std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
        std::vector<Value::Type> colsType;
        auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields(), colsType);
        if (!values.ok()) {
            return "";
        }
        return IndexKeyUtils::vertexIndexKey(planContext_->vIdLen_, partId, index->get_index_id(),
                                             vId, values.value(), colsType);
    }

private:
    // ============================ input =====================================================
    PlanContext                                                            *planContext_;
    TagContext                                                             *tagContext_;
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>             indexes_;
    // update <prop name, new value expression>
    std::vector<storage::cpp2::UpdatedProp>                                 updatedProps_;
    FilterNode<VertexID>                                                   *filterNode_;
    // Whether to allow insert
    bool                                                                    insertable_{false};
    TagID                                                                   tagId_;

    std::string                                                             key_;
    RowReader                                                              *reader_{nullptr};

    const meta::NebulaSchemaProvider                                       *schema_{nullptr};
    std::string                                                             tagName_;

    // use to save old row value
    std::string                                                             val_;
    std::unique_ptr<RowWriterV2>                                            rowWriter_;
    // tagId_ prop -> value
    std::unordered_map<std::string, Value>                                  props_;
    std::atomic<kvstore::ResultCode>                                        exeResult_;

    StorageExpressionContext                                               *expCtx_;
};

// Only use for update edge
// Update records, write to kvstore
class UpdateEdgeNode : public RelNode<cpp2::EdgeKey> {
public:
    UpdateEdgeNode(PlanContext* planCtx,
                   EdgeContext* edgeContext,
                   std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes,
                   std::vector<storage::cpp2::UpdatedProp>& updatedProps,
                   FilterNode<cpp2::EdgeKey>* filterNode,
                   bool insertable,
                   StorageExpressionContext* expCtx = nullptr)
        : planContext_(planCtx)
        , edgeContext_(edgeContext)
        , indexes_(indexes)
        , updatedProps_(updatedProps)
        , filterNode_(filterNode)
        , insertable_(insertable)
        , expCtx_(expCtx) {
            edgeType_ = planContext_->edgeType_;
        }

    kvstore::ResultCode execute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
        CHECK_NOTNULL(planContext_->env_->kvstore_);

        folly::Baton<true, std::atomic> baton;
        auto ret = kvstore::ResultCode::SUCCEEDED;
        planContext_->env_->kvstore_->asyncAtomicOp(planContext_->spaceId_, partId,
            [&partId, &edgeKey, this] ()
            -> folly::Optional<std::string> {
                this->exeResult_ = RelNode::execute(partId, edgeKey);
                if (this->exeResult_ == kvstore::ResultCode::SUCCEEDED) {
                    if (edgeKey.edge_type != this->edgeType_) {
                        this->exeResult_ = kvstore::ResultCode::ERR_KEY_NOT_FOUND;
                        return folly::none;
                    }
                    if (this->planContext_->resultStat_ == ResultStatus::ILLEGAL_DATA) {
                        this->exeResult_ = kvstore::ResultCode::ERR_INVALID_DATA;
                        return folly::none;
                    } else if (this->planContext_->resultStat_ ==  ResultStatus::FILTER_OUT) {
                        this->exeResult_ = kvstore::ResultCode::ERR_RESULT_FILTERED;
                        return folly::none;
                    }

                    this->reader_ = filterNode_->reader();
                    // reset StorageExpressionContext reader_ to nullptr
                    this->expCtx_->reset();

                    if (!this->reader_ && this->insertable_) {
                        this->exeResult_ = this->insertEdgeProps(partId, edgeKey);
                    } else if (this->reader_) {
                        this->key_ = filterNode_->key().str();
                        this->exeResult_ = this->collEdgeProp(edgeKey);
                    } else {
                        this->exeResult_ = kvstore::ResultCode::ERR_KEY_NOT_FOUND;
                    }

                    if (this->exeResult_ != kvstore::ResultCode::SUCCEEDED) {
                        return folly::none;
                    }
                    return this->updateAndWriteBack(partId, edgeKey);
                } else {
                    // If filter out, StorageExpressionContext is set in filterNode
                    return folly::none;
                }
            },
            [&ret, &baton, this] (kvstore::ResultCode code) {
                if (code == kvstore::ResultCode::ERR_ATOMIC_OP_FAILED &&
                    this->exeResult_ != kvstore::ResultCode::SUCCEEDED) {
                    ret = this->exeResult_;
                } else {
                    ret = code;
                }
                baton.post();
            });
        baton.wait();

        return ret;
    }

    kvstore::ResultCode getLatestEdgeSchemaAndName() {
        auto schemaIter = edgeContext_->schemas_.find(std::abs(edgeType_));
        if (schemaIter == edgeContext_->schemas_.end() ||
            schemaIter->second.empty()) {
            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
        }
        schema_ = schemaIter->second.back().get();
        if (!schema_) {
            LOG(ERROR) << "Get nullptr schema";
            return kvstore::ResultCode::ERR_UNKNOWN;
        }

        auto iter = edgeContext_->edgeNames_.find(edgeType_);
        if (iter == edgeContext_->edgeNames_.end()) {
            VLOG(1) << "Can't find spaceId " << planContext_->spaceId_
                    << " edgeType " << edgeType_;
            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
        }
        edgeName_ = iter->second;
        return kvstore::ResultCode::SUCCEEDED;
    }

    // Insert props row,
    // Operator props must have default value or nullable
    kvstore::ResultCode insertEdgeProps(const PartitionID partId, const cpp2::EdgeKey& edgeKey) {
        planContext_->insert_ = true;
        auto ret = getLatestEdgeSchemaAndName();
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        // props must have default value or nullable
        // all fields values of this edge puts updateContext_
        for (auto index = 0UL; index < schema_->getNumFields(); index++) {
            auto field = schema_->field(index);
            if (!field) {
                VLOG(1) << "Fail to read prop";
                return kvstore::ResultCode::ERR_EDGE_PROP_NOT_FOUND;
            }
            if (field->hasDefault()) {
                props_[field->name()] = field->defaultValue();
            } else if (field->nullable()) {
                props_[field->name()] = NullType::__NULL__;
            } else {
                return kvstore::ResultCode::ERR_INVALID_FIELD_VALUE;
            }
        }

        // build expression context
        // add _src, _type, _rank, _dst
        expCtx_->setEdgeProp(edgeName_, kSrc, edgeKey.src);
        expCtx_->setEdgeProp(edgeName_, kDst, edgeKey.dst);
        expCtx_->setEdgeProp(edgeName_, kRank, edgeKey.ranking);
        expCtx_->setEdgeProp(edgeName_, kType, edgeKey.edge_type);

        for (auto &e : props_) {
            expCtx_->setEdgeProp(edgeName_, e.first, e.second);
        }

        // build key, value is emtpy
        auto version =
            std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
        // Switch version to big-endian, make sure the key is in ordered.
        version = folly::Endian::big(version);
        key_ = NebulaKeyUtils::edgeKey(planContext_->vIdLen_,
                                       partId,
                                       edgeKey.src,
                                       edgeKey.edge_type,
                                       edgeKey.ranking,
                                       edgeKey.dst,
                                       version);
        rowWriter_ = std::make_unique<RowWriterV2>(schema_);

        return kvstore::ResultCode::SUCCEEDED;
    }

    // Collect edge prop
    kvstore::ResultCode collEdgeProp(const cpp2::EdgeKey& edgeKey) {
        auto ret = getLatestEdgeSchemaAndName();
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        for (auto index = 0UL; index < schema_->getNumFields(); index++) {
            auto propName = std::string(schema_->getFieldName(index));
            VLOG(1) << "Collect prop " << propName << ", edgeType " << edgeType_;

            // Read prop value, If the RowReader contains this field,
            // read from the rowreader, otherwise read the default value
            // or null value from the latest schema
            auto retVal = QueryUtils::readValue(reader_, propName, schema_);
            if (!retVal.ok()) {
                VLOG(1) << "Bad value for edge: " << edgeType_
                        << ", prop " << propName;
                return kvstore::ResultCode::ERR_EDGE_PROP_NOT_FOUND;
            }
            props_[propName] = std::move(retVal.value());
        }

        // build expression context
        // add _src, _type, _rank, _dst
        expCtx_->setEdgeProp(edgeName_, kSrc, edgeKey.src);
        expCtx_->setEdgeProp(edgeName_, kDst, edgeKey.dst);
        expCtx_->setEdgeProp(edgeName_, kRank, edgeKey.ranking);
        expCtx_->setEdgeProp(edgeName_, kType, edgeKey.edge_type);

        for (auto &e : props_) {
            expCtx_->setEdgeProp(edgeName_, e.first, e.second);
        }

        // After alter edge, the schema get from meta and the schema in RowReader
        // may be inconsistent, so the following method cannot be used
        // this->rowWriter_ = std::make_unique<RowWriterV2>(schema.get(), reader->getData());
        rowWriter_ = std::make_unique<RowWriterV2>(schema_);
        val_ = reader_->getData();
        return kvstore::ResultCode::SUCCEEDED;
    }

    folly::Optional<std::string>
    updateAndWriteBack(const PartitionID partId, const cpp2::EdgeKey& edgeKey) {
        for (auto& updateProp : updatedProps_) {
            auto propName = updateProp.get_name();
            auto updateExp = Expression::decode(updateProp.get_value());
            if (!updateExp) {
                return folly::none;
            }
            auto updateVal = updateExp->eval(*expCtx_);
            // update prop value to updateContext_
            props_[propName] = updateVal;
            // update expression context
            expCtx_->setEdgeProp(edgeName_, propName, std::move(updateVal));
        }

        for (auto& e : props_) {
            auto wRet = rowWriter_->setValue(e.first, e.second);
            if (wRet != WriteResult::SUCCEEDED) {
                VLOG(1) << "Add field faild ";
                return folly::none;
            }
        }

        std::unique_ptr<kvstore::BatchHolder> batchHolder
            = std::make_unique<kvstore::BatchHolder>();

        auto wRet = rowWriter_->finish();
        if (wRet != WriteResult::SUCCEEDED) {
            VLOG(1) << "Add field faild ";
            return folly::none;
        }

        auto nVal = rowWriter_->moveEncodedStr();
        // update index if exists
        // Note: when insert_ is true, either there is no origin data or TTL expired
        // when there is no origin data, there is no the old index.
        // when TTL exists, there is no index.
        // when insert_ is true, not old index, val_ is empty.
        if (!indexes_.empty()) {
            std::unique_ptr<RowReader> nReader;
            for (auto& index : indexes_) {
                if (edgeType_ == index->get_schema_id().get_edge_type()) {
                    // step 1, delete old version index if exists.
                    if (!val_.empty()) {
                        if (!reader_) {
                            LOG(ERROR) << "Bad format row";
                            return folly::none;
                        }
                        auto oi = indexKey(partId, reader_, edgeKey, index);
                        if (!oi.empty()) {
                            batchHolder->remove(std::move(oi));
                        }
                    }

                    // step 2, insert new edge index
                    if (!nReader) {
                        nReader = RowReader::getEdgePropReader(planContext_->env_->schemaMan_,
                                                               planContext_->spaceId_,
                                                               std::abs(edgeType_),
                                                               nVal);
                    }
                    if (!nReader) {
                        LOG(ERROR) << "Bad format row";
                        return folly::none;
                    }
                    auto ni = indexKey(partId, nReader.get(), edgeKey, index);
                    if (!ni.empty()) {
                        batchHolder->put(std::move(ni), "");
                    }
                }
            }
        }
        // step 3, insert new edge data
        batchHolder->put(std::move(key_), std::move(nVal));
        return encodeBatchValue(batchHolder->getBatch());
    }

    std::string indexKey(PartitionID partId,
                         RowReader* reader,
                         const cpp2::EdgeKey& edgeKey,
                         std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
        std::vector<Value::Type> colsType;
        auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields(), colsType);
        if (!values.ok()) {
            return "";
        }
        return IndexKeyUtils::edgeIndexKey(planContext_->vIdLen_,
                                           partId,
                                           index->get_index_id(),
                                           edgeKey.get_src(),
                                           edgeKey.get_ranking(),
                                           edgeKey.get_dst(),
                                           values.value(),
                                           colsType);
    }

private:
    // ============================ input =====================================================
    PlanContext                                                            *planContext_;
    EdgeContext                                                            *edgeContext_;
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>             indexes_;
    // update <prop name, new value expression>
    std::vector<storage::cpp2::UpdatedProp>                                 updatedProps_;
    FilterNode<cpp2::EdgeKey>                                              *filterNode_;

    // Whether to allow insert
    bool                                                                    insertable_{false};
    EdgeType                                                                edgeType_;

    std::string                                                             key_;
    RowReader                                                              *reader_{nullptr};

    const meta::NebulaSchemaProvider                                       *schema_{nullptr};
    std::string                                                             edgeName_;
    // use to save old row value
    std::string                                                             val_;
    std::unique_ptr<RowWriterV2>                                            rowWriter_;

    // edgeType_ prop -> value
    std::unordered_map<std::string, Value>                                  props_;
    std::atomic<kvstore::ResultCode>                                        exeResult_;

    StorageExpressionContext                                               *expCtx_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
