/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_UPDATERESULTNODE_H_
#define STORAGE_EXEC_UPDATERESULTNODE_H_

#include "common/base/Base.h"
#include "storage/exec/UpdateNode.h"
#include "storage/context/StorageExpressionContext.h"

namespace nebula {
namespace storage {

template<typename T>
class UpdateResNode : public RelNode<T>  {
public:
    using RelNode<T>::execute;

    UpdateResNode(PlanContext* planCtx,
                  RelNode<T>* updateNode,
                  std::vector<Expression*> returnPropsExp,
                  StorageExpressionContext* expCtx,
                  nebula::DataSet* result)
        : planContext_(planCtx)
        , updateNode_(updateNode)
        , returnPropsExp_(returnPropsExp)
        , expCtx_(expCtx)
        , result_(result) {
        }

    ErrorCode execute(PartitionID partId, const T& vId) override {
        auto ret = RelNode<T>::execute(partId, vId);
        if (ret != ErrorCode::SUCCEEDED &&
            ret != ErrorCode::E_STORAGE_QUERY_FILTER_NOT_PASSED) {
            return ret;
        }

        insert_ = planContext_->insert_;

        // Note: If filtered out, the result of tag prop is old
        result_->colNames.emplace_back("_inserted");
        std::vector<Value> row;
        row.emplace_back(insert_);

        for (auto& retExp : returnPropsExp_) {
            auto exp = static_cast<PropertyExpression*>(retExp);
            auto& val = exp->eval(*expCtx_);
            if (exp) {
                result_->colNames.emplace_back(folly::stringPrintf("%s.%s",
                                               exp->sym().c_str(),
                                               exp->prop().c_str()));
            } else {
                VLOG(1) << "Can't get expression name";
                result_->colNames.emplace_back("NULL");
            }
            row.emplace_back(std::move(val));
        }
        result_->rows.emplace_back(std::move(row));
        return ret;
    }

private:
    PlanContext                                             *planContext_;
    RelNode<T>                                              *updateNode_;
    std::vector<Expression*>                                 returnPropsExp_;
    StorageExpressionContext                                *expCtx_;

    // return prop sets
    nebula::DataSet                                         *result_;
    bool                                                     insert_{false};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_UPDATERESULTNODE_H_
