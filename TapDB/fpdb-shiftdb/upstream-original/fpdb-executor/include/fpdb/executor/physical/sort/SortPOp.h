//
// Created by Yifei Yang on 11/20/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SORT_SORTPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SORT_SORTPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/plan/prephysical/SortKey.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <arrow/compute/api.h>
#include <fpdb/executor/physical/POpContext.h>

using namespace fpdb::executor::message;
using namespace fpdb::plan::prephysical;
using namespace std;

namespace fpdb::executor::physical::sort {

class SortPOp : public PhysicalOp {

public:
  SortPOp(const string &name,
          const vector<string> &projectColumnNames,
          int nodeId,
          const vector<SortKey> &sortKeys);
  SortPOp() = default;
  SortPOp(const SortPOp&) = default;
  SortPOp& operator=(const SortPOp&) = default;

  void onReceive(const Envelope &msg) override;
  void clear() override;
  std::string getTypeString() const override;
  void send(bool force) override;

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTuple(const TupleMessage &message);

  void makeArrowSortOptions();
  void buffer(const shared_ptr<TupleSet> &tupleSet);
  shared_ptr<TupleSet> sort(bool readyToRun);
  void sort_single_file();

  vector<SortKey> sortKeys_;
  std::optional<arrow::compute::SortOptions> arrowSortOptions_;
  std::optional<shared_ptr<TupleSet>> buffer_;
  int totBuild_ = 0;
  int arrivedBuild_ = 0;
  int64_t MYMEMORYSIZE;
  int64_t LINESIZE=0;
  int64_t allRows=0;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SortPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("predictor", op.predictors_),
                               f.field("samplers", op.samplers_),
                               f.field("Pred2link", op.Pred2link),
                               f.field("sortKeys", op.sortKeys_),
                               f.field("spawnOnRemote", op.shouldSpawnOnRemote_),
                               f.field("receiveTupleSet", op.receiveTupleSet),
                               f.field("sendToPred", op.sendToPred),
                               f.field("recvFromPred", op.recvFromPred),
                               f.field("link2Pred", op.link2Pred),
                               f.field("recvAllCompleteMsg", op.recvAllCompleteMsg_),
                               f.field("nextOpLocation", op.nextOpLocation_),
                               f.field("fileBeginIdx", op.fileBeginIdx_),
                               f.field("fileEndIdx", op.fileEndIdx_),
                               f.field("tupleBeginIdx", op.tupleBeginIdx_),
                               f.field("tupleEndIdx", op.tupleEndIdx_),
                               f.field("consumerfactor", op.consumerFactor_),
                               f.field("factor", op.factor_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SORT_SORTPOP_H