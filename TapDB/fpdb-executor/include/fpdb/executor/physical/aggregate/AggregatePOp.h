//
// Created by matt on 11/12/19.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEPOP_H

#include <fpdb/executor/physical/aggregate/function/AggregateFunction.h>
#include <fpdb/executor/physical/aggregate/AggregateResult.h>
#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <memory>
#include <string>
#include <vector>
#include <filesystem>

#include <arrow/csv/options.h> 
#include <arrow/csv/reader.h>
#include <arrow/io/buffered.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/type_fwd.h>            // for default_memory_pool
#include <arrow/csv/api.h>

namespace shiftdb::native { class TapDBStateCodec; }

using namespace fpdb::executor::message;

namespace fpdb::executor::physical::aggregate {

class AggregatePOp : public fpdb::executor::physical::PhysicalOp {

public:
  AggregatePOp(string name,
               vector<string> projectColumnNames,
               int nodeId,
               vector<shared_ptr<AggregateFunction>> functions);
  AggregatePOp() = default;
  AggregatePOp(const AggregatePOp&) = default;
  AggregatePOp& operator=(const AggregatePOp&) = default;
  ~AggregatePOp() override = default;

  void onReceive(const Envelope &message) override;
  void clear() override;
  std::string getTypeString() const override;

  friend class ::shiftdb::native::TapDBStateCodec;

private:
  void onStart();
  void onDebug();
  void onTuple(const TupleMessage &message);
  void onComplete(const CompleteMessage &message);
  void onPredicate(const PredicateMessage &message);

  void compute(const shared_ptr<TupleSet> &tupleSet);
  shared_ptr<TupleSet> finalize();
  shared_ptr<TupleSet> finalizeEmpty();

  bool hasResult();
  
  vector<shared_ptr<AggregateFunction>> functions_;
  vector<vector<shared_ptr<AggregateResult>>> aggregateResults_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, AggregatePOp& op) {
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
                               f.field("functions", op.functions_),
                               f.field("aggregateResults", op.aggregateResults_),
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

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEPOP_H
