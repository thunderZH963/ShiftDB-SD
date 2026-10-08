//
// Created by matt on 29/4/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDPOP_H

#include <fpdb/executor/physical/join/hashjoin/HashJoinBuildKernel.h>
#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/PredicateMessage.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <arrow/scalar.h>
#include <unordered_map>
#include <filesystem>
#include <vector>
#include <string>

#include <arrow/csv/options.h> 
#include <arrow/csv/reader.h>
#include <arrow/io/buffered.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/type_fwd.h>            // for default_memory_pool
#include <arrow/csv/api.h>

using namespace fpdb::executor::message;
using namespace std;

namespace fpdb::executor::physical::join {

/**
 * Operator implementing build phase of a hash join
 *
 * Builds a hash table of tuples from one of the relations in a join (ideally the smaller relation). That hashtable is
 * then used in the probe phase to select rows to add to the final joined relation.
 *
 */
class HashJoinBuildPOp : public PhysicalOp {

public:
  explicit HashJoinBuildPOp(const string &name,
                            const vector<string> &projectColumnNames,
                            int nodeId,
                            const vector<string> &predColumnNames);
  HashJoinBuildPOp() = default;
  HashJoinBuildPOp(const HashJoinBuildPOp&) = default;
  HashJoinBuildPOp& operator=(const HashJoinBuildPOp&) = default;

  void onReceive(const Envelope &msg) override;
  void clear() override;
  std::string getTypeString() const override;

  void sendSavedTuple() override;

private:
  void onStart();
  void onDebug();
  void onTuple(const TupleMessage &msg);
  void onComplete(const CompleteMessage &msg);
  void onPredicate(const PredicateMessage &message);

  tl::expected<void, string> buffer(const shared_ptr<TupleSet>& tupleSet);
  void send(bool force) override;

  HashJoinBuildKernel kernel_;

  std::vector<std::string> columnName_; 

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, HashJoinBuildPOp& op) {
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
                               f.field("kernel", op.kernel_),
                               f.field("columnName", op.columnName_),
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

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDPOP_H
