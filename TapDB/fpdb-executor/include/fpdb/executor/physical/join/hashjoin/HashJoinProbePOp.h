//
// Created by matt on 29/4/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinProbeAbstractKernel.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/executor/message/PredicateMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/TupleSetIndexMessage.h>
#include <fpdb/plan/prephysical/JoinType.h>

using namespace fpdb::executor::message;
using namespace fpdb::plan::prephysical;
using namespace std;

namespace fpdb::executor::physical::join {

/**
 * Operator implementing probe phase of a hash join
 *
 * Takes hashtable produced from build phase on one of the relations in the join (ideally the smaller) and uses it
 * to select rows from the both relations to include in the join.
 *
 */
class HashJoinProbePOp : public PhysicalOp {

public:
  HashJoinProbePOp(string name,
                   vector<string> projectColumnNames,
                   int nodeId,
                   const HashJoinPredicate& pred,
                   JoinType joinType);
  HashJoinProbePOp() = default;
  HashJoinProbePOp(const HashJoinProbePOp&) = default;
  HashJoinProbePOp& operator=(const HashJoinProbePOp&) = default;

  void onReceive(const Envelope &msg) override;
  void clear() override;
  void send(bool force) override;
  std::string getTypeString() const override;

private:
  void onStart();
  void onTuple(const TupleMessage &msg);
  void onHashTable(const TupleSetIndexMessage &msg);
  void onComplete(const CompleteMessage &msg);
  void onDebug();
  void onPredicate(const PredicateMessage &message);

  void sendEmpty();

  shared_ptr<HashJoinProbeAbstractKernel> kernel_;
  bool sentResult = false;

  int totBuild_ = 0;
  int arrivedBuild_ = 0;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, HashJoinProbePOp& op) {
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

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEPOP_H
