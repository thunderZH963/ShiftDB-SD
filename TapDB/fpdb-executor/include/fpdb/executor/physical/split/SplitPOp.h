//
// Created by Yifei Yang on 12/13/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SPLIT_SPLITPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SPLIT_SPLITPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/tuple/TupleSet.h>

using namespace fpdb::executor::message;
using namespace fpdb::tuple;
using namespace std;

namespace fpdb::executor::physical::split {

class SplitPOp : public PhysicalOp {

public:
  SplitPOp(const string &name,
           const vector<string> &projectColumnNames,
           int nodeId);
  SplitPOp() = default;
  SplitPOp(const SplitPOp&) = default;
  SplitPOp& operator=(const SplitPOp&) = default;

  void onReceive(const Envelope &msg) override;
  std::string getTypeString() const override;
  void clear() override;
  void produce(const shared_ptr<PhysicalOp> &op) override;

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTuple(const TupleMessage &message);
  void onDebug();
  void onPredicate(const PredicateMessage &message);

  tl::expected<void, string> splitAndSend();
  tl::expected<void, string> bufferInput(const shared_ptr<TupleSet>& tupleSet);
  tl::expected<vector<shared_ptr<TupleSet>>, string> split();
  void send(const vector<shared_ptr<TupleSet>> &tupleSets);

  vector<string> consumerVec_;
  std::optional<shared_ptr<TupleSet>> inputTupleSet_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SplitPOp& op) {
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
                               f.field("consumerVec", op.consumerVec_),
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
                               f.field("factor", op.factor_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SPLIT_SPLITPOP_H
