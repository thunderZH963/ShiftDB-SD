//
// Created by Czk on 04/12/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TEST_PREDICATEPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TEST_PREDICATEPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/POpType.h>
#include <fpdb/executor/message/PredicateMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/expression/gandiva/Expression.h>
#include <memory>

using namespace fpdb::executor::message;
using namespace fpdb::catalogue;
using namespace fpdb::executor::physical;

namespace fpdb::executor::physical::predicate {

class PredicatePOp : public PhysicalOp {

public:
  explicit PredicatePOp(std::string name,
                  std::vector<std::string> projectColumnNames,
                  int nodeId,
                  POpType targetType);
  PredicatePOp() = default;
  PredicatePOp(const PredicatePOp&) = default;
  PredicatePOp& operator=(const PredicatePOp&) = default;

  void onReceive(const Envelope &Envelope) override;
  void clear() override;
  std::string getTypeString() const override;

private:
  void onStart();
  void onPredicate(const PredicateMessage& Message);
  void onComplete(const CompleteMessage& Message);

  int send2model(std::string opName, uint64_t inputSize, int location, uint64_t inputRow);
  int send2model(std::string opName, uint64_t inputSize, int location, uint64_t inputRow, uint64_t hashSize, uint64_t hashRow);

  bool recvFromTupleSet_ = false;//从producer收到基础信息
  bool recvFromBuild_ = false;//从另一个producer收到基础信息
  bool recvFromConsumer0_=false;//从0consumer收到采样时间
  bool recvFromConsumer_1_=false;//从-1consumer收到采样时间
  bool requested_=false;
  uint64_t hashRows_ = 0;
  uint64_t tupleRows_ = 0;
  uint64_t hashSize_ = 0;
  uint64_t tupleSize_ = 0;
  POpType targetType_;
  int producerLoc_=-1;
  std::string consumerOpName_;
  uint64_t sampleTime0_=0;//ns
  uint64_t sampleTime_1_=0;//ns _1=-1
// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, PredicatePOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("targetType", op.targetType_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("predictor", op.predictors_),
                               f.field("samplers", op.samplers_),
                               f.field("Pred2link", op.Pred2link),
                               f.field("recvFromTupleSet", op.recvFromTupleSet_),
                               f.field("recvFromBuild", op.recvFromBuild_),
                               f.field("hashRows", op.hashRows_),
                               f.field("tupleRows", op.tupleRows_),
                               f.field("hashSize", op.hashSize_),
                               f.field("tupleSize", op.tupleSize_),
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

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TEST_PREDICATEPOP_H
