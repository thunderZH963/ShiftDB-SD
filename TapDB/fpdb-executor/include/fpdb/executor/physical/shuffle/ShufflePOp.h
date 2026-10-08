//
// Created by matt on 17/6/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/tuple/TupleSet.h>
#include <filesystem>

#include <arrow/csv/options.h> 
#include <arrow/csv/reader.h>
#include <arrow/io/buffered.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/type_fwd.h>            // for default_memory_pool
#include <arrow/csv/api.h>

using namespace fpdb::executor::message;
using namespace fpdb::tuple;
using namespace std;

namespace fpdb::executor::physical::shuffle {

/**
 * A firts cut of a shuffle operator, shuffles on a single key only
 */
class ShufflePOp : public PhysicalOp {

public:
  ShufflePOp(string name,
             vector<string> projectColumnNames,
             int nodeId,
             vector<string> shuffleColumnNames);
  ShufflePOp() = default;
  ShufflePOp(const ShufflePOp&) = default;
  ShufflePOp& operator=(const ShufflePOp&) = default;

  /**
   * Operators message handler
   * @param msg
   */
  void onReceive(const Envelope &msg) override;
  void clear() override;
  std::string getTypeString() const override;

  /**
   * Set the producer operator
   * @param operator_
   */
  void produce(const shared_ptr<PhysicalOp> &operator_) override;

private:
  /**
   * Start message handler
   */
  void onStart();

  void onDebug();

  /**
   * Completion message handler
   */
  void onComplete(const CompleteMessage &);

  /**
   * Tuples message handler
   * @param message
   */
  void onTuple(const TupleMessage &message);

  /**
   * Adds the tuple set to the outbound buffer for the given slot
   * @param tupleSet
   * @param partitionIndex
   * @return
   */
  [[nodiscard]] tl::expected<void, string> buffer(const shared_ptr<TupleSet>& tupleSet, int partitionIndex);

  /**
   * Sends the buffered tupleset if its big enough or force is true
   * @param partitionIndex
   * @param force
   * @return
   */
  [[nodiscard]] tl::expected<void, string> send(int partitionIndex, bool force);

  vector<string> shuffleColumnNames_;
  vector<string> consumerVec_;
  vector<std::optional<shared_ptr<TupleSet>>> buffers_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ShufflePOp& op) {
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
                               f.field("shuffleColumnNames", op.shuffleColumnNames_),
                               f.field("consumerVec", op.consumerVec_),
                               f.field("fileBeginIdx", op.fileBeginIdx_),
                               f.field("fileEndIdx", op.fileEndIdx_),
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

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEPOP_H
