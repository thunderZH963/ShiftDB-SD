//
// Created by matt on 6/5/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILTER_FILTERPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILTER_FILTERPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/PredicateMessage.h>
#include <fpdb/cache/SegmentKey.h>
#include <fpdb/catalogue/Table.h>
#include <fpdb/expression/gandiva/Expression.h>
#include <fpdb/expression/Filter.h>
#include <fpdb/tuple/TupleSet.h>
#include <memory>
#include <filesystem>

#include <arrow/csv/options.h> 
#include <arrow/csv/reader.h>
#include <arrow/io/buffered.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/type_fwd.h>            // for default_memory_pool
#include <arrow/csv/api.h>

using namespace fpdb::executor::message;
using namespace fpdb::catalogue;

namespace fpdb::executor::physical::filter {

class FilterPOp : public PhysicalOp {

public:
  explicit FilterPOp(std::string name,
                  std::vector<std::string> projectColumnNames,
                  int nodeId,
                  std::shared_ptr<fpdb::expression::gandiva::Expression> predicate,
                  std::shared_ptr<Table> table,
                  std::vector<std::shared_ptr<fpdb::cache::SegmentKey>> weightedSegmentKeys = {});
  FilterPOp() = default;
  FilterPOp(const FilterPOp&) = default;
  FilterPOp& operator=(const FilterPOp&) = default;

  void onReceive(const Envelope &Envelope) override;
  void clear() override;
  std::string getTypeString() const override;

  [[nodiscard]] size_t getFilterTimeNS() const;
  [[nodiscard]] size_t getFilterInputBytes() const;
  [[nodiscard]] size_t getFilterOutputBytes() const;

private:
  shared_ptr<fpdb::expression::gandiva::Expression> predicate_;

  std::optional<std::shared_ptr<fpdb::expression::Filter>> filter_;

  /**
   * A buffer of received tuples not yet filtered
   */
  std::shared_ptr<fpdb::tuple::TupleSet> received_;

  /**
   * A buffer of filtered tuples not yet sent
   */
  std::shared_ptr<fpdb::tuple::TupleSet> filtered_;

  void onStart();
  void onDebug();
  void onTuple(const TupleMessage& Message);
  void onComplete(const CompleteMessage& Message);
  void onPredicate(const PredicateMessage &message);

  void bufferTuples(const std::shared_ptr<fpdb::tuple::TupleSet>& tupleSet);
  void buildFilter();
  void filterTuples();
  void sendTuples();
  void sendSegmentWeight();

  bool isApplicable(const std::shared_ptr<fpdb::tuple::TupleSet>& tupleSet);

  long totalNumRows_ = 0;
  long filteredNumRows_ = 0;
  size_t filterTimeNS_ = 0;
  size_t inputBytesFiltered_ = 0;
  size_t outputBytesFiltered_ = 0;

  /**
   * Whether all predicate columns are covered in the schema of received tuples
   */
  std::shared_ptr<bool> applicable_;

  /**
   * Used to compute filter weight, set to nullptr and {} if its producer is not table scan
   */
  std::shared_ptr<Table> table_;
  std::vector<std::shared_ptr<fpdb::cache::SegmentKey>> weightedSegmentKeys_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, FilterPOp& op) {
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
                               f.field("predicate", op.predicate_),
                               f.field("received", op.received_),
                               f.field("filtered", op.filtered_),
                               f.field("table", op.table_),
                               f.field("weightedSegmentKeys", op.weightedSegmentKeys_),
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

inline bool recordSpeeds = false;
inline size_t totalBytesFiltered_ = 0;
}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILTER_FILTERPOP_H
