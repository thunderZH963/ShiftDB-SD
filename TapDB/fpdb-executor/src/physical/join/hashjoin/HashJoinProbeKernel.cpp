//
// Created by matt on 31/7/20.
//

#include <fpdb/executor/physical/join/hashjoin/HashJoinProbeKernel.h>
#include <arrow/api.h>
#include <memory>                      // for make_unique, unique_ptr, __sha...
#include <utility>
#include <filesystem>
#include <arrow/csv/options.h> 
#include <arrow/csv/reader.h>
#include <arrow/io/buffered.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>

using namespace fpdb::tuple;

namespace fpdb::executor::physical::join {

HashJoinProbeKernel::HashJoinProbeKernel(HashJoinPredicate pred,
                                         set<string> neededColumnNames,
                                         bool isLeft,
                                         bool isRight,
                                         std::string name) :
  HashJoinProbeAbstractKernel(move(pred), move(neededColumnNames), move(name)),
  isLeft_(isLeft),
  isRight_(isRight) {}

shared_ptr<HashJoinProbeKernel> HashJoinProbeKernel::make(HashJoinPredicate pred,
                                                          set<string> neededColumnNames,
                                                          bool isLeft,
                                                          bool isRight,
                                                          std::string name) {
  return make_shared<HashJoinProbeKernel>(move(pred), move(neededColumnNames), isLeft, isRight, move(name));
}

bool HashJoinProbeKernel::isSemi() const {
  return false;
}

tl::expected<shared_ptr<fpdb::tuple::TupleSet>, string>
HashJoinProbeKernel::join(const shared_ptr<RecordBatchHashJoiner> &joiner,
                          const shared_ptr<TupleSet> &probeTupleSet,
                          int64_t probeRowOffset) {
  ::arrow::Result<shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;

  // std::cout << name_ + "join begin" << std::endl;

  // Read the table a batch at a time
  auto probeTable = probeTupleSet->table();
  ::arrow::TableBatchReader reader{*probeTable};
  reader.set_chunksize((int64_t) DefaultChunkSize);

  // Read a batch
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
    return tl::make_unexpected(recordBatchResult.status().message());
  }
  auto recordBatch = *recordBatchResult;
  int64_t localProbeRowOffset = 0;

  while (recordBatch) {

    // Join
    auto result = joiner->join(recordBatch, probeRowOffset + localProbeRowOffset);
    if (!result.has_value()) {
      return tl::make_unexpected(result.error());
    }

    // Increment localProbeRowOffset
    localProbeRowOffset += recordBatch->num_rows();

    // Read a batch
    recordBatchResult = reader.Next();
    if (!recordBatchResult.ok()) {
      return tl::make_unexpected(recordBatchResult.status().message());
    }
    recordBatch = *recordBatchResult;
  }

  // Save row match indexes
  if (leftJoinHelper_.has_value()) {
    // leftJoinHelper_.value()->putRowMatchIndexes(joiner->getBuildRowMatchIndexes());
  }
  if (rightJoinHelper_.has_value()) {
    // rightJoinHelper_.value()->putRowMatchIndexes(joiner->getProbeRowMatchIndexes());
  }

  // std::cout << name_ + "join end" << std::endl;
  // Return joined result
  return joiner->toTupleSet();
}

tl::expected<shared_ptr<fpdb::tuple::TupleSet>, string> HashJoinProbeKernel::readyToJoin(bool finalJoin) {
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
  uint64_t stTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif
  if (spawnOnRemote_ && rowLimit.load() <= 0) {
    return {};
  }

  if (!buildTupleSetIndex_.has_value() || buildTupleSetIndex_.value() == nullptr||buildTupleSetIndex_.value()->size() == 0) {
    std::cout << name_ + "Empty buildTupleSetIndex!" << std::endl;
    return {};
  }

  while (!probeTupleSet_.empty()) {
    if (spawnOnRemote_ && rowLimit.load() <= 0) {
      return {};
    }
    std::cout << name_ + "readyToJoin begin" << std::endl;
  auto expectedJoiner = RecordBatchHashJoiner::make(buildTupleSetIndex_.value(),
                                                    pred_.getRightColumnNames(),
                                                    outputSchema_.value(),
                                                    neededColumnIndice_,
                                                    0);
  if (!expectedJoiner.has_value()) {
    return tl::make_unexpected(expectedJoiner.error());
  }

    auto tupleSet = probeTupleSet_.begin();

    // Join
    auto expectedJoinedTupleSet = join(expectedJoiner.value(), *tupleSet, probeRowOffset_);
    probeRowOffset_ += (*tupleSet)->numRows();
    if (!expectedJoinedTupleSet.has_value())
      return tl::make_unexpected(expectedJoinedTupleSet.error());

    // Buffer join result
    auto bufferResult = buffer(expectedJoinedTupleSet.value());
    if (!bufferResult.has_value())
      return tl::make_unexpected(bufferResult.error());
    probePOp_->send(true);
    curNumRows_ -= (*tupleSet)->numRows();
    (*tupleSet)->clear();
    probeTupleSet_.erase(tupleSet);
  }

  while (!probePOp_->readAllFile()) {
    if (spawnOnRemote_ && rowLimit.load() <= 0) {
      return {};
    }
    auto expTupleSet = probePOp_->readTuple();

    auto expectedJoiner = RecordBatchHashJoiner::make(buildTupleSetIndex_.value(),
                                                pred_.getRightColumnNames(),
                                                outputSchema_.value(),
                                                neededColumnIndice_,
                                                0);
    if (!expectedJoiner.has_value()) {
      return tl::make_unexpected(expectedJoiner.error());
    }

    auto expectedJoinedTupleSet = join(expectedJoiner.value(), expTupleSet, probeRowOffset_);
    probeRowOffset_ += expTupleSet->numRows();
    if (!expectedJoinedTupleSet.has_value())
      return tl::make_unexpected(expectedJoinedTupleSet.error());

    auto bufferResult = buffer(expectedJoinedTupleSet.value());
    if (!bufferResult.has_value())
      return tl::make_unexpected(bufferResult.error());
    probePOp_->send(true);
    expTupleSet->clear();
  }
  // std::cout << name_ + "readyToJoin end" << std::endl;
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
  uint64_t enTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  exectime_+=enTime-stTime;
#endif
#if RUNTIME_SAMPLE_EN > 0
  probePOp_->sendSampleTime();
#endif
  return {};
}

tl::expected<void, string> HashJoinProbeKernel::joinBuildTupleSetIndex(const shared_ptr<TupleSetIndex> &tupleSetIndex, bool readyToRun) {
  // std::cout << name_ + "joinBuildTupleSetIndex begin" << std::endl;

  // Buffer tupleSetIndex
  auto putResult = putBuildTupleSetIndex(tupleSetIndex);
  if (!putResult)
    return tl::make_unexpected(putResult.error());

  // Create output schema and outer join helpers
  if (!probeTupleSet_.empty()) {
    bufferOutputSchema(tupleSetIndex, probeTupleSet_[0]);
    auto result = makeOuterJoinHelpers();
    if (!result.has_value()) {
      return result;
    }
  }

  // Check empty
  if (probeTupleSet_.empty() || tupleSetIndex->size() == 0) {
    // std::cout << name_ + "joinBuildTupleSetIndex end" << std::endl;
    return {};
  }

  if(readyToRun) {
    auto expectedJoinedTupleSet = readyToJoin(false);
    if (!expectedJoinedTupleSet.has_value())
      return tl::make_unexpected(expectedJoinedTupleSet.error());
  }
  // std::cout << name_ + "joinBuildTupleSetIndex end" << std::endl;
  return {};
}

tl::expected<void, string> HashJoinProbeKernel::joinProbeTupleSet(const shared_ptr<TupleSet> &tupleSet, bool readyToRun) {
  // std::cout << name_ + "joinProbeTupleSet begin" << std::endl;
  // Buffer tupleSet
  auto putResult = putProbeTupleSet(tupleSet);
  if (!putResult)
    return tl::make_unexpected(putResult.error());

  // Create output schema and outer join helpers
  if (buildTupleSetIndex_.has_value()) {
    bufferOutputSchema(buildTupleSetIndex_.value(), tupleSet);
    auto result = makeOuterJoinHelpers();
    if (!result.has_value()) {
      return result;
    }
  }

  // Check empty
  if (!buildTupleSetIndex_.has_value() || buildTupleSetIndex_.value()->size() == 0) {
    // std::cout << name_ + "joinProbeTupleSet end" << std::endl;
    return {};
  }

  if (readyToRun) {
    auto expectedJoinedTupleSet = readyToJoin(false);
    if (!expectedJoinedTupleSet.has_value())
    return tl::make_unexpected(expectedJoinedTupleSet.error());
  }
  // std::cout << name_ + "joinProbeTupleSet end" << std::endl;
  return {};
}

tl::expected<void, string> HashJoinProbeKernel::finalize() {
  auto expectedJoinedTupleSet = readyToJoin(true);
  if (!expectedJoinedTupleSet.has_value())
    return tl::make_unexpected(expectedJoinedTupleSet.error());
  // compute outer join
  auto result = computeOuterJoin();
  if (!result) {
    return result;
  }

  return {};
}

tl::expected<void, string> HashJoinProbeKernel::makeOuterJoinHelpers() {
  if (!isOuterJoinHelperCreated_) {
    if (!outputSchema_.has_value()) {
      return tl::make_unexpected("Output schema not set when making outer join helpers");
    }
    if (isLeft_ && !leftJoinHelper_.has_value()) {
      leftJoinHelper_ = OuterJoinHelper::make(true, outputSchema_.value(), neededColumnIndice_);
    }
    if (isRight_ && !rightJoinHelper_.has_value()) {
      rightJoinHelper_ = OuterJoinHelper::make(false, outputSchema_.value(), neededColumnIndice_);
    }
    isOuterJoinHelperCreated_ = true;
  }
  return {};
}

tl::expected<void, string> HashJoinProbeKernel::computeOuterJoin() {
  // left outer join
  // if (leftJoinHelper_.has_value() && buildTupleSetIndex_.has_value()) {
  //   const auto &expLeftOutput = leftJoinHelper_.value()->compute(buildTupleSetIndex_.value()->getTupleSet());
  //   if (!expLeftOutput.has_value()) {
  //     return tl::make_unexpected(expLeftOutput.error());
  //   }
  //   auto result = buffer(expLeftOutput.value());
  //   if (!result.has_value()) {
  //     return result;
  //   }
  // }

  // right outer join
  // if (rightJoinHelper_.has_value() && probeTupleSet_.has_value()) {
  //   const auto &expRightOutput = rightJoinHelper_.value()->compute(probeTupleSet_.value());
  //   if (!expRightOutput.has_value()) {
  //     return tl::make_unexpected(expRightOutput.error());
  //   }
  //   auto result = buffer(expRightOutput.value());
  //   if (!result.has_value()) {
  //     return result;
  //   }
  // }

  return {};
}

void HashJoinProbeKernel::clear() {
  leftJoinHelper_ = nullopt;
  rightJoinHelper_ = nullopt;
  HashJoinProbeAbstractKernel::clear();
}

}