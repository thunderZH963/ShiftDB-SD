//
// Created by Yifei Yang on 12/15/21.
//

#include <fpdb/executor/physical/join/hashjoin/HashSemiJoinProbeKernel.h>
#include <fpdb/executor/physical/join/hashjoin/RecordBatchHashSemiJoiner.h>
#include <arrow/api.h>
#include <utility>

namespace fpdb::executor::physical::join {

HashSemiJoinProbeKernel::HashSemiJoinProbeKernel(HashJoinPredicate pred, set<string> neededColumnNames, std::string name) :
  HashJoinProbeAbstractKernel(move(pred), move(neededColumnNames), move(name)) {}

shared_ptr<HashSemiJoinProbeKernel> HashSemiJoinProbeKernel::make(HashJoinPredicate pred, set<string> neededColumnNames, std::string name) {
  return make_shared<HashSemiJoinProbeKernel>(move(pred), move(neededColumnNames), move(name));
}

bool HashSemiJoinProbeKernel::isSemi() const {
  return true;
}

tl::expected<unordered_set<int64_t>, string>
join(const shared_ptr<RecordBatchHashSemiJoiner> &joiner, const shared_ptr<TupleSet> &tupleSet) {
  unordered_set<int64_t> joinResult;
  ::arrow::Result<shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;

  // Read the table a batch at a time
  auto probeTable = tupleSet->table();
  ::arrow::TableBatchReader reader{*probeTable};
  reader.set_chunksize((int64_t) DefaultChunkSize);

  // Read a batch
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
    return tl::make_unexpected(recordBatchResult.status().message());
  }
  auto recordBatch = *recordBatchResult;

  while (recordBatch) {
    // Join
    auto result = joiner->join(recordBatch);
    if (!result.has_value()) {
      return tl::make_unexpected(result.error());
    }

    // Read a batch
    recordBatchResult = reader.Next();
    if (!recordBatchResult.ok()) {
      return tl::make_unexpected(recordBatchResult.status().message());
    }
    recordBatch = *recordBatchResult;
  }

  // Return joined result
  return joiner->getRowMatchIndexes();
}

tl::expected<unordered_set<int64_t>, string> HashSemiJoinProbeKernel::readyToJoin(bool finalJoin) {
  if (spawnOnRemote_ && rowLimit.load() <= 0) {
    return {};
  }

  if (!buildTupleSetIndex_.has_value() || buildTupleSetIndex_.value() == nullptr) {
    std::cout << name_ + "Empty buildTupleSetIndex!" << std::endl;
    return {};
  }

  while (!probeTupleSet_.empty()) {
    if (spawnOnRemote_ && rowLimit.load() <= 0) {
      return {};
    }

    const auto &joiner = RecordBatchHashSemiJoiner::make(buildTupleSetIndex_.value(),
                                                        pred_.getRightColumnNames(),
                                                        0);
    auto tupleSet = probeTupleSet_.begin();
    
    //Join
    const auto &expJoinResult = join(joiner, *tupleSet);
    auto &joinResult = expJoinResult.value();
    // Buffer join result
    rowMatchIndexes_.insert(joinResult.begin(), joinResult.end());

    curNumRows_ -= (*tupleSet)->numRows();
    (*tupleSet)->clear();
    probeTupleSet_.erase(tupleSet);
  }

  while (!probePOp_->readAllFile()) {
    if (spawnOnRemote_ && rowLimit.load() <= 0) {
      return {};
    }
    auto expTupleSet = probePOp_->readTuple();

    const auto &joiner = RecordBatchHashSemiJoiner::make(buildTupleSetIndex_.value(),
                                                        pred_.getRightColumnNames(),
                                                        0);
    
    //Join
    const auto &expJoinResult = join(joiner, expTupleSet);
    auto &joinResult = expJoinResult.value();
    // Buffer join result
    rowMatchIndexes_.insert(joinResult.begin(), joinResult.end());
    expTupleSet->clear();
  }
  return {};
}

tl::expected<void, string> HashSemiJoinProbeKernel::joinBuildTupleSetIndex(const shared_ptr<TupleSetIndex> &tupleSetIndex, bool readyToRun) {
  // Buffer tupleSetIndex
  auto putResult = putBuildTupleSetIndex(tupleSetIndex);
  if (!putResult)
    return tl::make_unexpected(putResult.error());

  // Create output schema
  if (!probeTupleSet_.empty()) {
    bufferOutputSchema(tupleSetIndex, probeTupleSet_[0]);
  }

  // Check empty
  if (probeTupleSet_.empty() || tupleSetIndex->size() == 0) {
    return {};
  }

  // Join
  if (readyToRun) {
    auto expectedJoinedTupleSet = readyToJoin(false);
    if (!expectedJoinedTupleSet.has_value()) {
      return tl::make_unexpected(expectedJoinedTupleSet.error());
    }
  }
  return {};
}

tl::expected<void, string> HashSemiJoinProbeKernel::joinProbeTupleSet(const shared_ptr<TupleSet> &tupleSet, bool readyToRun) {
  // Buffer tupleSet
  auto putResult = putProbeTupleSet(tupleSet);
  if (!putResult)
    return tl::make_unexpected(putResult.error());

  // Create output schema
  if (buildTupleSetIndex_.has_value()) {
    bufferOutputSchema(buildTupleSetIndex_.value(), tupleSet);
  }

  // Check empty
  if (!buildTupleSetIndex_.has_value() || buildTupleSetIndex_.value()->size() == 0) {
    return {};
  }

  if (readyToRun) {
    auto expectedJoinedTupleSet = readyToJoin(false);
    if (!expectedJoinedTupleSet.has_value())
    return tl::make_unexpected(expectedJoinedTupleSet.error());
  }
  return {};
}

tl::expected<void, string> HashSemiJoinProbeKernel::finalize() {
  // Check if has input rows
  if (!buildTupleSetIndex_.has_value() || buildTupleSetIndex_.value()->size() == 0) {
    return {};
  }

  auto expectedJoinedTupleSet = readyToJoin(true);
  if (!expectedJoinedTupleSet.has_value())
    return tl::make_unexpected(expectedJoinedTupleSet.error());

  // Make input as a record batch, project only neededColumns of tupleSetIndex
  buildTupleSetIndex_.value()->combine();
  const auto &inputTable = buildTupleSetIndex_.value()->getTupleSet()->table();
  arrow::ArrayVector arrayVector;
  for (const auto &index: neededColumnIndice_) {
    if (index->first) {
      arrayVector.emplace_back(inputTable->column(index->second)->chunk(0));
    }
  }

  if (!outputSchema_.has_value()) {
    return tl::make_unexpected("Output schema not set yet during finalize");
  }
  const auto &recordBatch = arrow::RecordBatch::Make(outputSchema_.value(),
                                                     inputTable->num_rows(),
                                                     arrayVector);

  // Filter to get matched rows
  const auto &expSelectionVector = makeSelectionVector(rowMatchIndexes_);
  if (!expSelectionVector.has_value()) {
    return tl::make_unexpected(expSelectionVector.error());
  }
  const auto &selectionVector = expSelectionVector.value();
  const auto &expFilteredArrayVector =
          fpdb::expression::gandiva::Filter::evaluateBySelectionVectorStatic(*recordBatch,
                                                                               selectionVector);
  if (!expFilteredArrayVector.has_value()) {
    return tl::make_unexpected(expFilteredArrayVector.error());
  }

  // Buffer
  auto result = buffer(TupleSet::make(outputSchema_.value(), *expFilteredArrayVector));
  if (!result.has_value()) {
    return result;
  }

  return {};
}

tl::expected<shared_ptr<::gandiva::SelectionVector>, string>
HashSemiJoinProbeKernel::makeSelectionVector(const unordered_set<int64_t> &rowMatchIndexes) {
  // Create a selection vector
  shared_ptr<::gandiva::SelectionVector> selectionVector;
  auto status = ::gandiva::SelectionVector::MakeInt64((int64_t) rowMatchIndexes.size(),
                                                      ::arrow::default_memory_pool(),
                                                      &selectionVector);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }
  selectionVector->SetNumSlots((int64_t) rowMatchIndexes.size());

  // Sort rowMatchIndexes_
  vector<int64_t> rowMatchIndexesSorted(rowMatchIndexes.begin(), rowMatchIndexes.end());
  std::sort(rowMatchIndexesSorted.begin(), rowMatchIndexesSorted.end());

  // Set values for the selection vector
  for (uint i = 0; i < rowMatchIndexesSorted.size(); ++i) {
    selectionVector->SetIndex(i, rowMatchIndexesSorted[i]);
  }

  return selectionVector;
}

void HashSemiJoinProbeKernel::clear() {
  rowMatchIndexes_.clear();
  HashJoinProbeAbstractKernel::clear();
}

}