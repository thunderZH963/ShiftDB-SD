//
// Created by Yifei Yang on 12/15/21.
//

#include <fpdb/executor/physical/join/hashjoin/HashJoinProbeAbstractKernel.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/executor/physical/Globals.h>
#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>
#include <arrow/io/api.h>
#include <arrow/csv/options.h> 
#include <arrow/result.h>
#include <arrow/status.h>
#include <memory>                      // for make_unique, unique_ptr, __sha...
#include <utility>
#include <filesystem>

namespace fpdb::executor::physical::join {

HashJoinProbeAbstractKernel::HashJoinProbeAbstractKernel(HashJoinPredicate pred, set<string> neededColumnNames, std::string name) :
        pred_(move(pred)), neededColumnNames_(move(neededColumnNames)), name_(move(name)) {}

tl::expected<void, string> HashJoinProbeAbstractKernel::putBuildTupleSetIndex(const shared_ptr<TupleSetIndex> &tupleSetIndex) {
  const auto &validateRes = validateColumnNames(tupleSetIndex->getTupleSet()->schema(), pred_.getLeftColumnNames());
  if (!validateRes.has_value()) {
    return tl::make_unexpected(fmt::format("Cannot put build tuple set index into probe kernel. {}", validateRes.error()));
  }

  if (!buildTupleSetIndex_.has_value()) {
    buildTupleSetIndex_ = tupleSetIndex;
    return {};
  } else if (tupleSetIndex->size() > 0) {
    return buildTupleSetIndex_.value()->merge(tupleSetIndex);
  } else {
    return {};
  }
}

tl::expected<void, string> HashJoinProbeAbstractKernel::putProbeTupleSet(const shared_ptr<TupleSet> &tupleSet) {
  // std::cout << name_ + "putProbeTupleSet begin" << std::endl;
  if (tupleSet&&tupleSet->numRows() > 0) {
  const auto &validateRes = validateColumnNames(tupleSet->schema(), pred_.getRightColumnNames());
  if (!validateRes.has_value()) {
    return tl::make_unexpected(fmt::format("Cannot put probe tuple set into probe kernel. {}", validateRes.error()));
  }
    if (tupleSet->numRows() + curNumRows_ <= DefaultFileNum * DefaultBufferSize) {
      curNumRows_ += tupleSet->numRows();
      probeTupleSet_.emplace_back(tupleSet);
    } else {
      probePOp_->dumpToFile(tupleSet);
    }
    return {}; 
  } else {
    // std::cout << name_ + "putProbeTupleSet end" << std::endl;
    return {};
  }
}

tl::expected<void, string> HashJoinProbeAbstractKernel::buffer(const shared_ptr<TupleSet> &tupleSet) {
  std::cout << name_ + "HashJoinProbeAbstractKernel::buffer,row="<< tupleSet->numRows()<< std::endl;
  if(tupleSet->numRows()==0){
    return {};
  }
  if (!buffer_.has_value()) {
    buffer_ = tupleSet;
  }
  else {
    const auto &bufferedTupleSet = buffer_.value();
    const auto &concatenateResult = TupleSet::concatenate({bufferedTupleSet, tupleSet});
    if (!concatenateResult)
      return tl::make_unexpected(concatenateResult.error());

    buffer_ = concatenateResult.value();
  }

  return {};
}

void HashJoinProbeAbstractKernel::bufferOutputSchema(const shared_ptr<TupleSetIndex> &tupleSetIndex,
                                                     const shared_ptr<TupleSet> &tupleSet) {
  if (!outputSchema_.has_value()) {

    // Create the outputSchema_ and neededColumnIndice_ from neededColumnNames_
    vector<shared_ptr<::arrow::Field>> outputFields;

    for (int c = 0; c < tupleSetIndex->getTupleSet()->schema()->num_fields(); ++c) {
      auto field = tupleSetIndex->getTupleSet()->schema()->field(c);
      if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
        neededColumnIndice_.emplace_back(make_shared<pair<bool, int>>(true, c));
        outputFields.emplace_back(field);
      }
    }
    for (int c = 0; c < tupleSet->schema()->num_fields(); ++c) {
      auto field = tupleSet->schema()->field(c);
      if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
        neededColumnIndice_.emplace_back(make_shared<pair<bool, int>>(false, c));
        outputFields.emplace_back(field);
      }
    }
    outputSchema_ = make_shared<::arrow::Schema>(outputFields);
  }
}

void HashJoinProbeAbstractKernel::setName(const std::string name) {
  name_ = name;
}

const std::optional<shared_ptr<::arrow::Schema>> &HashJoinProbeAbstractKernel::getOutputSchema() const {
  return outputSchema_;
}

const std::optional<shared_ptr<fpdb::tuple::TupleSet>> &HashJoinProbeAbstractKernel::getBuffer() const {
  return buffer_;
}

void HashJoinProbeAbstractKernel::setOp(std::shared_ptr<PhysicalOp>& op) {
  probePOp_ = op;
  spawnOnRemote_ = op->spawnOnRemote();
}

void HashJoinProbeAbstractKernel::clear() {
  buildTupleSetIndex_ = nullopt;
  for (auto tupleSet : probeTupleSet_) {
    tupleSet->clear();
  }
  while (!probeTupleSet_.empty()) {
    probeTupleSet_.pop_back();
  }
  curNumRows_ = 0;
  probePOp_ = nullptr;
  clearBuffer();
}

void HashJoinProbeAbstractKernel::clearBuffer() {
  buffer_ = nullopt;
}

tl::expected<void, string> HashJoinProbeAbstractKernel::validateColumnNames(const shared_ptr<arrow::Schema> &schema,
                                                                            const vector<string> &columnNames) {
  for (const auto &columnName: columnNames) {
    if(schema->GetFieldIndex(columnName) == -1) {
      return tl::make_unexpected(fmt::format("Column '{}' does not exist.", columnName));
    }
  }

  return {};
}

}
