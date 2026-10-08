//
// Created by matt on 14/4/20.
//

#include <fpdb/executor/physical/project/ProjectPOp.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/expression/gandiva/Projector.h>
#include <fpdb/expression/gandiva/Expression.h>
#include <utility>

namespace fpdb::executor::physical::project {

ProjectPOp::ProjectPOp(std::string name,
                 std::vector<std::string> projectColumnNames,
                 int nodeId,
                 std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>> exprs,
                 std::vector<std::string> exprNames,
                 std::vector<std::pair<std::string, std::string>> projectColumnNamePairs):
  PhysicalOp(std::move(name), PROJECT, std::move(projectColumnNames), nodeId),
  exprs_(std::move(exprs)),
  exprNames_(std::move(exprNames)),
  projectColumnNamePairs_(std::move(projectColumnNamePairs)) {}

std::string ProjectPOp::getTypeString() const {
  return "ProjectPOp";
}

void ProjectPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void ProjectPOp::onReceive(const Envelope &message) {
  if (message.message().type() == MessageType::START) {
    this->onStart();
  } else if (message.message().type() == MessageType::TUPLE) {
    auto tupleMessage = dynamic_cast<const TupleMessage &>(message.message());
    this->onTuple(tupleMessage);
  } else if (message.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else if (message.message().type() == MessageType::DEBUG) {
    this->onDebug();
  } else if (message.message().type() == MessageType::PREDICATE) {
    auto predicateMessage = dynamic_cast<const PredicateMessage &>(message.message());
    this->onPredicate(predicateMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + message.message().getTypeString());
  }
}

void ProjectPOp::onPredicate(const PredicateMessage &message) {
  //TODO: 还未完成
}
 
void ProjectPOp::onTuple(const TupleMessage &message) {
  receiveTupleSet = true;
  // Build the project if not built
  buildProjector(message);

  auto tuple = message.tuples();
#if OP_INFO>0
  input_row+=tuple->numRows();
	input_size+=tuple->size();
  if(input_row_first==0){
    input_row_first=tuple->numRows();
  }
#endif
  if (!shouldSpawnOnRemote_ || rowLimit.load() > 0) {
    // Add the tuples to the internal buffer
    bufferTuples(tuple);

    // Project and send if the buffer is full enough
    projectAndSendTuples();
  } else {
    dumpToFile(tuple);
  }
}

void ProjectPOp::onComplete(const CompleteMessage &) {
  if(recvAllCompleteMsg()){
    if (!receiveTupleSet) {
      ctx()->notifyComplete();appendOpTime();
    } else if (readAllFile()) {
      // Project and send any remaining tuples
      projectAndSendTuples();
      ctx()->notifyComplete();appendOpTime();
    } else {
      rowLimit_mtx.lock();
      vec.push_back(this->ctx()->operatorActor());
      rowLimit_mtx.unlock();
    }
  }
}

void ProjectPOp::onDebug() {
  while (!readAllFile()) {
    if (rowLimit.load() <= 0) {
        rowLimit_mtx.lock();
        vec.push_back(this->ctx()->operatorActor());
        rowLimit_mtx.unlock();
        return;
    }
    auto expTupleSet = readTuple();
    bufferTuples(expTupleSet);

    // Project and send if the buffer is full enough
    projectAndSendTuples();
  }
  //No more file in disk
  projectAndSendTuples();
  ctx()->notifyComplete();appendOpTime();
}

void ProjectPOp::buildProjector(const TupleMessage &message) {
  if(!projector_.has_value() && !exprs_.empty()){
    const auto &inputSchema = message.tuples()->table()->schema();
    projector_ = std::make_shared<fpdb::expression::gandiva::Projector>(exprs_);
    projector_.value()->compile(inputSchema);
  }
}

void ProjectPOp::bufferTuples(const std::shared_ptr<TupleSet> &tuple) {
  if (!tuples_) {
    // Initialise tuples buffer with message contents
    tuples_ = tuple;
  } else {
    // Append message contents to tuples buffer
    auto tables = {tuples_->table(), tuple->table()};
    auto expTable = arrow::ConcatenateTables(tables);
    if (!expTable.ok()) {
      ctx()->notifyError(expTable.status().message());
    }
    tuples_->table(*expTable);
  }
}

void ProjectPOp::projectAndSendTuples() {
  if (tuples_) {
    // Collect project columns and project expr columns
    std::vector<std::shared_ptr<Column>> columns;

    // Project columns
    for (const auto &projectColumnPair: projectColumnNamePairs_) {
      const auto &inputColumnName = projectColumnPair.first;
      const auto &outputColumnName = projectColumnPair.second;
      const auto &expColumn = tuples_->getColumnByName(inputColumnName);
      if (!expColumn.has_value()) {
        ctx()->notifyError(expColumn.error());
      }
      const auto &column = expColumn.value();
      column->setName(outputColumnName);
      columns.emplace_back(column);
    }

    // Columns from project exprs
    if (!exprs_.empty()) {
      // Evaluate
      const auto &expProjExprTuples = projector_.value()->evaluate(*tuples_);
      if (!expProjExprTuples.has_value()) {
        ctx()->notifyError(expProjExprTuples.error());
      }
      const auto &projExprTuples = *expProjExprTuples;

      // Rename
      auto renameRes = projExprTuples->renameColumns(exprNames_);
      if (!renameRes.has_value()) {
        ctx()->notifyError(renameRes.error());
      }

      // Add columns
      for (int c = 0; c < projExprTuples->numColumns(); ++c) {
        const auto &expColumn = projExprTuples->getColumnByIndex(c);
        if (!expColumn.has_value()) {
          ctx()->notifyError(expColumn.error());
        }
        columns.emplace_back(expColumn.value());
      }
    }

    // Make output tupleSet
    std::shared_ptr<TupleSet> fullTupleSet = TupleSet::make(columns);

    // Send, here no need to project the fullTupleSet using projectColumnNames as it won't have redundant columns
    sendTuples(fullTupleSet);
    tuples_ = nullptr;
  }
}

void ProjectPOp::sendTuples(std::shared_ptr<TupleSet> &projected) {
	std::shared_ptr<Message> tupleMessage = std::make_shared<TupleMessage>(projected, name());
	ctx()->tell(tupleMessage, nextOpLocation_);
}

void ProjectPOp::clear() {
  tuples_.reset();
}

}
