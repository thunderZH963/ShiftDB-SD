//
// Created by matt on 5/12/19.
//

#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <arrow/table.h>               // for ConcatenateTables, Table (ptr ...
#include <arrow/pretty_print.h>
#include <vector>                      // for vector

namespace fpdb::executor::physical::collate {

CollatePOp::CollatePOp(std::string name,
                       std::vector<std::string> projectColumnNames,
                       int nodeId) :
  PhysicalOp(std::move(name), COLLATE, std::move(projectColumnNames), nodeId) {
}

std::string CollatePOp::getTypeString() const {
  return "CollatePOp";
}

void CollatePOp::onReceive(const fpdb::executor::message::Envelope &message) {
  if (message.message().type() == MessageType::START) {
    this->onStart();
  } else if (message.message().type() == MessageType::TUPLE) {
    auto tupleMessage = dynamic_cast<const fpdb::executor::message::TupleMessage &>(message.message());
    this->onTuple(tupleMessage);
  } else if (message.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const fpdb::executor::message::CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + message.message().getTypeString());
  }
}

void CollatePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void CollatePOp::onComplete(const fpdb::executor::message::CompleteMessage &) {
  if(recvAllCompleteMsg()){
    if (!tables_.empty()) {
      tables_.push_back(tuples_->table());
      const arrow::Result<std::shared_ptr<arrow::Table>> &res = arrow::ConcatenateTables(tables_);
      if (!res.ok()) {
        ctx()->notifyError(res.status().message());
      }
      tuples_->table(*res);
      tables_.clear();
    }

    // make the order of output columns the same as the query specifies
    if (tuples_ && tuples_->valid()) {
      const auto &expTupleSet = tuples_->projectExist(getProjectColumnNames());
      if (!expTupleSet.has_value()) {
        ctx()->notifyError(expTupleSet.error());
      }
      tuples_ = expTupleSet.value();
    } else {
      tuples_ = TupleSet::makeWithEmptyTable();
    }
#if (OP_INFO>0) || (PREDICATE_EN>0)
    std::cout<<"CollatePOp result"<<std::endl;
    std::cout<<tuples_->toString()<<std::endl;
    std::cout<<tuples_->showString()<<std::endl;
    std::string file_path = "/home/huazhang/Work/FPDB-build-4242/cardinality_log.txt";
    std::ofstream output_file(file_path, std::ios::app);
    if (!output_file.is_open()) {
        std::cerr << "Failed to open file for writing." << std::endl;
        return;
    }
    if (output_file.tellp() == 0) {
        output_file << "target OP,tupleSize,tupleRows,Factor,producer Location,hashSize,hashRows" << std::endl;
    }
    for(auto str:cardinality_log){
       output_file<<str<<std::endl;
    }
    output_file.close();
#endif
	  ctx()->notifyComplete();
  }
}

void CollatePOp::onTuple(const fpdb::executor::message::TupleMessage &message) {
  if (!tuples_) {
    assert(message.tuples());
    tuples_ = message.tuples();
  } else {
    tables_.push_back(message.tuples()->table());
    if (tables_.size() > tablesCutoff_) {
      tables_.push_back(tuples_->table());
      const arrow::Result<std::shared_ptr<arrow::Table>> &res = arrow::ConcatenateTables(tables_);
      if (!res.ok()) {
        ctx()->notifyError(res.status().message());
      }
      tuples_->table(*res);
      tables_.clear();
    }
  }
}

std::shared_ptr<TupleSet> CollatePOp::tuples() {
  assert(tuples_);
  return tuples_;
}

void CollatePOp::clear() {
  tuples_.reset();
  tables_.clear();
}

}