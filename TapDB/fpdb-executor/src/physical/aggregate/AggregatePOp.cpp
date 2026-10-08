//
// Created by matt on 11/12/19.
//

#include <fpdb/executor/physical/aggregate/AggregatePOp.h>
#include <fpdb/executor/physical/aggregate/AggregateResult.h>
#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/executor/message/Message.h>
#include <arrow/scalar.h>
#include <string>
#include <utility>
#include <memory>

namespace fpdb::executor::physical::aggregate {

AggregatePOp::AggregatePOp(string name,
                           vector<string> projectColumnNames,
                           int nodeId,
                           vector<shared_ptr<aggregate::AggregateFunction>> functions):
  PhysicalOp(move(name), AGGREGATE, move(projectColumnNames), nodeId),
  functions_(move(functions)) {

  // initialize aggregate results
  for (uint i = 0; i < functions_.size(); ++i) {
    aggregateResults_.emplace_back(vector<shared_ptr<AggregateResult>>{});
  }
}

std::string AggregatePOp::getTypeString() const {
  return "AggregatePOp";
}

void AggregatePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void AggregatePOp::onReceive(const Envelope &message) {
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

void AggregatePOp::onTuple(const TupleMessage &message) {
#if OP_INFO>0
  	if(op_start_time_==0){
		op_start_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
	}
#endif
  receiveTupleSet = true;
  auto tuple = message.tuples();
#if OP_INFO>0
  input_row+=tuple->numRows();
	input_size+=tuple->size();
  if(input_row_first==0){
    input_row_first=tuple->numRows();
  }
#endif
  if (!shouldSpawnOnRemote_ || rowLimit.load() > 0) {
    compute(tuple);
  } else {
    dumpToFile(tuple);
  }
}

void AggregatePOp::onDebug() {
  while (!readAllFile()) {
    if (rowLimit.load() <= 0) {
        rowLimit_mtx.lock();
        vec.push_back(this->ctx()->operatorActor());
        rowLimit_mtx.unlock();
        return;
    }
    auto expTupleSet = readTuple();
    compute(expTupleSet);
  }
  //No more file in disk. Finalize
  shared_ptr<TupleSet> tupleSet;
  if (hasResult()) {
    tupleSet = finalize();
  } else {
    tupleSet = finalizeEmpty();
  }
#if RUNTIME_SAMPLE_EN>0
  if(receivedTupleSet>=2){
      uint64_t numRows = tupleSet->numRows();
      uint64_t size = tupleSet->size();
      if (link2Pred&&!recvFromPred) {
        std::string target = "";
        for (auto consumer : consumers_) {
          if (consumer.find("[0]") != std::string::npos) {
            target = consumer;
            break;
          }
        }
        if (!recvFromPred && !sendToPred && numRows > 0&&iflock()) {
          auto msg = std::make_shared<PredicateMessage>(name(), target, nodeId_, numRows * consumerFactor_, size * consumerFactor_,consumerFactor_);
          ctx()->producer2predictor(msg);
          sendToPred = true;
        }
         if(!sendSampleTuple){//切一块数据作为采样数据
            std::cout<<name()<<" send sample 2 "<<std::endl;
            sendSampleTuple=true;

            std::unordered_map<std::string, std::string> pathConfig = readConfig("filescan.conf");
            uint16_t PROPORTION=std::stoi(pathConfig["PROPORTION"]);
            double sample_ratio = (1.0*PROPORTION)/100;
            uint64_t splitRows = (uint64_t)(sample_ratio * numRows);
            auto sampleTupleSet=tupleSet->slice(0, splitRows);
            tupleSet=tupleSet->slice(splitRows,numRows-splitRows);
            // send sample tuple to -1 & 0
            auto nodeSet=getNodeSet();
            for (const int& element : nodeSet) {	
              auto tmpTuple=sampleTupleSet->slice(0, sampleTupleSet->numRows());
			        std::shared_ptr<Message> sampleTupleMessage = std::make_shared<TupleMessage>(tmpTuple, this->name());	
              ctx()->tell(sampleTupleMessage, element);
            }
        }
      }
      if (link2Pred && !recvFromPred) {
        std::cout<<name()<<" save "<<std::endl;
        saveTuple(tupleSet);
      } else {
        std::cout<<name()<<" send "<<std::endl;
        shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(tupleSet, this->name());
        ctx()->tell(tupleMessage, nextOpLocation_);
      }
  }
#elif PREDICATE_EN > 0
  if (link2Pred) {
    uint64_t numRows = tupleSet->numRows();
    uint64_t size = tupleSet->size();
    std::string target = "";
    for (auto consumer : consumers_) {
      if (consumer.find("[0]") != std::string::npos) {
        target = consumer;
        break;
      }
    }
    if (!recvFromPred && !sendToPred && numRows > 0) {
      auto msg = std::make_shared<PredicateMessage>(name(), target, nodeId_, numRows * consumerFactor_, size * consumerFactor_,consumerFactor_);
      ctx()->producer2predictor(msg);
      sendToPred = true;
    }
  }
  if (link2Pred && !recvFromPred) {
    saveTuple(tupleSet);
  } else {
      shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(tupleSet, this->name());
      ctx()->tell(tupleMessage, nextOpLocation_);
  }
#else
  shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(tupleSet, this->name());
  ctx()->tell(tupleMessage, nextOpLocation_);
#endif
#if RUNTIME_SAMPLE_EN > 0
      if(Pred2link&&(receivedTupleSet<2)){
        dropSavedTuple();
      }
#endif
  if (recvAllCompleteMsg() && readAllFile() && readAllSavedTuple()) {
#if OP_INFO>0
    if(op_stop_time_==0){
			op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
		}
#endif
    ctx()->notifyComplete();appendOpTime();
  }
}

void AggregatePOp::onPredicate(const PredicateMessage &message) {
	recvFromPred = true;
	std::cout << name_ + " receive Predicate Msg from " + message.sender() + ", location: " + std::to_string(message.getLocation()) << std::endl;
  nextOpLocation_ = message.getLocation();
  sendSavedTuple();
#if RUNTIME_SAMPLE_EN > 0
    if(Pred2link&&(receivedTupleSet<2)){
      dropSavedTuple();
    }
#endif
  if (recvAllCompleteMsg() && readAllFile() && readAllSavedTuple()) {
#if OP_INFO>0
    if(op_stop_time_==0){
			op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
		}
#endif
    ctx()->notifyComplete();appendOpTime();
  }
}

void AggregatePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() &&
      this->ctx()->operatorMap().allComplete(fpdb::executor::physical::POpRelationshipType::Producer)) {

    if (!receiveTupleSet) {
#if OP_INFO>0
      if(op_stop_time_==0){
			op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
		}
#endif
      ctx()->notifyComplete();appendOpTime();
    } else if (readAllFile()) {
      // Finalize
      shared_ptr<TupleSet> tupleSet;
      if (hasResult()) {
        tupleSet = finalize();
      } else {
        tupleSet = finalizeEmpty();
      }
#if RUNTIME_SAMPLE_EN > 0
    if(receivedTupleSet>=2){
      uint64_t numRows = tupleSet->numRows();
      uint64_t size = tupleSet->size();
      if (link2Pred&&!recvFromPred) {
        std::string target = "";
        for (auto consumer : consumers_) {
          if (consumer.find("[0]") != std::string::npos) {
            target = consumer;
            break;
          }
        }
        if (!recvFromPred && !sendToPred && numRows > 0&&iflock()) {
          auto msg = std::make_shared<PredicateMessage>(name(), target, nodeId_, numRows * consumerFactor_, size * consumerFactor_,consumerFactor_);
          ctx()->producer2predictor(msg);
          sendToPred = true;
        }
         if(!sendSampleTuple){//切一块数据作为采样数据
            std::cout<<name()<<" send sample 2 "<<std::endl;
            sendSampleTuple=true;

            std::unordered_map<std::string, std::string> pathConfig = readConfig("filescan.conf");
            uint16_t PROPORTION=std::stoi(pathConfig["PROPORTION"]);
            double sample_ratio = (1.0*PROPORTION)/100;
            uint64_t splitRows = (uint64_t)(sample_ratio * numRows);
            auto sampleTupleSet=tupleSet->slice(0, splitRows);
            tupleSet=tupleSet->slice(splitRows,numRows-splitRows);
            // send sample tuple to -1 & 0
            auto nodeSet=getNodeSet();
            for (const int& element : nodeSet) {	
              auto tmpTuple=sampleTupleSet->slice(0, sampleTupleSet->numRows());
			        std::shared_ptr<Message> sampleTupleMessage = std::make_shared<TupleMessage>(tmpTuple, this->name());		
              ctx()->tell(sampleTupleMessage, element);
            }
        }
      }
      if (link2Pred && !recvFromPred) {
        std::cout<<name()<<" save "<<std::endl;
        saveTuple(tupleSet);
      } else {
        std::cout<<name()<<" send "<<std::endl;
        shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(tupleSet, this->name());
        ctx()->tell(tupleMessage, nextOpLocation_);
      }
    }
#elif PREDICATE_EN > 0
      if (link2Pred) {
        uint64_t numRows = tupleSet->numRows();
        uint64_t size = tupleSet->size();
        std::string target = "";
        for (auto consumer : consumers_) {
          if (consumer.find("[0]") != std::string::npos) {
            target = consumer;
            break;
          }
        }
        if (!recvFromPred && !sendToPred && numRows > 0) {
          auto msg = std::make_shared<PredicateMessage>(name(), target, nodeId_, numRows * consumerFactor_, size * consumerFactor_,consumerFactor_);
          ctx()->producer2predictor(msg);
          sendToPred = true;
        }
      }
      if (link2Pred && !recvFromPred) {
        saveTuple(tupleSet);
      } else {
        shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(tupleSet, this->name());
        ctx()->tell(tupleMessage, nextOpLocation_);
      }
#else
      shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(tupleSet, this->name());
      ctx()->tell(tupleMessage, nextOpLocation_);
#endif
#if RUNTIME_SAMPLE_EN > 0
      if(Pred2link&&(receivedTupleSet<2)){
        dropSavedTuple();
        tupleSet->clear();
      }
#endif
      if (recvAllCompleteMsg() && readAllFile() && readAllSavedTuple()) {
#if OP_INFO>0
        if(op_stop_time_==0){
          op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        }
#endif
        ctx()->notifyComplete();appendOpTime();
      }
    } else {
      rowLimit_mtx.lock();
      vec.push_back(this->ctx()->operatorActor());
      rowLimit_mtx.unlock();
    }
  }
}

void AggregatePOp::compute(const shared_ptr<TupleSet> &tupleSet) {
  if(tupleSet->numRows()>0){
  // compute and save aggregate results
  for (uint i = 0; i < functions_.size(); ++i) {
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
    uint64_t stTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif
    const auto &expAggregateResult = functions_[i]->computePartial(tupleSet);
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
    uint64_t enTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    exectime_+=enTime-stTime;
#endif
    if (!expAggregateResult.has_value()) {
      ctx()->notifyError(expAggregateResult.error());
    }
    aggregateResults_[i].emplace_back(expAggregateResult.value());
  }
  }
#if RUNTIME_SAMPLE_EN > 0
  std::cout<<name()<<" AggregatePOp::compute, receivedTupleSet="<<receivedTupleSet<<",Pred2link="<<Pred2link<<",link2Pred="<<link2Pred<<std::endl;
  if(receivedTupleSet==1&&!sendSampleTimeToPred&&Pred2link){
    sendSampleTimeToPred=true;
    std::cout<<name()<<" consumer2predictor "<<std::endl;
    auto msg = std::make_shared<PredicateMessage>(name(),true,exectime_,nodeId_);
    ctx()->consumer2predictor(msg);
  }
#endif 
}

shared_ptr<TupleSet> AggregatePOp::finalize() {
  
  // Create output schema
  vector<shared_ptr<arrow::Field>> fields;
  for (const auto &function: functions_) {
    shared_ptr<arrow::Field> field = arrow::field(function->getOutputColumnName(), function->returnType());
    fields.emplace_back(field);
  }
  const auto &schema = arrow::schema(fields);

  // Create output tuple
  vector<shared_ptr<arrow::Array>> columns;
  for (uint i = 0; i < functions_.size(); ++i) {
    const auto &function = functions_[i];
    // Finalize
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
    uint64_t stTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif
    const auto &expFinalResult = function->finalize(aggregateResults_[i]);
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
    uint64_t enTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    exectime_+=enTime-stTime;
#endif
    if (!expFinalResult.has_value()) {
      ctx()->notifyError(expFinalResult.error());
    }

    // Make the column of the final result
    const auto &finalResult = expFinalResult.value();
    if (function->returnType() == arrow::int32()) {
      auto colArgh = makeArgh<arrow::Int32Type>(static_pointer_cast<arrow::Int32Scalar>(finalResult));
      columns.emplace_back(colArgh.value());
    } else if (function->returnType() == arrow::int64()) {
      auto colArgh = makeArgh<arrow::Int64Type>(static_pointer_cast<arrow::Int64Scalar>(finalResult));
      columns.emplace_back(colArgh.value());
    } else if (function->returnType() == arrow::float64()) {
      auto colArgh = makeArgh<arrow::DoubleType>(static_pointer_cast<arrow::DoubleScalar>(finalResult));
      columns.emplace_back(colArgh.value());
    } else {
      ctx()->notifyError("Unsupported aggregate output field type " + function->returnType()->name());
    }
  }

  // Make tupleSet
  const auto &table = arrow::Table::Make(schema, columns);
  const shared_ptr<TupleSet> &aggregatedTuples = TupleSet::make(table);

  // Project using projectColumnNames
  auto expProjectTupleSet = aggregatedTuples->projectExist(getProjectColumnNames());
  if (!expProjectTupleSet) {
    ctx()->notifyError(expProjectTupleSet.error());
  }
  
  SPDLOG_DEBUG("Completing  |  Aggregation result: \n{}", aggregatedTuples->toString());
  return expProjectTupleSet.value();
}

shared_ptr<TupleSet> AggregatePOp::finalizeEmpty() {
  arrow::FieldVector fields;
  for (const auto &function: functions_) {
    fields.emplace_back(make_shared<arrow::Field>(function->getOutputColumnName(), function->returnType()));
  }
  auto outputSchema = arrow::schema(fields);
  return TupleSet::make(outputSchema);
}

bool AggregatePOp::hasResult() {
  return !aggregateResults_[0].empty();
}

void AggregatePOp::clear() {
  aggregateResults_.clear();
}

}