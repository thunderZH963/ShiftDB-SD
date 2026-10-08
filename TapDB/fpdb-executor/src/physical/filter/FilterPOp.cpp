//
// Created by matt on 6/5/20.
//

#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/cache/WeightRequestMessage.h>
#include <fpdb/expression/gandiva/Filter.h>
#include <fpdb/expression/gandiva/BinaryExpression.h>
#include <fpdb/tuple/Globals.h>
#include <utility>

using namespace fpdb::executor::physical::filter;
using namespace fpdb::cache;
using namespace fpdb::expression::gandiva;

FilterPOp::FilterPOp(std::string name,
               std::vector<std::string> projectColumnNames,
               int nodeId,
               std::shared_ptr<Expression> predicate,
               std::shared_ptr<Table> table,
               std::vector<std::shared_ptr<SegmentKey>> weightedSegmentKeys) :
	PhysicalOp(std::move(name), FILTER, std::move(projectColumnNames), nodeId),
  predicate_(std::move(predicate)),
	received_(fpdb::tuple::TupleSet::makeWithNullTable()),
	filtered_(fpdb::tuple::TupleSet::makeWithNullTable()),
  table_(std::move(table)),
	weightedSegmentKeys_(std::move(weightedSegmentKeys)) {}

std::string FilterPOp::getTypeString() const {
  return "FilterPOp";
}

void FilterPOp::onReceive(const Envelope &Envelope) {
  const auto& message = Envelope.message();

  if (message.type() == MessageType::START) {
	  this->onStart();
  } else if (message.type() == MessageType::TUPLE) {
    auto tupleMessage = dynamic_cast<const TupleMessage &>(message);
    this->onTuple(tupleMessage);
  } else if (message.type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(message);
    this->onComplete(completeMessage);
  } else if (message.type() == MessageType::DEBUG) {
    this->onDebug();
  } else if (message.type() == MessageType::PREDICATE) {
    auto predicateMessage = dynamic_cast<const PredicateMessage &>(message);
    this->onPredicate(predicateMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + message.getTypeString());
  }
}

void FilterPOp::onStart() {
  assert(received_->validate());
  assert(filtered_->validate());
}

void FilterPOp::onPredicate(const PredicateMessage &message) {
	recvFromPred = true;
	std::cout << name_ + " receive Predicate Msg from " + message.sender() + ", location: " + std::to_string(message.getLocation()) << std::endl;
  nextOpLocation_ = message.getLocation();
  sendSavedTuple();
  if (recvAllCompleteMsg() && readAllFile() && readAllSavedTuple()) {
#if OP_INFO>0
    if(op_stop_time_==0){
			op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
		}
#endif
    ctx()->notifyComplete();appendOpTime();
  }
}

void FilterPOp::onTuple(const TupleMessage &Message) {
#if OP_INFO>0
  if(op_start_time_==0){
		op_start_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
	}
#endif
  receiveTupleSet = true;
  SPDLOG_DEBUG("onTuple  |  Message tupleSet - numRows: {}", Message.tuples()->numRows());
  /**
   * Check if this filter is applicable, if not, just send an empty table and complete
   */
  const auto& tupleSet = Message.tuples();
  if (applicable_ == nullptr) {
    applicable_ = std::make_shared<bool>(isApplicable(tupleSet));
  }

  if (*applicable_) {
#if OP_INFO>0
    input_row+=tupleSet->numRows();
		input_size+=tupleSet->size();
    if(input_row_first==0){
    input_row_first=tupleSet->numRows();
  }
#endif
    SPDLOG_DEBUG("FilterPOp onTuple: {}, {}, {}", tupleSet->numRows(), received_->numRows(), name());
    if (!shouldSpawnOnRemote_ || rowLimit.load() > 0) {
      bufferTuples(tupleSet);
      buildFilter();
      filterTuples();
      sendTuples();
    } else {
      dumpToFile(tupleSet);
    }
  }
}

void FilterPOp::onDebug() {
  while (!readAllFile()) {
    if (rowLimit.load() <= 0) {
        rowLimit_mtx.lock();
        vec.push_back(this->ctx()->operatorActor());
        rowLimit_mtx.unlock();
        return;
    }
    auto expTupleSet = readTuple();
    bufferTuples(expTupleSet);
    buildFilter();
    filterTuples();
    sendTuples();
  }
  //No more file in disk
  if(received_->valid()) {
      filterTuples();
      sendTuples();
  }
  if (!weightedSegmentKeys_.empty() && totalNumRows_ > 0 && *applicable_) {
    sendSegmentWeight();
  }

  filter_ = std::nullopt;
  assert(this->received_->numRows() == 0);
  assert(this->filtered_->numRows() == 0);
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

void FilterPOp::onComplete(const CompleteMessage&) {
  if(recvAllCompleteMsg()) {
    if (!receiveTupleSet) {
#if OP_INFO>0
      if(op_stop_time_==0){
			op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
		}
#endif
      ctx()->notifyComplete();appendOpTime();
    } else if (applicable_ != nullptr && *applicable_) {
      SPDLOG_DEBUG("onComplete  |  Received buffer tupleSet - numRows: {}", received_->numRows());

      if (readAllFile()) {
        if(received_->valid()) {
          filterTuples();
          sendTuples();
        }

        if (!weightedSegmentKeys_.empty() && totalNumRows_ > 0 && *applicable_) {
          sendSegmentWeight();
        }

        filter_ = std::nullopt;
        assert(this->received_->numRows() == 0);
        assert(this->filtered_->numRows() == 0);
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
      } else {
        rowLimit_mtx.lock();
        vec.push_back(this->ctx()->operatorActor());
        rowLimit_mtx.unlock();
      }
    } else {
      // empty table
      auto emptyTupleSet = fpdb::tuple::TupleSet::makeWithEmptyTable();
#if RUNTIME_SAMPLE_EN > 0
      if ((link2Pred && !recvFromPred)||(Pred2link&&(receivedTupleSet<2))) {
        saveTuple(emptyTupleSet);
      } else {
        std::shared_ptr<fpdb::executor::message::Message> tupleMessage = std::make_shared<TupleMessage>(emptyTupleSet,
                                                                                                        name());
        ctx()->tell(tupleMessage, nextOpLocation_);
      }
#elif PREDICATE_EN > 0
      if (link2Pred && !recvFromPred) {
        saveTuple(emptyTupleSet);
      } else {
        std::shared_ptr<fpdb::executor::message::Message> tupleMessage = std::make_shared<TupleMessage>(emptyTupleSet,
                                                                                                        name());
        ctx()->tell(tupleMessage, nextOpLocation_);
      }
#else
      std::shared_ptr<fpdb::executor::message::Message> tupleMessage = std::make_shared<TupleMessage>(emptyTupleSet,
                                                                                                        name());
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
  }
}

void FilterPOp::bufferTuples(const std::shared_ptr<fpdb::tuple::TupleSet>& tupleSet) {
  if(!received_->valid()) {
    received_ = tupleSet;
  } else {
    auto result = received_->append(tupleSet);
    if (!result.has_value()) {
      ctx()->notifyError(result.error());
    }
    assert(received_->validate());
  }
}

bool FilterPOp::isApplicable(const std::shared_ptr<fpdb::tuple::TupleSet>& tupleSet) {
  auto predicateColumnNames = predicate_->involvedColumnNames();
  auto tupleColumnNames = std::make_shared<std::vector<std::string>>();
  for (auto const &field: tupleSet->schema()->fields()) {
    tupleColumnNames->emplace_back(field->name());
  }

  for (auto const &columnName: predicateColumnNames) {
    if (std::find(tupleColumnNames->begin(), tupleColumnNames->end(), columnName) == tupleColumnNames->end()) {
      return false;
    }
  }
  return true;
}

void FilterPOp::buildFilter() {
  if(!filter_.has_value()){
	filter_ = Filter::make(predicate_);
	filter_.value()->compile(Schema::make(received_->schema()));
  }
}

void FilterPOp::filterTuples() {
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
  uint64_t stTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif
  // input metrics
  if (recordSpeeds) {
    totalBytesFiltered_ += received_->size();
  }
  totalNumRows_ += received_->numRows();
  inputBytesFiltered_ += received_->size();
  delta_=received_->numRows();

  // do filter
  std::chrono::steady_clock::time_point startTime = std::chrono::steady_clock::now();

  const auto &expFiltered = filter_.value()->evaluate(*received_);
  if (!expFiltered.has_value()) {
    ctx()->notifyError(expFiltered.error());
  }
  filtered_ = *expFiltered;
  delta_-=filtered_->numRows();

  assert(filtered_->validate());
  std::chrono::steady_clock::time_point stopTime = std::chrono::steady_clock::now();
  filterTimeNS_ += std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();

  // output metrics
  filteredNumRows_ += filtered_->numRows();
  outputBytesFiltered_ += filtered_->size();

  received_->clear();
  assert(received_->validate());
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
  uint64_t enTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  exectime_+=enTime-stTime;
#endif
}

void FilterPOp::sendTuples() {
  // Project using projectColumnNames
  auto expProjectTupleSet = TupleSet::make(filtered_->table())->projectExist(getProjectColumnNames());
  if (!expProjectTupleSet) {
    ctx()->notifyError(expProjectTupleSet.error());
  }
#if OP_INFO>0
  output_row+=expProjectTupleSet.value()->numRows();
  output_size+=expProjectTupleSet.value()->size();
#endif
#if RUNTIME_SAMPLE_EN > 0
std::cout<<name()<<" FilterPOp::sendTuples, receivedTupleSet="<<receivedTupleSet<<",Pred2link="<<Pred2link<<",link2Pred="<<link2Pred<<std::endl;
  if(receivedTupleSet==1&&!sendSampleTimeToPred&&Pred2link){//在发送这条消息之前，上游不会发送第二条tuple消息
    sendSampleTimeToPred=true;
    std::cout<<name()<<" consumer2predictor "<<std::endl;
    auto msg = std::make_shared<PredicateMessage>(name(),true,exectime_,nodeId_);
    ctx()->consumer2predictor(msg);
  }
  if(receivedTupleSet>=2){//已被选中
      uint64_t numRows = expProjectTupleSet.value()->numRows();
      uint64_t size = expProjectTupleSet.value()->size();
      if(SampleDataResult_){
        numRows=expProjectTupleSet.value()->numRows()+SampleDataResult_->numRows();
      }

      if (link2Pred&&!recvFromPred) {
        std::string target = "";
        for (auto consumer : consumers_) {
          if (consumer.find("[0]") != std::string::npos) {
            target = consumer;
            break;
          }
        }
        std::cout<<name()<<"recvFromPred,sendToPred,numRows"<<recvFromPred<<" "<<sendToPred<<" "<<numRows<<std::endl;
        if (!recvFromPred && !sendToPred && numRows > 0&&iflock()) {
          std::cout<<name()<<"send basicinfo"<<std::endl;
          auto msg = std::make_shared<PredicateMessage>(name(), target, nodeId_, numRows * consumerFactor_, size * consumerFactor_,consumerFactor_);
          ctx()->producer2predictor(msg);
          sendToPred = true;
        }
        if(!sendSampleTuple){//被selected的op 读取采样数据结果，作为新的采样数据向下发送
            std::cout<<name()<<" send sample 2 "<<std::endl;
            sendSampleTuple=true;
            auto sampleTupleSet=SampleDataResult_;
            // send sample tuple to -1 & 0
            auto nodeSet=getNodeSet();
            for (const int& element : nodeSet) {	
              auto tmpTuple=sampleTupleSet->slice(0, sampleTupleSet->numRows());
			        std::shared_ptr<Message> sampleTupleMessage = std::make_shared<TupleMessage>(tmpTuple, this->name());		
              ctx()->tell(sampleTupleMessage, element);
            }
        }
      }
      else{
          if(SampleDataResult_->numRows()>0&&!sendSampleTuple){
            sendSampleTuple=true;
            std::cout<<name()<<" send sample rows= "<<SampleDataResult_->numRows()<<std::endl;
            std::shared_ptr<Message> tupleMessage = std::make_shared<TupleMessage>(SampleDataResult_,name());
            ctx()->tell(tupleMessage, nextOpLocation_);
            // SampleDataResult_->clear();
          }
      } 
      if ((link2Pred && !recvFromPred)) {//不知道往哪里发数据
        std::cout<<name()<<" save "<<std::endl;
        saveTuple(expProjectTupleSet.value());
      }
      else {
        std::cout<<name()<<" send "<<std::endl;
        std::shared_ptr<Message> tupleMessage = std::make_shared<TupleMessage>(expProjectTupleSet.value(),name());
        ctx()->tell(tupleMessage, nextOpLocation_);
      }
  }
  if(Pred2link&&(receivedTupleSet<2)){//尚未被选中
    std::cout<<name()<<" save sample "<<std::endl;
    SampleDataResult_=expProjectTupleSet.value();
  }
#elif PREDICATE_EN > 0
  if (link2Pred) {
    uint64_t numRows = expProjectTupleSet.value()->numRows();
    uint64_t size = expProjectTupleSet.value()->size();
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
    saveTuple(expProjectTupleSet.value());
  } else {
    std::shared_ptr<Message> tupleMessage = std::make_shared<TupleMessage>(expProjectTupleSet.value(),name());
    ctx()->tell(tupleMessage, nextOpLocation_);
  }
#else
  std::shared_ptr<Message> tupleMessage = std::make_shared<TupleMessage>(expProjectTupleSet.value(),name());
  ctx()->tell(tupleMessage, nextOpLocation_);
#endif
  filtered_->clear();
  assert(filtered_->validate());
}

int getPredicateNum(const std::shared_ptr<Expression> &expr) {
  if (expr->getType() == AND || expr->getType() == OR) {
    auto biExpr = std::static_pointer_cast<BinaryExpression>(expr);
    return getPredicateNum(biExpr->getLeft()) + getPredicateNum(biExpr->getRight());
  } else {
    return 1;
  }
}

void FilterPOp::sendSegmentWeight() {
  /**
   * Weight function:
   *   w = sel / vNetwork + (lenRow / (lenCol * vScan) + #pred / (lenCol * vFilterPOp)) / #key
   */
  auto selectivity = ((double) filteredNumRows_) / ((double ) totalNumRows_);
  auto predicateNum = (double) getPredicateNum(predicate_);
  auto numKey = (double) weightedSegmentKeys_.size();
  std::unordered_map<std::shared_ptr<SegmentKey>, double> weightMap;
  for (auto const &segmentKey: weightedSegmentKeys_) {
    auto columnName = segmentKey->getColumnName();
    auto lenCol = (double) table_->getApxColumnLength(columnName);
    auto lenRow = (double) table_->getApxRowLength();

    auto weight = selectivity / vNetwork + (lenRow / (lenCol * vS3Scan) + predicateNum / (lenCol * vS3Filter)) / numKey;
    weightMap.emplace(segmentKey, weight);
  }

  ctx()->send(WeightRequestMessage::make(weightMap, name()), "SegmentCache");
}

size_t FilterPOp::getFilterTimeNS() const {
  return filterTimeNS_;
}

size_t FilterPOp::getFilterInputBytes() const {
  return inputBytesFiltered_;
}

size_t FilterPOp::getFilterOutputBytes() const {
  return outputBytesFiltered_;
}

void FilterPOp::clear() {
  received_.reset();
  filtered_.reset();
}
