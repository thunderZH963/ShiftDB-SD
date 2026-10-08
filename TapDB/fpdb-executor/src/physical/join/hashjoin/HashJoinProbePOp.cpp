//
// Created by matt on 29/4/20.
//

#include <fpdb/executor/physical/join/hashjoin/HashJoinProbePOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinProbeKernel.h>
#include <fpdb/executor/physical/join/hashjoin/HashSemiJoinProbeKernel.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/message/TupleSetIndexMessage.h>
#include <fpdb/tuple/arrow/SchemaHelper.h>
#include <utility>

using namespace fpdb::executor::physical::join;

HashJoinProbePOp::HashJoinProbePOp(string name,
                                   vector<string> projectColumnNames,
                                   int nodeId,
                                   const HashJoinPredicate& pred,
                                   JoinType joinType) :
	PhysicalOp(name, HASH_JOIN_PROBE, move(projectColumnNames), nodeId) {

  set<string> neededColumnNames(getProjectColumnNames().begin(), getProjectColumnNames().end());
  switch (joinType) {
    case INNER: {
      kernel_ = HashJoinProbeKernel::make(pred, move(neededColumnNames), false, false, name);
      break;
    }
    case LEFT: {
      kernel_ = HashJoinProbeKernel::make(pred, move(neededColumnNames), true, false, name);
      break;
    }
    case RIGHT: {
      kernel_ = HashJoinProbeKernel::make(pred, move(neededColumnNames), false, true, name);
      break;
    }
    case FULL: {
      kernel_ = HashJoinProbeKernel::make(pred, move(neededColumnNames), true, true, name);
      break;
    }
    case SEMI: {
      kernel_ = HashSemiJoinProbeKernel::make(pred, move(neededColumnNames), name);
      break;
    }
    default:
      // semi is not supported
      // This is not inside actor processing, so we cannot use ctx()->notifyError()
      throw runtime_error(fmt::format("Unsupported hash join type, {}", joinType));
  }
}

std::string HashJoinProbePOp::getTypeString() const {
  return "HashJoinProbePOp";
}

void HashJoinProbePOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
	  this->onStart();
  } else if (msg.message().type() == MessageType::TUPLE) {
    auto tupleMessage = dynamic_cast<const TupleMessage &>(msg.message());
    this->onTuple(tupleMessage);
  } else if (msg.message().type() == MessageType::TUPLESET_INDEX) {
    auto hashTableMessage = dynamic_cast<const TupleSetIndexMessage &>(msg.message());
    this->onHashTable(hashTableMessage);
  } else if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else if (msg.message().type() == MessageType::DEBUG) {
    this->onDebug();
  } else if (msg.message().type() == MessageType::PREDICATE) {
    auto predicateMessage = dynamic_cast<const PredicateMessage &>(msg.message());
    this->onPredicate(predicateMessage);
  } else {
	  ctx()->notifyError(fmt::format("Unrecognized message type: {}, {}", msg.message().getTypeString(), name()));
  }
}

void HashJoinProbePOp::onStart() {
  kernel_->setName(name_);
  auto ptr = ctx()->operatorActor()->operator_();
  kernel_->setOp(ptr);
  for (std::string producer : this->producers()) {
    if (producer.find("HashJoinBuild") == 0) {
      totBuild_++;
    }
  }
  // std::cout << name_ + " totBuild_ is: " + std::to_string(totBuild_) << std::endl;
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void HashJoinProbePOp::onPredicate(const PredicateMessage &message) {
	recvFromPred = true;
	std::cout << name_ + " receive Predicate Msg from " + message.sender() + ", location: " + std::to_string(message.getLocation()) << std::endl;
  nextOpLocation_ = message.getLocation();
  sendSavedTuple();
#if RUNTIME_SAMPLE_EN > 0
    if(Pred2link&&(maxReceivedFormProducer()<2)){
      dropSavedTuple();
    }
#endif
  if (recvAllCompleteMsg() && readAllFile() && readAllSavedTuple()) {
#if OP_INFO>0
    if(op_stop_time_==0){
			op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
		}
#endif
    ctx()->notifyComplete();exectime_=kernel_->exectime_;appendOpTime();
  }
}

void HashJoinProbePOp::onTuple(const TupleMessage &msg) {
#if OP_INFO>0
  if(op_start_time_==0){
		op_start_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
	}
#endif
  receiveTupleSet = true;
  // Incremental join immediately
  const auto& tupleSet = msg.tuples();
#if OP_INFO>0
  input_row+=tupleSet->numRows();
	input_size+=tupleSet->size();
  if(input_row_first==0){
    input_row_first=tupleSet->numRows();
  }
#endif
  auto result = kernel_->joinProbeTupleSet(tupleSet, totBuild_ == arrivedBuild_);
  if(!result)
    ctx()->notifyError(fmt::format("{}, {}", result.error(), name()));
}

void HashJoinProbePOp::onComplete(const CompleteMessage &msg) {
  // std::cout << name_ + " receive complete from " + msg.sender() << std::endl;
  if (msg.sender().find("HashJoinBuild") == 0) {
    arrivedBuild_++;
    // std::cout << name_ + " arrivedBuild_ is: " + std::to_string(arrivedBuild_) << std::endl;
  }
  if (recvAllCompleteMsg()&&arrivedBuild_>=1) {
    if (!receiveTupleSet) {
#if OP_INFO>0
      if(op_stop_time_==0){
        op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
      }
#endif
#if RUNTIME_SAMPLE_EN > 0
      if(Pred2link&&(maxReceivedFormProducer()<2)){
        dropSavedTuple();
      }
#endif
      ctx()->notifyComplete();exectime_=kernel_->exectime_;appendOpTime();
    } else {
      // Finalize
      auto result = kernel_->finalize();
      if (!result) {
        ctx()->notifyError(result.error());
      }

      // Send final tupleSet
      send(true);

      if (readAllFile()) {
        // Send empty if no result
        if (!sentResult) {
          sendEmpty();
        }
#if RUNTIME_SAMPLE_EN > 0
      if(Pred2link&&(maxReceivedFormProducer()<2)){
        dropSavedTuple();
      }
#endif
        // Complete
        if (recvAllCompleteMsg() && readAllFile() && readAllSavedTuple()) {
#if OP_INFO>0
          if(op_stop_time_==0){
            op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
          }
#endif
          ctx()->notifyComplete();exectime_=kernel_->exectime_;appendOpTime();
        }
      } else {
        rowLimit_mtx.lock();
        vec.push_back(this->ctx()->operatorActor());
        rowLimit_mtx.unlock();
      }
    }
  }
}

void HashJoinProbePOp::onDebug() {
  auto result = kernel_->finalize();
  if (!result) {
    ctx()->notifyError(result.error());
  }
  send(true);
  
  if (readAllFile()) {
    /*if (!sentResult) {
      sendEmpty();
    }*/
#if RUNTIME_SAMPLE_EN > 0
      if(Pred2link&&(maxReceivedFormProducer()<2)){
        dropSavedTuple();
      }
#endif
    // Complete
  	if (recvAllCompleteMsg() && readAllFile() && readAllSavedTuple()) {
#if OP_INFO>0
      if(op_stop_time_==0){
        op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
      }
#endif
      ctx()->notifyComplete();exectime_=kernel_->exectime_;appendOpTime();
    }
  } else {
    rowLimit_mtx.lock();
    vec.push_back(this->ctx()->operatorActor());
    rowLimit_mtx.unlock();
  }
}

void HashJoinProbePOp::onHashTable(const TupleSetIndexMessage &msg) {
#if OP_INFO>0
  if(op_start_time_==0){
		op_start_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
	}
#endif
  // Incremental join immediately
  auto tupleSet=msg.getTupleSetIndex()->getTupleSet();
#if OP_INFO>0
  right_receivedTupleSet++;
  hashInput_row+=tupleSet->numRows();
	hashInput_size+=tupleSet->size();
  if(right_input_row_first==0){
      right_input_row_first=tupleSet->numRows();
  }
#endif
  auto result = kernel_->joinBuildTupleSetIndex(msg.getTupleSetIndex(), totBuild_ == arrivedBuild_);
  if(!result)
    ctx()->notifyError(fmt::format("{}, {}", result.error(), name()));
}

void HashJoinProbePOp::send(bool force) {
  auto buffer = kernel_->getBuffer();
#if RUNTIME_SAMPLE_EN > 0
//采样join结果未必有数据
  std::cout<<name()<<" HashJoinProbePOp::send, maxReceivedFormProducer()="<<maxReceivedFormProducer()<<",Pred2link="<<Pred2link<<",link2Pred="<<link2Pred<<std::endl;
  if(maxReceivedFormProducer()==1&&!sendSampleTimeToPred&&Pred2link){
    sendSampleTimeToPred=true;
    std::cout<<name()<<" consumer2predictor "<<std::endl;
    auto msg = std::make_shared<PredicateMessage>(name(),true,exectime_,nodeId_);
    ctx()->consumer2predictor(msg);
  }
#endif
  if (buffer.has_value()) {
    uint64_t numRows = buffer.value()->numRows();
    uint64_t size = buffer.value()->size();
    if (numRows >= DefaultBufferSize || (force && numRows > 0)) {
      // Here no need to project buffer using projectColumnNames as it won't have redundant columns
      auto tupleSet = TupleSet::make(buffer.value()->table());
#if RUNTIME_SAMPLE_EN > 0
if(maxReceivedFormProducer()>=2){
      if (link2Pred&&!recvFromPred) {
        std::string target = "";
        for (auto consumer : consumers_) {
          if (consumer.find("[0]") != std::string::npos) {
            target = consumer;
            break;
          }
        }
        if (!recvFromPred && !sendToPred && numRows > 0) {
          auto msg = std::make_shared<PredicateMessage>(name(), target, nodeId_, numRows * consumerFactor_, size * consumerFactor_,consumerFactor_);
          opContext_->producer2predictor(msg);
          sendToPred = true;
        }
        if(!sendSampleTuple){//被selected的op 读取采样数据结果，作为新的采样数据向下发送
            if(!SampleDataResult_)
              SampleDataResult_=TupleSet::makeWithEmptyTable();
            std::cout<<name()<<" send sample 2 ,rows="<<SampleDataResult_->numRows()<<std::endl;
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
          if(SampleDataResult_->numRows()>0&&!sendSampleTuple){//下游已经被选中，不需要此采样tuple带来判断
            sendSampleTuple=true;
            std::cout<<name()<<" send sample rows= "<<SampleDataResult_->numRows()<<std::endl;
            std::shared_ptr<Message> tupleMessage = std::make_shared<TupleMessage>(SampleDataResult_,name());
            ctx()->tell(tupleMessage, nextOpLocation_);
          }
      }
      if (link2Pred && !recvFromPred) {
        saveTuple(tupleSet);
      } else {
        shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(tupleSet, name());
        ctx()->tell(tupleMessage, nextOpLocation_);
      } 
    }
    if(Pred2link&&(maxReceivedFormProducer()<2)){//尚未被选中 因为多对一，这里得改成append
      std::cout<<name()<<" save sample "<<std::endl;
      // SampleDataResult_=tupleSet;
      if(SampleDataResult_){
          const auto &concatenateResult = TupleSet::concatenate({SampleDataResult_, tupleSet});
          if (!concatenateResult)
            return ;
          SampleDataResult_ = concatenateResult.value();
      }
      else{
        SampleDataResult_=tupleSet;
      }
      std::cout<<name()<<" save sample, rows="<<SampleDataResult_->numRows()<<std::endl;
    }
#elif PREDICATE_EN > 0
      if (link2Pred) {
        std::string target = "";
        for (auto consumer : consumers_) {
          if (consumer.find("[0]") != std::string::npos) {
            target = consumer;
            break;
          }
        }
        if (!recvFromPred && !sendToPred && numRows > 0) {
          auto msg = std::make_shared<PredicateMessage>(name(), target, nodeId_, numRows * consumerFactor_, size * consumerFactor_,consumerFactor_);
          opContext_->producer2predictor(msg);
          sendToPred = true;
        }
      }
      if (link2Pred && !recvFromPred) {
        saveTuple(tupleSet);
      } else {
        shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(tupleSet, name());
        ctx()->tell(tupleMessage, nextOpLocation_);
      }
#else
      shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(tupleSet, name());
      ctx()->tell(tupleMessage, nextOpLocation_);
#endif
      sentResult = true;
      kernel_->clearBuffer();
    }
  } else {
    std::cout << name_ << " has no buffer!" << std::endl;
  }
}

void HashJoinProbePOp::sendEmpty() {
  auto outputSchema = kernel_->getOutputSchema();
  if (!outputSchema.has_value()) {
    ctx()->notifyError("OutputSchema not set yet");
  }
  auto tupleSet = TupleSet::make(outputSchema.value());
#if PREDICATE_EN > 0
  if (link2Pred && !recvFromPred) {
    saveTuple(tupleSet);
  } else {
    shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(tupleSet, name());
    ctx()->tell(tupleMessage, nextOpLocation_);
  }
#else
  shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(tupleSet, name());
  ctx()->tell(tupleMessage, nextOpLocation_);
#endif
}

void HashJoinProbePOp::clear() {
  kernel_->clear();
}
