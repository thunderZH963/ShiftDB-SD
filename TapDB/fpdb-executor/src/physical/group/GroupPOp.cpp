//
// Created by matt on 13/5/20.
//

#include <fpdb/executor/physical/group/GroupPOp.h>

using namespace fpdb::tuple;

namespace fpdb::executor::physical::group {

GroupPOp::GroupPOp(const string &name,
                   const vector<string> &projectColumnNames,
                   int nodeId,
                   const vector<string> &groupColumnNames,
                   const vector<shared_ptr<aggregate::AggregateFunction>> &aggregateFunctions) :
	PhysicalOp(name, GROUP, projectColumnNames, nodeId),
  kernel_(groupColumnNames, aggregateFunctions) {
}

std::string GroupPOp::getTypeString() const {
  return "GroupPOp";
}

void GroupPOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
	  this->onStart();
  } else if (msg.message().type() == MessageType::TUPLE) {
    auto tupleMessage = dynamic_cast<const TupleMessage &>(msg.message());
    this->onTuple(tupleMessage);
  } else if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else if (msg.message().type() == MessageType::DEBUG) {
    this->onDebug();
  } else if (msg.message().type() == MessageType::PREDICATE) {
    auto predicateMessage = dynamic_cast<const PredicateMessage &>(msg.message());
    this->onPredicate(predicateMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + msg.message().getTypeString());
  }
}

void GroupPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void GroupPOp::onTuple(const TupleMessage &message) {
#if OP_INFO>0
  	if(op_start_time_==0){
		op_start_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
	}
#endif
  receiveTupleSet = true;
  const auto &tupleSet = message.tuples();
#if OP_INFO>0
  input_row+=tupleSet->numRows();
	input_size+=tupleSet->size();
  if(input_row_first==0){
    input_row_first=tupleSet->numRows();
  }
#endif
  if (!shouldSpawnOnRemote_ || rowLimit.load() > 0) {
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
    uint64_t stTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif
    if(tupleSet->numRows()>0){
      auto expectedGroupResult = kernel_.group(*tupleSet);
      if(!expectedGroupResult)
        ctx()->notifyError(expectedGroupResult.error());
    }
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
    uint64_t enTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  	exectime_+=enTime-stTime;
#endif
#if RUNTIME_SAMPLE_EN > 0
  std::cout<<name()<<" GroupPOp::onTuple, receivedTupleSet="<<receivedTupleSet<<",Pred2link="<<Pred2link<<",link2Pred="<<link2Pred<<std::endl;
  if(receivedTupleSet==1&&!sendSampleTimeToPred&&Pred2link){
    sendSampleTimeToPred=true;
    std::cout<<name()<<" consumer2predictor "<<std::endl;
    auto msg = std::make_shared<PredicateMessage>(name(),true,exectime_,nodeId_);
    ctx()->consumer2predictor(msg);
  }
#endif
  } else {
    dumpToFile(tupleSet);
  }
}

void GroupPOp::onDebug() {
  while (!readAllFile()) {
    if (rowLimit.load() <= 0) {
        rowLimit_mtx.lock();
        vec.push_back(this->ctx()->operatorActor());
        rowLimit_mtx.unlock();
        return;
    }
    auto expTupleSet = readTuple();

    auto expectedGroupResult = kernel_.group(*expTupleSet);
    if(!expectedGroupResult)
      ctx()->notifyError(expectedGroupResult.error());
  }
  //No more file in disk
  auto expectedGroupedTupleSet = kernel_.finalise();
  if (!expectedGroupedTupleSet)
    ctx()->notifyError(expectedGroupedTupleSet.error());

  // Project using projectColumnNames
  auto expProjectTupleSet = expectedGroupedTupleSet.value()->projectExist(getProjectColumnNames());
  if (!expProjectTupleSet) {
    ctx()->notifyError(expProjectTupleSet.error());
  }
#if RUNTIME_SAMPLE_EN > 0
if(receivedTupleSet>=2){
      uint64_t numRows = expProjectTupleSet.value()->numRows();
      uint64_t size = expProjectTupleSet.value()->size();
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
            auto sampleTupleSet=expProjectTupleSet.value()->slice(0, splitRows);
            expProjectTupleSet=expProjectTupleSet.value()->slice(splitRows,numRows-splitRows);
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
        saveTuple(expProjectTupleSet.value());
      } else {
        std::cout<<name()<<" send "<<std::endl;
        shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(expProjectTupleSet.value(), this->name());
        ctx()->tell(tupleMessage, nextOpLocation_);
      }

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
    shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(expProjectTupleSet.value(), this->name());
    ctx()->tell(tupleMessage, nextOpLocation_);
  }
#else
  shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(expProjectTupleSet.value(), this->name());
  ctx()->tell(tupleMessage, nextOpLocation_);
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

void GroupPOp::onPredicate(const PredicateMessage &message) {
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

void GroupPOp::onComplete(const CompleteMessage &) {
  if (recvAllCompleteMsg()) {
    if (!receiveTupleSet) {
#if OP_INFO>0
      if(op_stop_time_==0){
			op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
		}
#endif
      ctx()->notifyComplete();appendOpTime();
    } else if (readAllFile()) {
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
      uint64_t stTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif
      auto expectedGroupedTupleSet = kernel_.finalise();
      if (expectedGroupedTupleSet) {
        // Project using projectColumnNames
        auto expProjectTupleSet = expectedGroupedTupleSet.value()->projectExist(getProjectColumnNames());
        if (!expProjectTupleSet) {
          ctx()->notifyError(expProjectTupleSet.error());
        }
#if RUNTIME_SAMPLE_EN > 0
if(receivedTupleSet>=2){
            uint64_t numRows = expProjectTupleSet.value()->numRows();
            uint64_t size = expProjectTupleSet.value()->size();
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
                  auto sampleTupleSet=expProjectTupleSet.value()->slice(0, splitRows);
                  expProjectTupleSet=expProjectTupleSet.value()->slice(splitRows,numRows-splitRows);
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
              saveTuple(expProjectTupleSet.value());
            } else {
              shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(expProjectTupleSet.value(), this->name());
              ctx()->tell(tupleMessage, nextOpLocation_);
            }

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
          shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(expProjectTupleSet.value(), this->name());
          ctx()->tell(tupleMessage, nextOpLocation_);
        }
#else
        shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(expProjectTupleSet.value(), this->name());
        ctx()->tell(tupleMessage, nextOpLocation_);
#endif
      }
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
      uint64_t enTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  		exectime_+=enTime-stTime;
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

void GroupPOp::clear() {
  kernel_.clear();
}

}
