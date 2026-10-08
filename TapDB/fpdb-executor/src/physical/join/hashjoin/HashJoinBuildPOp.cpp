//
// Created by matt on 29/4/20.
//

#include <fpdb/executor/physical/join/hashjoin/HashJoinBuildPOp.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/message/TupleSetIndexMessage.h>
#include <fpdb/tuple/TupleSet.h>
#include <utility>
#include "string"

using namespace fpdb::executor::physical::join;
using namespace fpdb::tuple;

HashJoinBuildPOp::HashJoinBuildPOp(const string &name,
                                   const vector<string> &projectColumnNames,
                                   int nodeId,
                                   const vector<string> &predColumnNames) :
	PhysicalOp(name, HASH_JOIN_BUILD, projectColumnNames, nodeId),
  columnName_(predColumnNames),
	kernel_(HashJoinBuildKernel::make(predColumnNames)) {
}

std::string HashJoinBuildPOp::getTypeString() const {
  return "HashJoinBuildPOp";
}

void HashJoinBuildPOp::onReceive(const Envelope &msg) {
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
    ctx()->notifyError(fmt::format("Unrecognized message type: {}, {}", msg.message().getTypeString(), name()));
  }
}

void HashJoinBuildPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void HashJoinBuildPOp::onPredicate(const PredicateMessage &message) {
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

void HashJoinBuildPOp::onTuple(const TupleMessage &msg) {
#if OP_INFO>0
  if(op_start_time_==0){
		op_start_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
	}
#endif
  receiveTupleSet = true;
  const auto& tupleSet = msg.tuples();
  if(tupleSet){
  SPDLOG_DEBUG("Adding tuple set to hash table  |  operator: '{}', tupleSet:\n{}", this->name(), tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 1000)));
#if OP_INFO>0
  input_row+=tupleSet->numRows();
	input_size+=tupleSet->size();
  if(input_row_first==0){
    input_row_first=tupleSet->numRows();
  }
#endif
  if (!shouldSpawnOnRemote_ || rowLimit.load() > 0) {
    auto result = buffer(tupleSet);
    if(!result)
      ctx()->notifyError(fmt::format("{}, {}", result.error(), name()));
    // send(true);
  } else {
    dumpToFile(tupleSet);
  }
  }
}

void HashJoinBuildPOp::onDebug() {
  while (!readAllFile()) {
    if (rowLimit.load() <= 0) {
        rowLimit_mtx.lock();
        vec.push_back(this->ctx()->operatorActor());
        rowLimit_mtx.unlock();
        return;
    }
    auto expTupleSet = readTuple();
    auto result = buffer(expTupleSet);
    if(!result)
      ctx()->notifyError(fmt::format("{}, {}", result.error(), name()));

  }
  //No more file in disk
  send(true);
  if (recvAllCompleteMsg() && readAllFile() && readAllSavedTuple()) {
#if OP_INFO>0
    if(op_stop_time_==0){
			op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
		}
#endif
    ctx()->notifyComplete();appendOpTime();
  }
}

void HashJoinBuildPOp::onComplete(const CompleteMessage &) {
  if(recvAllCompleteMsg()) {
    if (!receiveTupleSet) {
#if OP_INFO>0
      if(op_stop_time_==0){
        op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
      }
#endif
      ctx()->notifyComplete();appendOpTime();
    } else if (readAllFile()) {
      send(true);
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

tl::expected<void, string> HashJoinBuildPOp::buffer(const shared_ptr<TupleSet> &tupleSet) {
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
  uint64_t stTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif
  delta_=tupleSet->numRows();
  tl::expected<void, string> result={};
  if(tupleSet->numRows()>0){
  result=kernel_.put(tupleSet);
  }
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
  uint64_t enTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  exectime_+=enTime-stTime;
#endif
#if RUNTIME_SAMPLE_EN > 0
std::cout<<name()<<" HashJoinBuildPOp::buffer, maxReceivedFormProducer()="<<maxReceivedFormProducer()<<",Pred2link="<<Pred2link<<",link2Pred="<<link2Pred<<std::endl;
if((maxReceivedFormProducer()==1)&&!sendSampleTimeToPred&&Pred2link){
    sendSampleTimeToPred=true;
    std::cout<<name()<<" consumer2predictor "<<std::endl;
    auto msg = std::make_shared<PredicateMessage>(name(),true,exectime_,nodeId_);
    ctx()->consumer2predictor(msg);
}
#endif
  return result;
}

void HashJoinBuildPOp::send(bool force) {
  if (kernel_.getTupleSetIndex().has_value() && (force || kernel_.getTupleSetIndex().value()->size() >= DefaultBufferSize)) {
    auto tupleSetIndex = kernel_.getTupleSetIndex().value();
    shared_ptr<Message> tupleSetIndexMessage =
            make_shared<TupleSetIndexMessage>(tupleSetIndex, name());
#if RUNTIME_SAMPLE_EN > 0
if(maxReceivedFormProducer()>=2){
      if (link2Pred&&!recvFromPred) {
        auto tupleSet = tupleSetIndex->getTupleSet();
        uint64_t numRows = tupleSet->numRows();
        uint64_t size = tupleSet->size();
        std::string target = "";
        for (auto consumer : consumers_) {
          if (consumer.find("[0]") != std::string::npos) {
            target = consumer;
            break;
          }
        }
        if (!recvFromPred && !sendToPred && numRows > 0&&iflock()) {
          auto msg = std::make_shared<PredicateMessage>(name(), target, nodeId_, numRows * 1, size * 1,1);
          ctx()->producer2predictor(msg);
          sendToPred = true;
        }
        if(!sendSampleTuple){
            std::cout<<name()<<" send sample 2, rows="<<tupleSetIndex->getTupleSet()->numRows()<<std::endl;
            sendSampleTuple=true;
            // send sample tuple to -1 & 0
            auto nodeSet=getNodeSet();
            for (const int& element : nodeSet) {		
              ctx()->tell(tupleSetIndexMessage, element);
            }
        }
      }
      else{
        std::cout<<name()<<" send sample"<<std::endl;
        ctx()->tell(tupleSetIndexMessage, nextOpLocation_);
      }//Build不需要等recvFromPred，因为发出去的采样数据就是完整的哈希表，Build已经完成任务了

    }
#elif PREDICATE_EN > 0
    if (link2Pred) {
      auto tupleSet = tupleSetIndex->getTupleSet();
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
        auto msg = std::make_shared<PredicateMessage>(name(), target, nodeId_, numRows * 1, size * 1,1);
        ctx()->producer2predictor(msg);
        sendToPred = true;
      }
    }
    
    if (link2Pred && !recvFromPred) {
      saveTuple(tupleSetIndex->getTupleSet());
    } else {
      ctx()->tell(tupleSetIndexMessage, nextOpLocation_);
    }
#else
    ctx()->tell(tupleSetIndexMessage, nextOpLocation_);
#endif
    kernel_.clear();
  }
}

void HashJoinBuildPOp::sendSavedTuple() {
  while (tupleBeginIdx_ <= tupleEndIdx_) {
    std::unordered_map<std::string, std::string> pathConfig = readConfig("exec.conf");
    auto filePath = std::filesystem::absolute("/home/huazhang/Work/FPDB-build-"+pathConfig["CAF_SERVER_PORT"]+"/tmp/" + name_ + ".tuple." + std::to_string(tupleBeginIdx_));
    auto expectedInFile = arrow::io::ReadableFile::Open(filePath);
    auto inputStream = *expectedInFile;
    auto ioContext = arrow::io::IOContext();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto read_options = arrow::csv::ReadOptions::Defaults();
    read_options.use_threads = false;
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    // Instantiate TableReader from input stream and options
    auto makeReaderResult = arrow::csv::TableReader::Make(ioContext,
                                                          inputStream,
                                                          read_options,
                                                          parse_options,
                                                          convert_options);
    if (!makeReaderResult.ok()) {
      std::cout << "Could not create a table reader in" + name_ << std::endl;
    }
    std::shared_ptr<arrow::csv::TableReader> tableReader = *makeReaderResult;
    // Parse the payload and create the tupleset
    auto expTupleSet = TupleSet::make(tableReader);
    inputStream->Close();
    std::string delPath = "/home/huazhang/Work/FPDB-build-"+pathConfig["CAF_SERVER_PORT"]+"/tmp/" + name_ + ".tuple." + std::to_string(tupleBeginIdx_++);
    std::remove(delPath.c_str());
    if (!expTupleSet.has_value()) {
        std::cout << "Unable to make TupleSet in " + name_ << std::endl;
    }

    auto tupleSetIndex = TupleSetIndex::make(columnName_, expTupleSet.value());

    std::shared_ptr<Message> tupleMessage = make_shared<TupleSetIndexMessage>(tupleSetIndex.value(), name());
    ctx()->tell(tupleMessage, nextOpLocation_);
  }
}

void HashJoinBuildPOp::clear() {
  kernel_.clear();
}
