//
// Created by Czk on 04/12/23.
//

#include <fpdb/executor/physical/predicate/PredicatePOp.h>
#include <fpdb/executor/physical/HttpGetPostMethod.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/expression/gandiva/BinaryExpression.h>
#include <fpdb/tuple/Globals.h>
#include <fpdb/util/Util.h>
#include <utility>
#include <string>
#include "arpa/inet.h"
#include "memory"
#include <chrono>

using namespace fpdb::executor::physical::predicate;
using namespace fpdb::cache;
using namespace fpdb::expression::gandiva;
using namespace fpdb::executor::physical;

PredicatePOp::PredicatePOp(std::string name,
              std::vector<std::string> projectColumnNames,
              int nodeId,
              POpType targetType) :
	PhysicalOp(std::move(name), PREDICATE, std::move(projectColumnNames), nodeId), targetType_(targetType) {}

std::string PredicatePOp::getTypeString() const {
  return "PredicatePOp";
}

void PredicatePOp::onReceive(const Envelope &Envelope) {
  const auto& message = Envelope.message();

  if (message.type() == MessageType::START) {
	  this->onStart();
  } else if (message.type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(message);
    this->onComplete(completeMessage);
  } else if (message.type() == MessageType::PREDICATE) {
    auto predicateMessage = dynamic_cast<const PredicateMessage &>(message);
    this->onPredicate(predicateMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + message.getTypeString());
  }
}

void PredicatePOp::onStart() {
    //Noop
}

int PredicatePOp::send2model(std::string opName, uint64_t inputSize, int location, uint64_t inputRow) {
  std::cout<<"PredicatePOp::send2model4"<<std::endl;
  auto start = std::chrono::steady_clock::now();
  std::unordered_map<std::string, std::string> LimitConfig = readConfig("port.conf");
  auto ip = LimitConfig["ip"];
  std::shared_ptr<HttpGetPostMethod> http_request = std::make_shared<HttpGetPostMethod>();
  std::string content = "op=" + opName + "&inputSize=" + std::to_string(inputSize) + "&location=" + 
                        std::to_string(location) + "&inputRow=" + std::to_string(inputRow);
  std::cout<<"PredicatePOp::send2model content1: "<<content<<std::endl;
  int ret = http_request->HttpGet(ip, "/OpDecision/get/", content);
  if(ret == -1) {
      std::cout << name_ + " Http Socket error!" << std::endl;
      return -1;
  }
  if(http_request->get_return_status_code() != 200) {
      std::cout << name_ + " Http get status code was: " << http_request->get_return_status_code() << std::endl;
      std::cout << content << std::endl;
      return -1;
  }
  std::string request_return = http_request->get_request_return();
  std::string main_text = http_request->get_main_text();

  auto end = chrono::steady_clock::now();
  auto used = chrono::duration_cast<chrono::nanoseconds>(end - start).count() / 1000000000.0;
  std::cout << name() + " receive model location: " + main_text + ", time used: " + std::to_string(used) + " secs" << std::endl;
 
  return std::stoi(main_text);
}

int PredicatePOp::send2model(std::string opName, uint64_t inputSize, int location, uint64_t inputRow, uint64_t hashSize, uint64_t hashRow) {
  std::cout<<"PredicatePOp::send2model6"<<std::endl;
  auto start = std::chrono::steady_clock::now();
  std::unordered_map<std::string, std::string> LimitConfig = readConfig("port.conf");
  auto ip = LimitConfig["ip"];
  std::shared_ptr<HttpGetPostMethod> http_request = std::make_shared<HttpGetPostMethod>();
  std::string content = "op=" + opName + "&inputSize=" + std::to_string(inputSize) + "&location=" + 
                        std::to_string(location) + "&inputRow=" + std::to_string(inputRow) + "&hashtableSize=" +
                        std::to_string(hashSize) + "&hashtableRow=" + std::to_string(hashRow);
  std::cout<<"PredicatePOp::send2model content2: "<<content<<std::endl;
  int ret = http_request->HttpGet(ip, "/OpDecision/get/", content);
  if(ret == -1) {
      std::cout << name_ + " Http Socket error!" << std::endl;
      return -1;
  }
  if(http_request->get_return_status_code() != 200) {
      std::cout << name_ + " Http get status code was: " << http_request->get_return_status_code() << std::endl;
      std::cout << content << std::endl;
      return -1;
  }
  std::string request_return = http_request->get_request_return();
  std::string main_text = http_request->get_main_text();

  auto end = chrono::steady_clock::now();
  auto used = chrono::duration_cast<chrono::nanoseconds>(end - start).count() / 1000000000.0;
  std::cout << name() + " receive model location: " + main_text + ", time used: " + std::to_string(used) + " secs" << std::endl;
 
  return std::stoi(main_text);
}

void PredicatePOp::onPredicate(const PredicateMessage& Message) {
  std::cout << name_ + " receive Predicate Msg from " + Message.sender() + ", consumer: " + Message.getConsumer() + 
              ", size: " + std::to_string(Message.getSize()) + ", rows: " + std::to_string(Message.getRows())+", sample: "+std::to_string(Message.getSample())+
              ", smapleTime:"+ std::to_string(Message.getSampleTime())+", sampleLocation: "+std::to_string(Message.getSampleLocation())<< std::endl;
#if PREDICATE_EN>0
  switch (targetType_) {
    case POpType::FILTER :
    case POpType::SORT : 
    case POpType::GROUP : 
    case POpType::AGGREGATE :
    case POpType::HASH_JOIN_BUILD : {
      if (!recvFromTupleSet_) {
        recvFromTupleSet_ = true;
        tupleSize_ = Message.getSize();
        tupleRows_ = Message.getRows();
        int loc = send2model(Message.getConsumer(), tupleSize_, Message.getLocation(), tupleRows_);
        auto msg = make_shared<PredicateMessage>(name(), loc);
        ctx()->predictor2producer(msg);
        std::string log=Message.getConsumer()+","+std::to_string(tupleSize_)+","+std::to_string(tupleRows_)+","+std::to_string(Message.getFactor())+","+std::to_string(Message.getLocation());
        cardinality_mtx.lock();
        cardinality_log.push_back(log);
        cardinality_mtx.unlock();
      }
      break;
    }
    case POpType::HASH_JOIN_PROBE : {
      if (Message.sender().substr(0, 13).compare("HashJoinBuild") == 0) {
        if (!recvFromBuild_) {
          recvFromBuild_ = true;
          hashSize_ = Message.getSize();
          hashRows_ = Message.getRows();
          if (recvFromTupleSet_) {
            int loc = send2model(Message.getConsumer(), tupleSize_, Message.getLocation(), tupleRows_, hashSize_, hashRows_);
            auto msg = make_shared<PredicateMessage>(name(), loc);
            ctx()->predictor2producer(msg);
            std::string log=Message.getConsumer()+","+std::to_string(tupleSize_)+","+std::to_string(tupleRows_)+","+std::to_string(Message.getFactor())+","+std::to_string(Message.getLocation())+","+std::to_string(hashSize_)+","+std::to_string(hashRows_);
            cardinality_mtx.lock();
            // std::cout<<"log:"<<log<<std::endl;
            cardinality_log.push_back(log);
            cardinality_mtx.unlock();
          }
        }
      } else {
        if (!recvFromTupleSet_) {
          recvFromTupleSet_ = true;
          tupleSize_ = Message.getSize();
          tupleRows_ = Message.getRows();
          if (recvFromBuild_) {
            int loc = send2model(Message.getConsumer(), tupleSize_, Message.getLocation(), tupleRows_, hashSize_, hashRows_);
            auto msg = make_shared<PredicateMessage>(name(), loc);
            ctx()->predictor2producer(msg);
            std::string log=Message.getConsumer()+","+std::to_string(tupleSize_)+","+std::to_string(tupleRows_)+","+std::to_string(Message.getFactor())+","+std::to_string(Message.getLocation())+","+std::to_string(hashSize_)+","+std::to_string(hashRows_);
            cardinality_mtx.lock();
            cardinality_log.push_back(log);
            cardinality_mtx.unlock();
          }
        }
      }
      break;
    }
    
    
    
    
  }
#endif
#if RUNTIME_SAMPLE_EN>0
  switch (targetType_) {
    case POpType::HASH_JOIN_PROBE : {
      // recvFromTupleSet_mtx.lock();
      if(!Message.getSample()){
          if (Message.sender().substr(0, 13).compare("HashJoinBuild") == 0) {
          if (!recvFromBuild_) {
            std::cout<<name()<<" set basic hash row info"<<std::endl;
            recvFromBuild_ = true;
            hashSize_ = Message.getSize();
            hashRows_ = Message.getRows();
          }
        } else {
          if (!recvFromTupleSet_) {
            std::cout<<name()<<" set basic row info"<<std::endl;
            recvFromTupleSet_ = true;
            tupleSize_ = Message.getSize();
            tupleRows_ = Message.getRows();
            producerLoc_ = Message.getLocation();//直接选择非哈希表
            consumerOpName_ = Message.getConsumer();
          }

        }
      }
      if(Message.getSample()){
        if(!recvFromConsumer0_&&Message.getSampleLocation()==0){
          std::cout<<name()<<" set sample form 0"<<std::endl;
          recvFromConsumer0_=true;
          sampleTime0_=Message.getSampleTime();
        }
        else if(!recvFromConsumer_1_){
          std::cout<<name()<<" set sample form -1"<<std::endl;
          recvFromConsumer_1_=true;
          sampleTime_1_=Message.getSampleTime();
        }
      }
      if (recvFromTupleSet_&&recvFromBuild_&&recvFromConsumer0_&&recvFromConsumer_1_&&!requested_) {
          requested_=true;
          int loc = send2model(consumerOpName_, tupleSize_, producerLoc_, tupleRows_, hashSize_, hashRows_);
          auto msg = make_shared<PredicateMessage>(name(), loc);
          ctx()->predictor2producer(msg);
      }
      // recvFromTupleSet_mtx.unlock();
      break;
    }
    case POpType::HASH_JOIN_BUILD : 
    case POpType::AGGREGATE : 
    case POpType::GROUP : 
    case POpType::SORT : 
    case POpType::FILTER : {
      if (!recvFromTupleSet_&&(!Message.getSample())) {
        std::cout<<name()<<" set basic row info"<<std::endl;
        recvFromTupleSet_ = true;
        tupleSize_ = Message.getSize();
        tupleRows_ = Message.getRows();
        producerLoc_ = Message.getLocation();
        consumerOpName_ = Message.getConsumer();
      }
      if(Message.getSample()){
        if(!recvFromConsumer0_&&Message.getSampleLocation()==0){
          std::cout<<name()<<" set sample form 0"<<std::endl;
          recvFromConsumer0_=true;
          sampleTime0_=Message.getSampleTime();
        }
        else if(!recvFromConsumer_1_){
          std::cout<<name()<<" set sample form -1"<<std::endl;
          recvFromConsumer_1_=true;
          sampleTime_1_=Message.getSampleTime();
        }

      }
      if(recvFromTupleSet_&&recvFromConsumer0_&&recvFromConsumer_1_&&!requested_){
        requested_=true;
        int loc = send2model(consumerOpName_, tupleSize_, producerLoc_ , tupleRows_);
        auto msg = make_shared<PredicateMessage>(name(), loc);
        ctx()->predictor2producer(msg);
      }
      break; 
    }
  }
#endif
}

void PredicatePOp::onComplete(const CompleteMessage& Message) {
    if(recvAllCompleteMsg()) {
        std::cout << name_ + " receive all complete message!" << std::endl;
        std::cout << name_ + " Tuple Rows: " + std::to_string(tupleRows_) << std::endl;
        std::cout << name_ + " Tuple Size: " + std::to_string(tupleSize_) << std::endl;
        std::cout << name_ + " Hash Rows: " + std::to_string(hashRows_) << std::endl;
        std::cout << name_ + " Hash Size: " + std::to_string(hashSize_) << std::endl;
        ctx()->notifyComplete();
    }
}

void PredicatePOp::clear() {
    //Noop
}