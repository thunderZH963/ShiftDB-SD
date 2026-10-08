//
// Created by matt on 5/12/19.
//

#include <fpdb/executor/physical/POpContext.h>
#include <fpdb/executor/cache/SegmentCacheActor.h>
#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/executor/message/DebugMessage.h>
#include <fpdb/executor/message/cache/LoadRequestMessage.h>
#include <fpdb/executor/message/cache/CacheMetricsMessage.h>
#include <spdlog/spdlog.h>
#include <utility>
#include <cassert>
#include <set>

using namespace fpdb::executor::message;

namespace fpdb::executor::physical {

std::mutex msgLock;
std::queue<fpdb::executor::message::Envelope> msgQueue;
std::queue<::caf::actor> actorQueue;
std::atomic_bool nicFull = false;
std::atomic_bool execFinish = false;

//Only TupleMesasge and TupleSetIndexMessage.
void POpContext::tell(std::shared_ptr<Message> &msg, int location) {
  assert(this);
  if(complete_)
    notifyError(fmt::format("Cannot tell message to consumers, operator {} ('{}') is complete", this->operatorActor()->id(), this->operatorActor()->operator_()->name()));

  message::Envelope e(msg);

  std::string rec_type;
  switch (msg->type()) {
    case MessageType::TUPLE : {
      rec_type = "Tuple";
      break;
    }
    case MessageType::TUPLESET_INDEX : {
      rec_type = "Index";
      break;
    }
    default: {
      rec_type = "Error";
      return;
    }
  }

  // Send message to consumers
  string str = this->operatorActor()->operator_()->name();
  uint64_t curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  msg->setSentTime(curTime);
  for(const auto& consumer: this->operatorActor()->operator_()->consumers()){
#if (PREDICATE_EN>0) || (RUNTIME_SAMPLE_EN>0)
    if (spawnAddr_.at(consumer) != location) {
      //This op is not target, continue.
      continue;
    }
#else
  if (spawnAddr_.at(consumer) != opActiveAddr(consumer)) {
    //This op is not target, stop.
    continue;
  }
#endif
    double elapsedTime = std::chrono::system_clock::now().time_since_epoch().count() / 1000000000.0;
    std::cout << "Tring 1...  sender: " + str + ", recipient: " + consumer + ", type: " + rec_type + ", time: " 
                + std::to_string(elapsedTime) + " secs" << std::endl;
    ::caf::actor actorHandle = operatorMap_.get(consumer).value().getActor();
    operatorActor_->anon_send(actorHandle, e);
  }
}

/*
* Only TupleMessage, LoadRequestMessage, StoreRequestMessage, ScanMessage, CacheMetricsMessage, WeightRequestMessage.
* Only SplitPOp, ShufflePop, CacheLoadPOp will send TupleMessage
*/
void POpContext::send(const std::shared_ptr<message::Message> &msg, const std::string& recipientId) {
  message::Envelope e(msg);

  std::string rec_type;
  string str = this->operatorActor()->operator_()->name();
  switch (msg->type()) {
    case MessageType::TUPLE : {
      rec_type = "TUPLE";
      break;
    }
    default: {
      rec_type = "Other";
    }
  }

  if(recipientId == "SegmentCache"){
    if(msg->type() == MessageType::LOAD_REQUEST){
      operatorActor_->request(segmentCacheActor_, infinite, LoadAtom_v, std::static_pointer_cast<fpdb::executor::message::LoadRequestMessage>(msg))
      .then([=](const std::shared_ptr<fpdb::executor::message::LoadResponseMessage>& response){
      operatorActor_->anon_send(this->operatorActor(), Envelope(response));
      });
    }
    else if(msg->type() == MessageType::STORE_REQUEST){
      operatorActor_->anon_send(segmentCacheActor_, StoreAtom_v, std::static_pointer_cast<fpdb::executor::message::StoreRequestMessage>(msg));
    }
    else if(msg->type() == MessageType::WEIGHT_REQUEST){
      operatorActor_->anon_send(segmentCacheActor_, WeightAtom_v, std::static_pointer_cast<fpdb::executor::message::WeightRequestMessage>(msg));
    }
    else if(msg->type() == MessageType::CACHE_METRICS){
      operatorActor_->anon_send(segmentCacheActor_, MetricsAtom_v, std::static_pointer_cast<fpdb::executor::message::CacheMetricsMessage>(msg));
    }
    else{
      notifyError("Unrecognized message " + msg->getTypeString());
    }

    return;
  }

  if (msg->type() == MessageType::TUPLE && spawnAddr_.at(recipientId) != opActiveAddr(recipientId)) {
    //This op is not target, stop.
    return;
  }

  auto expectedOperator = operatorMap_.get(recipientId);
  if(expectedOperator.has_value()){
    auto recipientOperator = expectedOperator.value();
    auto actorHandle = recipientOperator.getActor();
    // unsigned long long curTime = chrono::steady_clock::now().time_since_epoch().count();
    double elapsedTime = std::chrono::system_clock::now().time_since_epoch().count() / 1000000000.0;
    std::cout << "Tring 2...  sender: " + str + ", recipient: " + recipientId + ", type: " + rec_type 
                + ", time: " + std::to_string(elapsedTime) + " secs" << std::endl;
    operatorActor_->anon_send(actorHandle, e);
  } else{
  	notifyError(fmt::format("Actor with id '{}' not found", recipientId));
  }
}

/**
 * Send a CompleteMessage to all consumers and the root actor
 */
void POpContext::notifyComplete() {
  SPDLOG_DEBUG("Completing operator  |  source: {} ('{}')", this->operatorActor()->id(), this->operatorActor()->operator_()->name());
  if(complete_)
    notifyError(fmt::format("Cannot complete already completed operator {} ('{}')", this->operatorActor()->id(), this->operatorActor()->operator_()->name()));

  POpActor* operatorActor = this->operatorActor();

  std::shared_ptr<message::Message> msg = std::make_shared<message::CompleteMessage>(operatorActor->operator_()->name());
  message::Envelope e(msg);

  // Send message to consumers
  string str = this->operatorActor()->operator_()->name();
  for(const auto& consumer: this->operatorActor()->operator_()->consumers()){
    ::caf::actor actorHandle = operatorMap_.get(consumer).value().getActor();
    // unsigned long long curTime = chrono::steady_clock::now().time_since_epoch().count();
    double elapsedTime = std::chrono::system_clock::now().time_since_epoch().count() / 1000000000.0;
    std::cout << "Tring 3... sender: " + str + ", recipient: " + consumer + ", type: COMPLETE, time: " 
                + std::to_string(elapsedTime) + " secs"<< std::endl;
    operatorActor_->anon_send(actorHandle, e);
  }
  //Send message to predictors
  for(const auto& predictor: this->operatorActor()->operator_()->predictors()) {
    ::caf::actor actorHandle = operatorMap_.get(predictor).value().getActor();
    // unsigned long long curTime = chrono::steady_clock::now().time_since_epoch().count();
    double elapsedTime = std::chrono::system_clock::now().time_since_epoch().count() / 1000000000.0;
    std::cout << "Tring 3... sender: " + str + ", recipient: " + predictor + ", type: COMPLETE, time: " 
                + std::to_string(elapsedTime) + " secs"<< std::endl;
    operatorActor_->anon_send(actorHandle, e);
  }

  std::cout << "Tring 3... sender: " + str + ", recipient: root actor, type: COMPLETE" << std::endl; 
  // Send message to root actor
  operatorActor->anon_send(rootActor_, e);

  // Clear internal state, except collate whose internal state is final result
  if (operatorActor->operator_()->getType() != POpType::COLLATE) {
    operatorActor->operator_()->clear();
  }

  std::cout << str << " expects to receive TupleSet: " << this->operatorActor()->operator_()->getFactor() << ", truly receive TupleSet: " << this->operatorActor()->operator_()->receivedTupleSet << std::endl;

  complete_ = true;
}

/**
 * Send error to the root actor
 */
void POpContext::notifyError(const std::string &content) {
  std::shared_ptr<Message> errorMsg = std::make_shared<ErrorMessage>(content, operatorActor_->name());
  message::Envelope e(errorMsg);
  operatorActor_->anon_send(rootActor_, e);
  operatorActor_->on_exit();
}

void POpContext::notifyDebug() {
  POpActor* operatorActor = this->operatorActor();
  std::shared_ptr<message::Message> msg = std::make_shared<message::DebugMessage>(operatorActor->operator_()->name());
  std::cout << operatorActor->operator_()->name() + " trying to send debug msg to root actor." << std::endl;
  message::Envelope e(msg);
  operatorActor->anon_send(rootActor_, e);
}

void POpContext::producer2predictor(const std::shared_ptr<message::Message> &msg) {
  message::Envelope e(msg);
  string str = this->operatorActor()->operator_()->name();
  for(const auto& predictor: this->operatorActor()->operator_()->predictors()) {
    ::caf::actor actorHandle = operatorMap_.get(predictor).value().getActor();
    std::cout << "Tring 4... sender: " + str + ", recipient: " + predictor + ", type: PREDICATE" << std::endl;
    operatorActor_->anon_send(actorHandle, e);
  }
}

void POpContext::consumer2predictor(const std::shared_ptr<message::Message> &msg) {
  message::Envelope e(msg);
  string str = this->operatorActor()->operator_()->name();
  std::cout<<str<<" samplers.size "<<this->operatorActor()->operator_()->samplers().size()<<std::endl;
  for(const auto& predictor: this->operatorActor()->operator_()->samplers()) {
    std::cout<<"sampler "<<predictor<<std::endl;
    ::caf::actor actorHandle = operatorMap_.get(predictor).value().getActor();
    std::cout << "Tring 4... sender: " + str + ", recipient: " + predictor + ", type: SMAPLE PREDICATE" << std::endl;
    operatorActor_->anon_send(actorHandle, e);
  }
}

void POpContext::predictor2producer(const std::shared_ptr<message::Message> &msg) {
  message::Envelope e(msg);
  string str = this->operatorActor()->operator_()->name();
  for(const auto& producer: this->operatorActor()->operator_()->producers()){
    ::caf::actor actorHandle = operatorMap_.get(producer).value().getActor();
    std::cout << "Tring 5... sender: " + str + ", recipient: " + producer + ", type: PREDICATE" << std::endl;
    operatorActor_->anon_send(actorHandle, e);
  }
}

POpContext::POpContext(::caf::actor rootActor, ::caf::actor segmentCacheActor):
    operatorActor_(nullptr),
    rootActor_(std::move(rootActor)),
    segmentCacheActor_(std::move(segmentCacheActor)) 
{}

LocalPOpDirectory &POpContext::operatorMap() {
  return operatorMap_;
}

std::unordered_map<std::string, int> &POpContext::spawnAddr() {
  return spawnAddr_;
}

std::unordered_map<std::string, int> &POpContext::activeAddr() {
  return activeAddr_;
}

int POpContext::opActiveAddr(std::string name) {
#if FINE_GRAIN == 0
  std::set<std::string> strSet{"AggregateReduce", "Aggregate", "Collate", "FileScan",
                              "Filter", "Group", "HashJoinBuild", "HashJoinProbe", "Shuffle", "Sort",
                              "Predicate", "Split", "Project", "LimitSort", "NestedLoopJoin"};
  for (auto it: strSet) {
    if (name.substr(0, it.length()).compare(it) == 0) {
      return activeAddr_.at(it);
    }
  }
  return -1;
#else
  auto it = activeAddr_.find(name);
  if (it != activeAddr_.end()) {
    return it->second;
  } else {
    return -1;
  }
#endif
}

POpActor* POpContext::operatorActor() {
  return operatorActor_;
}
void POpContext::operatorActor(POpActor *operatorActor) {
  operatorActor_ = operatorActor;
}

::caf::actor POpContext::getRootActor() {
  return rootActor_;
}

bool POpContext::isComplete() const {
  return complete_;
}

void POpContext::destroyActorHandles() {
  operatorMap_.destroyActorHandles();
  destroy(rootActor_);
  destroy(segmentCacheActor_);
}

} // namespace
