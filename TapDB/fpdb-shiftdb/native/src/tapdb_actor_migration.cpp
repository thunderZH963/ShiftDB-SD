#include "shiftdb/tapdb_actor_migration.hpp"
#include "shiftdb/tapdb_operator_state.hpp"
#include <fpdb/executor/physical/POpActor.h>
#include <fpdb/executor/physical/POpContext.h>
#include <fpdb/executor/physical/aggregate/AggregatePOp.h>
#include <fpdb/executor/physical/group/GroupPOp.h>
#include <fpdb/executor/message/Envelope.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <limits>
namespace shiftdb::native {
using namespace fpdb::executor::physical;
using namespace fpdb::executor::message;
namespace {
constexpr uint64_t queue_limit=64ULL*1024*1024, message_limit=4096;
void eligible(PhysicalOp& op) {
 if(!dynamic_cast<aggregate::AggregatePOp*>(&op)&&!dynamic_cast<group::GroupPOp*>(&op))
  throw std::runtime_error("active migration supports AggregatePOp and GroupPOp only");
 if(op.shiftPredicateManaged()||op.ctx()->isComplete()||!op.readAllFile()||!op.readAllSavedTuple())
  throw std::runtime_error("predicate-managed, completed, or file-backed operator is pinned");
}
StateImage binding(PhysicalOp& op,const std::string& fingerprint) {
 StateImage s;s.query_operator=std::to_string(op.getQueryId())+"/"+op.name();
 s.plan_fingerprint=fingerprint;s.generation=1;return s;
}
Bytes snapshot(PhysicalOp& op,StateImage s) {
 if(auto* a=dynamic_cast<aggregate::AggregatePOp*>(&op))return TapDBStateCodec::snapshot(*a,s);
 return TapDBStateCodec::snapshot(dynamic_cast<group::GroupPOp&>(op).shiftStateKernel(),s);
}
void restore(PhysicalOp& op,const Bytes& bytes,const StateImage& expected) {
 if(auto* a=dynamic_cast<aggregate::AggregatePOp*>(&op))TapDBStateCodec::restore(*a,bytes,expected);
 else TapDBStateCodec::restore(dynamic_cast<group::GroupPOp&>(op).shiftStateKernel(),bytes,expected);
}
}
bool TapDBActorMigration::intercept(const Envelope& msg) {
 if(replaying_)return false;
 auto phase=gate_.phase();
 if(phase==MigrationGate::Phase::Forwarding){actor_.send(target_,msg);return true;}
 if(phase==MigrationGate::Phase::Aborted){actor_.operator_()->ctx()->notifyError("message sent to aborted migration target");return true;}
 if(phase!=MigrationGate::Phase::Frozen&&phase!=MigrationGate::Phase::Prepared)return false;
 uint64_t bytes=256;
 if(msg.message().type()==MessageType::TUPLE){
  auto n=dynamic_cast<const TupleMessage&>(msg.message()).tuples()->size();
  bytes=n>queue_limit?queue_limit+1:bytes+n;
 }
 if(queued_.size()>=message_limit||bytes>queue_limit-queued_bytes_){
  actor_.operator_()->ctx()->notifyError("migration input queue exhausted; query must restart");return true;
 }
 queued_.emplace_back(actor_.current_mailbox_element()->content(),actor_.current_sender());
 queued_bytes_+=bytes;return true;
}
caf::message TapDBActorMigration::freeze(const std::string& ticket,const std::string& fingerprint) {
 if(fingerprint.empty())throw std::invalid_argument("empty canonical plan fingerprint");
 if(gate_.phase()==MigrationGate::Phase::Frozen){
  if(ticket!=gate_.ticket()||fingerprint!=fingerprint_)throw std::logic_error("conflicting freeze retry");
 } else {
  auto& op=*actor_.operator_();eligible(op);
  if(!actor_.running_||!actor_.buffer_.empty())throw std::logic_error("source must be running with no startup backlog");
  // No input handler executes concurrently with this actor control handler.
  auto image=snapshot(op,binding(op,fingerprint));
  auto ctx=std::make_shared<POpContext>(*op.ctx());
  gate_.freeze(ticket);image_=std::move(image);context_=std::move(ctx);
  fingerprint_=fingerprint;received_=op.receivedTupleSetMap;count_=op.receivedTupleSet;
 }
 return caf::make_message(image_,*context_,received_,count_);
}
bool TapDBActorMigration::install(const std::string& ticket,const std::string& fingerprint,
 const Bytes& image,const std::shared_ptr<POpContext>& context,const std::map<std::string,int>& received,int64_t count) {
 if(gate_.phase()==MigrationGate::Phase::Prepared){
  if(gate_.ticket()!=ticket||fingerprint_!=fingerprint||image_!=image||received_!=received||count_!=count)
   throw std::logic_error("conflicting install retry");
  return true;
 }
 if(gate_.phase()!=MigrationGate::Phase::Running||actor_.running_||!actor_.buffer_.empty()||!context||count<0||ticket.empty())
  throw std::logic_error("target must be fresh and inactive");
 auto& op=*actor_.operator_();eligible(op);
 for(const auto& [edge,n]:received)if(n<0||!op.producers().count(edge))throw std::logic_error("invalid received channel");
 // A failed restore poisons this instance: controller must discard it.
 gate_.prepare(ticket,2);fingerprint_=fingerprint;
 try {
  restore(op,image,binding(op,fingerprint));
  auto ctx=std::make_shared<POpContext>(*context);ctx->operatorActor(&actor_);
  op.create(ctx);op.receivedTupleSetMap=received;op.receivedTupleSet=count;op.shiftReceivedInput(count>0);
  received_=received;count_=count;image_=image;
 }catch(...){gate_.abort_target(ticket);throw;}
 return true;
}
bool TapDBActorMigration::commit(const std::string& ticket,const caf::actor& target) {
 if(!target||target.address()==actor_.address())throw std::invalid_argument("invalid target actor");
 // Coordinator must have received a successful install response first.
 gate_.commit(ticket,std::to_string(target.id()) + "@" + caf::to_string(target.node()));target_=target;
 actor_.running_=false;
 actor_.operator_()->clear(); // Free native state; source retains only routing context.
 image_.clear();context_.reset();drain(true);return true;
}
bool TapDBActorMigration::activate(const std::string& ticket) {
 gate_.activate(ticket);actor_.running_=true;image_.clear();drain(false);return true;
}
bool TapDBActorMigration::abort(const std::string& ticket) {
 if(gate_.phase()==MigrationGate::Phase::Frozen){gate_.abort_source(ticket);image_.clear();context_.reset();drain(false);return true;}
 if(gate_.phase()==MigrationGate::Phase::Prepared){gate_.abort_target(ticket);actor_.operator_()->clear();image_.clear();queued_.clear();queued_bytes_=0;return true;}
 throw std::logic_error("abort forbidden after forwarding or activation");
}
void TapDBActorMigration::drain(bool forward) {
 replaying_=true;
 try {
  while(!queued_.empty()){
   auto item=std::move(queued_.front());queued_.pop_front();
   if(forward)actor_.send(target_,item.first);
   else{actor_.overriddenMessageSender_=item.second;actor_.call_handler(actor_.current_behavior(),item.first);}
  }
 }catch(...){replaying_=false;actor_.overriddenMessageSender_=std::nullopt;throw;}
 queued_bytes_=0;replaying_=false;actor_.overriddenMessageSender_=std::nullopt;
}
std::string TapDBActorMigration::status(const std::string& ticket)const {
 if(ticket!=gate_.ticket())return "OTHER";
 switch(gate_.phase()){
 case MigrationGate::Phase::Frozen:return "FROZEN";
 case MigrationGate::Phase::Prepared:return "PREPARED";
 case MigrationGate::Phase::Forwarding:return "COMMITTED";
 case MigrationGate::Phase::Active:return "ACTIVE";
 case MigrationGate::Phase::Aborted:return "ABORTED";
 default:return "RUNNING";
 }
}
}
