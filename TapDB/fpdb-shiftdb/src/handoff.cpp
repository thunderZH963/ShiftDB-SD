#include "shiftdb/handoff.hpp"
#include <cmath>
#include <limits>
#include <stdexcept>
namespace shiftdb {
PauseEstimate estimate_pause(uint64_t dirty,double generated,double applied,double drain,double commit) {
 for(double n:{generated,applied,drain,commit})if(!std::isfinite(n)||n<0)throw std::invalid_argument("invalid handoff rate");
 if(applied<=0)return {std::numeric_limits<double>::infinity(),false,std::numeric_limits<double>::infinity()};
 return {drain+dirty/applied+commit,applied>generated,applied>generated?dirty/(applied-generated):std::numeric_limits<double>::infinity()};
}
Handoff::Handoff(std::string ticket,std::string op,std::string destination,StatePartition& src,
 StatePartition& dst,OwnershipStore& owners,ResourceLedger& resources):ticket_(std::move(ticket)),op_(std::move(op)),destination_(std::move(destination)),source_(src),target_(dst),owners_(owners),resources_(resources) {
 if(&src==&dst||ticket_.empty())throw std::invalid_argument("invalid handoff endpoints");
}
bool Handoff::begin(const Demands& demand,uint64_t now,uint64_t ttl,bool stop_copy) {
 if(started_)throw std::logic_error("ticket already started");
 // A new controller object must not take ownership of another active ticket.
 if(resources_.valid(ticket_,now))return false;
 auto owner=owners_.read(op_);if(owner.fenced||owner.node==destination_)return false;
 generation_=owner.generation;source_node_=owner.node;
 if(!resources_.reserve(ticket_,demand,resources_.version(),now,ttl))return false;
 bool snapshot_started=false;
 try {
  auto base=source_.snapshot(ticket_);snapshot_started=true;
  if(stop_copy)source_.pause();
  target_.restore(base,ticket_);started_=true;return true;
 } catch(...) {
  resources_.release(ticket_);if(snapshot_started){source_.stop_logging();source_.activate(generation_); }throw;
 }
}
bool Handoff::catch_up(uint64_t now) {
 if(!started_||finished_||!resources_.valid(ticket_,now))return false;
 for(const auto& d:source_.updates())source_.acknowledge(target_.apply(d));
 return true;
}
bool Handoff::cutover(uint64_t now,double applied,double drain,double commit_cost,double limit,double gain,const std::function<void()>& after_commit) {
 if(!started_||finished_||!resources_.valid(ticket_,now))return false;
 if(!std::isfinite(limit)||limit<0||!std::isfinite(gain)||gain<=0)return false;
 if(estimate_pause(source_.dirty_bytes(),0,applied,drain,commit_cost).seconds>limit)return false;
 auto final=source_.freeze();
 if(estimate_pause(source_.dirty_bytes(),0,applied,drain,commit_cost).seconds>limit){source_.activate(generation_);return false;}
 if(!catch_up(now)){source_.activate(generation_);return false;}
 auto installed=target_.progress();
 if(installed.log_sequence!=final.log_sequence||installed.input!=final.input){source_.activate(generation_);return false;}
 // Convert preparation to a non-expiring hold before durable ownership changes.
 // It remains charged until target execution releases it, including on UNKNOWN.
 if(!resources_.commit(ticket_,now)){source_.activate(generation_);return false;}
 if(!owners_.fence(op_,generation_,ticket_)){cancel();return false;}
 if(!owners_.commit(op_,generation_,ticket_,destination_)){cancel();return false;}
 if(after_commit)after_commit(); // fault injection: reply can be lost after fsync
 auto committed=owners_.read(op_);target_.activate(committed.generation);
 source_.retire();finished_=true;return true;
}
void Handoff::cancel() {
 if(!started_||finished_)return;
 auto record=owners_.read(op_); // may throw; uncertainty retains fences and holds
 if(record.ticket==ticket_&&record.decision=="COMMITTED") {
  target_.activate(record.generation);source_.retire();finished_=true;return;
 }
 if(record.generation!=generation_||record.node!=source_node_)throw std::runtime_error("owner changed outside handoff");
 if(record.fenced&&!owners_.abort(op_,ticket_))throw std::runtime_error("cannot resolve abort");
 target_.retire();source_.stop_logging();source_.activate(generation_);resources_.release(ticket_);finished_=true;
}
}
