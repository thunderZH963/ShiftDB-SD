#pragma once
#include "state.hpp"
#include "ownership.hpp"
#include "resources.hpp"
#include <functional>
namespace shiftdb {
struct PauseEstimate { double seconds=0; bool converges=false; double catchup_seconds=0; };
PauseEstimate estimate_pause(uint64_t dirty,double dirty_rate,double applied_rate,double drain,double commit);
class Handoff {
 std::string ticket_,op_,source_node_,destination_;
 StatePartition &source_,&target_;
 OwnershipStore& owners_;ResourceLedger& resources_;
 uint64_t generation_=0;bool started_=false,finished_=false;
public:
 Handoff(std::string ticket,std::string op,std::string destination,StatePartition& source,
         StatePartition& target,OwnershipStore&,ResourceLedger&);
 bool begin(const Demands&,uint64_t now,uint64_t ttl,bool stop_copy=false);
 bool catch_up(uint64_t now);
 // Caller supplies current estimates. False leaves the source resumable, not
 // silently migrated. Invoke cancel() to return holds after a rejected cutover.
 bool cutover(uint64_t now,double applied_rate,double drain,double commit_cost,
              double pause_limit,double net_benefit,
              const std::function<void()>& after_durable_commit={});
 void cancel();
 bool finished()const{return finished_;}
};
}
