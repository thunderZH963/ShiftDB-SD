#include "shiftdb/planner.hpp"
#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <tuple>
namespace shiftdb {
namespace {
constexpr double kMinimumPressure = 1e-6;
void nonnegative(double value) {
 if(!std::isfinite(value)||value<0)
  throw std::invalid_argument("non-finite or negative cost");
}
double add_cost(double first,double second) {
 const double result=first+second;
 if(!std::isfinite(result)) throw std::overflow_error("cost overflow");
 return result;
}
void set_location(Plan& plan,const Move& move) {
 for(auto& op:plan.ops) if(op.id==move.op) {
  if(move.destination.empty()||(op.pinned&&op.owner!=move.destination)||
     !op.legal.count(move.destination)||!op.service_seconds.count(move.destination))
   throw std::invalid_argument("illegal placement");
  op.owner=move.destination;
  return;
 }
 throw std::invalid_argument("unknown operator");
}
void validate_demands(const Demands& demands) {
 for(const auto& [resource,value]:demands) {
  if(resource.empty()) throw std::invalid_argument("empty resource name");
  nonnegative(value);
 }
}
// Equal-cost moves must not depend on telemetry/candidate enumeration order.
bool prefer_tie(const Choice& candidate,const Choice& incumbent) {
 if(candidate.benefit!=incumbent.benefit) return candidate.benefit>incumbent.benefit;
 return std::tie(candidate.move.op,candidate.move.destination,
                 candidate.move.exposed_transition_seconds,candidate.move.additional_demand)<
        std::tie(incumbent.move.op,incumbent.move.destination,
                 incumbent.move.exposed_transition_seconds,incumbent.move.additional_demand);
}
}
void apply_placement(Plan& plan,const Move& move) {
 nonnegative(move.exposed_transition_seconds);
 validate_demands(move.additional_demand);
 Plan next=plan;
 set_location(next,move);
 remaining_seconds(next); // Missing directed paths/cycles cannot leave a partial update.
 plan=std::move(next);
}
double remaining_seconds(const Plan& plan) {
 std::map<std::string,const OperatorCost*> operators;
 std::map<std::string,std::vector<const EdgeCost*>> inputs,outputs;
 std::map<std::string,std::size_t> degree;
 for(const auto& op:plan.ops) {
  if(op.id.empty()||!operators.emplace(op.id,&op).second)
   throw std::invalid_argument("empty or duplicate operator");
  if(op.owner.empty()||!op.legal.count(op.owner))
   throw std::invalid_argument("illegal owner");
  for(const auto& location:op.legal)
   if(location.empty()) throw std::invalid_argument("empty legal location");
  for(const auto& [location,seconds]:op.service_seconds) {
   if(location.empty()) throw std::invalid_argument("empty estimate location");
   nonnegative(seconds);
  }
  if(!op.service_seconds.count(op.owner))
   throw std::invalid_argument("missing service estimate");
  degree.emplace(op.id,0);
 }
 for(const auto& [path,rate]:plan.bandwidth) {
  if(path.first.empty()||path.second.empty()||!std::isfinite(rate)||rate<=0)
   throw std::invalid_argument("invalid route throughput");
 }
 for(const auto& edge:plan.edges) {
  if(!operators.count(edge.from)||!operators.count(edge.to))
   throw std::invalid_argument("unknown edge endpoint");
  nonnegative(edge.bytes);
  inputs[edge.to].push_back(&edge);
  outputs[edge.from].push_back(&edge);
  ++degree.at(edge.to);
 }
 std::set<std::string> ready;
 for(const auto& [id,count]:degree) if(count==0) ready.insert(id);
 struct Timing { double start=0,finish=0; };
 std::map<std::string,Timing> timing;
 double result=0;
 while(!ready.empty()) {
  const std::string id=*ready.begin(); ready.erase(ready.begin());
  const auto& op=*operators.at(id);
  double start=0,stream_finish=0;
  for(const auto* edge:inputs[id]) {
   const auto& source=*operators.at(edge->from);
   double transfer=0;
   if(source.owner!=op.owner&&edge->bytes>0) {
    const auto route=plan.bandwidth.find({source.owner,op.owner});
    if(route==plan.bandwidth.end()) throw std::invalid_argument("missing route throughput");
    transfer=edge->bytes/route->second;
    if(!std::isfinite(transfer)) throw std::overflow_error("transfer cost overflow");
   }
   const auto upstream=timing.at(edge->from);
   const double arrival=add_cost(upstream.finish,transfer);
   if(edge->blocking) start=std::max(start,arrival);
   else {
    // A stream cannot begin before its producer passes its own blocking inputs.
    start=std::max(start,upstream.start);
    stream_finish=std::max(stream_finish,arrival);
   }
  }
  const double finish=std::max(add_cost(start,op.service_seconds.at(op.owner)),stream_finish);
  timing.emplace(id,Timing{start,finish});
  result=std::max(result,finish);
  for(const auto* edge:outputs[id])
   if(--degree.at(edge->to)==0) ready.insert(edge->to);
 }
 if(timing.size()!=operators.size()) throw std::invalid_argument("cyclic physical plan");
 return result;
}
std::optional<Choice> best_move(const Plan& plan,const std::vector<Move>& moves,
 const Demands& capacity,const Demands& used,double minimum_gain) {
 nonnegative(minimum_gain);
 validate_demands(capacity); validate_demands(used);
 const double base=remaining_seconds(plan);
 // A snapshot already overcommitted cannot authorize additional transition work.
 for(const auto& [resource,occupied]:used) {
  const auto cap=capacity.find(resource);
  if((cap==capacity.end()&&occupied>0)||(cap!=capacity.end()&&occupied>cap->second))
   return std::nullopt;
 }
 std::optional<Choice> best;
 double best_denominator=1;
 for(const auto& move:moves) {
  nonnegative(move.exposed_transition_seconds);
  validate_demands(move.additional_demand);
  const auto op=std::find_if(plan.ops.begin(),plan.ops.end(),[&](const auto& item){return item.id==move.op;});
  if(op==plan.ops.end()||op->owner==move.destination) continue;
  double pressure=0;
  bool feasible=true;
  for(const auto& [resource,demand]:move.additional_demand) {
   if(demand==0) continue;
   const auto cap=capacity.find(resource);
   const auto occupied=used.find(resource);
   const double committed=occupied==used.end()?0:occupied->second;
   // Subtraction avoids overflow in committed + demand near the numeric limit.
   if(cap==capacity.end()||demand>cap->second-committed) {feasible=false;break;}
   pressure=std::max(pressure,demand/cap->second);
  }
  if(!feasible) continue;
  Plan next=plan;
  double after;
  try {
   set_location(next,move);
   after=add_cost(remaining_seconds(next),move.exposed_transition_seconds);
  } catch(const std::invalid_argument&) {continue;}
    catch(const std::overflow_error&) {continue;}
  const double gain=base-after;
  if(gain<=minimum_gain) continue;
  const double denominator=std::max(kMinimumPressure,pressure);
  Choice candidate{move,base,after,gain,
    std::min(gain/denominator,std::numeric_limits<double>::max())};
  // Compare ratios after scaling gains, avoiding score overflow even on systems
  // where long double has the same range as double.
  const double scale=best?std::max(gain,best->benefit):gain;
  const double lhs=(gain/scale)*best_denominator;
  const double rhs=best?(best->benefit/scale)*denominator:0;
  if(!best||lhs>rhs||(lhs==rhs&&prefer_tie(candidate,*best))) {
   best=std::move(candidate); best_denominator=denominator;
  }
 }
 return best;
}
}
