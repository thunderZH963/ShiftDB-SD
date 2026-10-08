#include "shiftdb/planner.hpp"
#include <algorithm>
#include <cmath>
#include <functional>
#include <stdexcept>
namespace shiftdb {
static void nonnegative(double x) {
 if(!std::isfinite(x)||x<0) throw std::invalid_argument("non-finite or negative cost");
}
void apply_placement(Plan& p,const Move& m) {
 for(auto& o:p.ops) if(o.id==m.op) {
  if(o.pinned||!o.legal.count(m.destination)||!o.service_seconds.count(m.destination))
   throw std::invalid_argument("illegal placement");
  o.owner=m.destination; return;
 }
 throw std::invalid_argument("unknown operator");
}
double remaining_seconds(const Plan& p) {
 std::map<std::string,const OperatorCost*> ops;
 for(const auto& o:p.ops) {
  if(o.id.empty()||!ops.emplace(o.id,&o).second) throw std::invalid_argument("duplicate operator");
  if(!o.legal.count(o.owner)) throw std::invalid_argument("illegal owner");
 }
 std::map<std::string,std::vector<const EdgeCost*>> input;
 for(const auto& e:p.edges) {
  if(!ops.count(e.from)||!ops.count(e.to)) throw std::invalid_argument("unknown edge endpoint");
  nonnegative(e.bytes); input[e.to].push_back(&e);
 }
 std::map<std::string,int> color;
 std::map<std::string,double> done;
 std::function<double(const std::string&)> visit=[&](const std::string& id)->double {
  if(color[id]==1) throw std::invalid_argument("cyclic physical plan");
  if(color[id]==2) return done.at(id);
  color[id]=1; const auto& o=*ops.at(id);
  auto svc=o.service_seconds.find(o.owner);
  if(svc==o.service_seconds.end()) throw std::invalid_argument("missing service estimate");
  nonnegative(svc->second);
  double blocking=0,streaming=0;
  for(auto e:input[id]) {
   auto src=ops.at(e->from)->owner; double transfer=0;
   if(src!=o.owner && e->bytes>0) {
    auto rate=p.bandwidth.find({src,o.owner});
    if(rate==p.bandwidth.end()||!std::isfinite(rate->second)||rate->second<=0)
     throw std::invalid_argument("missing route throughput");
    transfer=e->bytes/rate->second;
   }
   auto arrival=visit(e->from)+transfer;
   if(e->blocking) blocking=std::max(blocking,arrival);
   else streaming=std::max(streaming,arrival);
  }
  color[id]=2; return done[id]=std::max(blocking+svc->second,streaming);
 };
 double result=0; for(const auto& o:p.ops) result=std::max(result,visit(o.id));
 if(!std::isfinite(result)) throw std::overflow_error("cost overflow");
 return result;
}
std::optional<Choice> best_move(const Plan& p,const std::vector<Move>& moves,
 const Demands& capacity,const Demands& used,double minimum_gain) {
 nonnegative(minimum_gain); const double base=remaining_seconds(p);
 std::optional<Choice> best;
 for(const auto& m:moves) {
  nonnegative(m.exposed_transition_seconds); double pressure=0; bool feasible=true;
  for(const auto& [r,d]:m.additional_demand) {
   nonnegative(d); auto cap=capacity.find(r); auto u=used.find(r);
   double occupied=u==used.end()?0:u->second;
   nonnegative(occupied);
   if(cap==capacity.end()||!std::isfinite(cap->second)||cap->second<0||occupied+d>cap->second) {feasible=false;break;}
   if(d>0) pressure=std::max(pressure,d/cap->second);
  }
  if(!feasible) continue;
  Plan next=p;
  try { apply_placement(next,m); }
  catch(const std::invalid_argument&) {continue;}
  double after;
  try {after=remaining_seconds(next)+m.exposed_transition_seconds;}
  catch(const std::invalid_argument&) {continue;}
  const double gain=base-after;
  if(gain<=minimum_gain) continue;
  Choice c{m,base,after,gain,gain/std::max(1e-6,pressure)};
  if(!best||c.score>best->score) best=c;
 }
 return best;
}
}
