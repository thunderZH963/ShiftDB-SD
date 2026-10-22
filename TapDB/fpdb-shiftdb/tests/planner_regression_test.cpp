#include "shiftdb/planner.hpp"
#include <algorithm>
#include <cmath>
#include <iostream>
#include <limits>
#include <stdexcept>
using namespace shiftdb;
namespace {
int checks=0;
#define CHECK(condition) do { ++checks; if(!(condition)) throw std::runtime_error(std::string("check failed: ")+ #condition +" line "+std::to_string(__LINE__)); } while(false)
template<class Exception=std::invalid_argument,class Function> void rejects(Function function) {
 bool caught=false;
 try { function(); } catch(const Exception&) { caught=true; }
 CHECK(caught);
}
OperatorCost op(const std::string& id,const std::string& owner,double seconds) {
 return {id,owner,{owner},{{owner,seconds}},false};
}
Plan movable() {
 return {{{"scan","S1",{"S1","S2","C"},{{"S1",100},{"S2",10},{"C",20}},false}}, {}, {}};
}
void dependencies() {
 Plan pipeline{{op("scan","S1",10),op("filter","S1",6)},{{"scan","filter",10,false}}, {}};
 CHECK(remaining_seconds(pipeline)==10); // Streaming work overlaps, rather than summing to 16.
 pipeline.edges.front().blocking=true;
 CHECK(remaining_seconds(pipeline)==16);
 Plan gated{{op("scan","S1",100),op("build","S1",10),op("probe","S1",90)},
            {{"scan","build",0,true},{"build","probe",0,false}}, {}};
 CHECK(remaining_seconds(gated)==190); // Probe cannot start before the build's input gate opens.
 gated.edges.back().blocking=true;
 CHECK(remaining_seconds(gated)==200);
 Plan mixed{{op("a","S1",10),op("b","S1",35),op("merge","S1",20)},
            {{"a","merge",0,true},{"b","merge",0,false}}, {}};
 CHECK(remaining_seconds(mixed)==35); // Required blocking input and streaming tail both matter.
 Plan branch=movable(); branch.ops.push_back(op("critical","S1",100));
 CHECK(!best_move(branch,{{"scan","S2",0,{}}},{},{}));
 // Iterative propagation also handles a deep DAG without recursive stack growth.
 Plan deep;
 for(int index=0;index<5000;++index) {
  deep.ops.push_back(op(std::to_string(index),"S1",1));
  if(index) deep.edges.push_back({std::to_string(index-1),std::to_string(index),0,true});
 }
 CHECK(remaining_seconds(deep)==5000);
}
void routes_and_legality() {
 Plan plan{{op("source","S1",10),{"sink","S1",{"S1","S2","C"},{{"S1",20},{"S2",1},{"C",5}},false}},
           {{"source","sink",100,true}}, {{{"S1","C"},100}}};
 const auto choice=best_move(plan,{{"sink","S2",0,{}},{"sink","C",0,{}}},{},{});
 CHECK(choice&&choice->move.destination=="C"&&choice->after==16);
 rejects([&]{apply_placement(plan,{"sink","S2",0,{}});});
 CHECK(plan.ops.back().owner=="S1"); // Failed apply is atomic.
 plan.bandwidth[ {"S2","S1"} ]=100;
 CHECK(!best_move(plan,{{"sink","S2",0,{}}},{},{})); // Reverse link does not imply a forward route.
 plan.bandwidth[ {"S1","S2"} ]=100;
 CHECK(best_move(plan,{{"sink","S2",0,{}}},{},{})->after==12);
 // Output edges are re-scored as well as the moved operator's inputs.
 plan.ops.push_back(op("consumer","S1",1)); plan.edges.push_back({"sink","consumer",50,true});
 plan.bandwidth.erase({"S2","S1"});
 CHECK(!best_move(plan,{{"sink","S2",0,{}}},{},{}));
 rejects([&]{apply_placement(plan,{"unknown","C",0,{}});});
 rejects([&]{apply_placement(plan,{"sink","unlisted",0,{}});});
 auto scan=movable();
 apply_placement(scan,{"scan","S2",0,{}}); CHECK(scan.ops.front().owner=="S2"); // No scan-specific pin.
 scan.ops.front().pinned=true;
 rejects([&]{apply_placement(scan,{"scan","C",0,{}});});
 apply_placement(scan,{"scan","S2",0,{}}); // Idempotent Keep is legal even for a pinned adapter.
 CHECK(!best_move(scan,{{"scan","S2",0,{}},{"scan","C",0,{}}},{},{}));
}
void resources_and_benefit() {
 auto plan=movable();
 const std::vector<Move> candidates{{"scan","S2",0,{{"mem",8}}},{"scan","C",0,{{"mem",1}}}};
 auto choice=best_move(plan,candidates,{{"mem",100}},{});
 CHECK(choice&&choice->move.destination=="C"&&choice->benefit==80); // Benefit per largest normalized increase.
 CHECK(best_move(plan,candidates,{{"mem",100}},{{"mem",95}})->move.destination=="C");
 CHECK(!best_move(plan,candidates,{{"mem",100}},{{"mem",100}}));
 CHECK(!best_move(plan,{{"scan","S2",0,{}}},{{"mem",100}},{{"mem",101}}));
 CHECK(!best_move(plan,{{"scan","S2",0,{}}},{},{{"unknown",1}}));
 CHECK(!best_move(plan,{{"scan","S2",0,{{"missing",1}}}},{},{}));
 CHECK(best_move(plan,{{"scan","S2",0,{{"zero",0}}}},{{"zero",0}},{}));
 CHECK(!best_move(plan,{{"scan","S2",90,{}}},{},{})); // Transition cost consumes the entire benefit.
 CHECK(!best_move(plan,{{"scan","S2",0,{}}},{},{},90));
 CHECK(best_move(plan,{{"scan","S2",1,{}}},{},{},88)->benefit==89);
 // Moving to the same owner is Keep, not another placement decision.
 CHECK(!best_move(plan,{{"scan","S1",0,{}}},{},{}));
 // One expensive resource dominates the normalized pressure even if another is cheap.
 auto ranked=best_move(plan,{{"scan","S2",0,{{"mem",1},{"link",9}}},{"scan","C",0,{{"mem",2},{"link",2}}}},
                       {{"mem",100},{"link",10}},{});
 CHECK(ranked&&ranked->move.destination=="C");
 std::vector<Move> ties{{"scan","S2",0,{{"mem",1}}},{"scan","C",0,{{"mem",1}}}};
 plan.ops.front().service_seconds["C"]=10;
 CHECK(best_move(plan,ties,{{"mem",10}},{})->move.destination=="C");
 std::reverse(ties.begin(),ties.end());
 CHECK(best_move(plan,ties,{{"mem",10}},{})->move.destination=="C");
}
void malformed_inputs() {
 const double nan=std::numeric_limits<double>::quiet_NaN();
 const double infinity=std::numeric_limits<double>::infinity();
 auto plan=movable();
 rejects([&]{best_move(plan,{},{{"mem",nan}},{});});
 rejects([&]{best_move(plan,{},{{"mem",10}},{{"unrelated",infinity}});});
 rejects([&]{best_move(plan,{},{{"mem",-1}},{});});
 rejects([&]{best_move(plan,{},{{"",1}},{});});
 rejects([&]{best_move(plan,{{"scan","S2",nan,{}}},{},{});});
 rejects([&]{best_move(plan,{{"scan","S2",0,{{"mem",nan}}}},{{"mem",10}},{});});
 rejects([&]{best_move(plan,{}, {}, {},infinity);});
 rejects([&]{apply_placement(plan,{"scan","S2",-1,{}});});
 CHECK(plan.ops.front().owner=="S1");
 plan.ops.front().service_seconds["C"]=nan;
 rejects([&]{remaining_seconds(plan);});
 plan=movable(); plan.bandwidth[{"unused","link"}]=0;
 rejects([&]{remaining_seconds(plan);});
 plan=movable(); plan.ops.push_back(plan.ops.front());
 rejects([&]{remaining_seconds(plan);});
 plan=movable(); plan.edges.push_back({"scan","missing",1,true});
 rejects([&]{remaining_seconds(plan);});
 plan=movable(); plan.edges.push_back({"scan","scan",1,true});
 rejects([&]{remaining_seconds(plan);});
 plan=movable(); plan.ops.front().owner="unknown";
 rejects([&]{remaining_seconds(plan);});
 plan=movable(); plan.edges.push_back({"scan","scan",nan,false});
 rejects([&]{remaining_seconds(plan);});
 CHECK(remaining_seconds({})==0);
}
void numerical_limits() {
 const double limit=std::numeric_limits<double>::max();
 Plan plan{{op("a","S1",limit),op("b","S1",limit)},{{"a","b",0,true}}, {}};
 rejects<std::overflow_error>([&]{remaining_seconds(plan);});
 plan={{op("a","S1",1),op("b","S2",1)},{{"a","b",limit,true}},{{{"S1","S2"},1e-300}}};
 rejects<std::overflow_error>([&]{remaining_seconds(plan);});
 auto candidate=movable(); candidate.ops.front().service_seconds["S2"]=limit;
 auto best=best_move(candidate,{{"scan","S2",limit,{}},{"scan","C",0,{}}},{},{});
 CHECK(best&&best->move.destination=="C"); // An overflowing hypothetical continuation cannot abort other choices.
 candidate=movable(); candidate.ops.front().service_seconds["S1"]=limit;
 best=best_move(candidate,{{"scan","S2",0,{}}},{},{});
 CHECK(best&&std::isfinite(best->score)); // Ranking remains finite when the score saturates double.
 best=best_move(candidate,{{"scan","C",0,{{"mem",0.2}}},{"scan","S2",0,{{"mem",0.1}}}},{{"mem",1}},{});
 CHECK(best&&best->move.destination=="S2"); // Both exposed scores saturate, but their order is still meaningful.
 CHECK(!best_move(movable(),{{"scan","S2",0,{{"mem",limit}}}},{{"mem",limit}},{{"mem",limit}}));
}
}
int main() {
 try {
  dependencies(); routes_and_legality(); resources_and_benefit(); malformed_inputs(); numerical_limits();
  std::cout<<"planner regressions: "<<checks<<" checks passed\n";
  return 0;
 } catch(const std::exception& error) {std::cerr<<error.what()<<'\n';return 1;}
}
