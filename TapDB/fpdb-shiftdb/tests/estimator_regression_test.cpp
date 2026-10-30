#include "shiftdb/estimator.hpp"
#include <cmath>
#include <iostream>
#include <limits>
#include <stdexcept>
using namespace shiftdb;
namespace {
int checks=0;
#define CHECK(x) do {++checks;if(!(x))throw std::runtime_error(#x);}while(false)
template<class E=std::invalid_argument,class F>void rejects(F f){bool caught=false;try{f();}catch(const E&){caught=true;}CHECK(caught);}
bool near(double a,double b){return std::abs(a-b)<1e-8;}
void windows() {
 RateEstimator e;
 CHECK(!e.rate("absent",0));
 rejects([&]{e.calibrate("a",{1,2},0);});
 e.calibrate("a",{500,100,300,200,400},0);
 CHECK(*e.rate("a",0)==300);
 e.observe("a",10,.1,10);e.observe("a",30,.1,50);
 CHECK(*e.rate("a",99)==300);
 CHECK(near(*e.rate("a",100),280)); // Ratio of summed work/time, then EWMA.
 CHECK(near(*e.rate("a",400),280)); // Empty windows do not decay.
 CHECK(!e.rate("a",551)); // Last real observation was 50, not read at 400.
 e.observe("a",100,.1,560);
 CHECK(!e.rate("a",560)); // Unfinished window must not refresh stale estimate.
 CHECK(near(*e.rate("a",600),424));
 rejects([&]{e.observe("a",1,.1,599);});
 rejects([&]{e.observe("a",-1,.1,601);});
 rejects([&]{e.observe("a",1,0,601);});
 rejects([&]{e.calibrate("a",{1,2,3,4,INFINITY},601);});
 RateEstimator first;
 first.observe("x",10,.2,0);CHECK(!first.rate("x",99));CHECK(*first.rate("x",100)==50);
 first.observe("x",0,.1,110);CHECK(near(*first.rate("x",200),40));
 rejects([]{RateEstimator bad({0,500,.2});});
 rejects([]{RateEstimator bad({100,500,NAN});});
}
void plans() {
 RateEstimator e;
 e.calibrate("slow",{10,10,10,10,10},0);
 e.calibrate("fast",{100,100,100,100,100},0);
 Plan base{{{"agg","S1",{"S1","S2","C"},{{"S1",99}},false}}, {}, {}};
 std::vector<WorkEstimate> w{{"agg",1000,{{"S1","slow"},{"S2","fast"},{"C","missing"}}}};
 auto p=estimate_plan(base,w,{},e,100);CHECK(p);CHECK(remaining_seconds(*p)==100);
 CHECK(!p->ops[0].service_seconds.count("C"));
 auto choice=best_move(*p,{{"agg","S2",1,{}},{"agg","C",0,{}}},{},{});
 CHECK(choice&&choice->move.destination=="S2"&&choice->benefit==89);
 CHECK(base.ops[0].service_seconds.at("S1")==99); // Input plan is unchanged.
 CHECK(!estimate_plan(base,w,{},e,501));
 rejects([&]{estimate_plan(base,{}, {},e,501);});
 RateEstimator links;
 for(auto key:{"slow","fast","link"})links.calibrate(key,{10,10,10,10,10},0);
 Plan pair=base;pair.ops.push_back({"sink","S2",{"S2"},{{"S2",1}},false});
 pair.edges.push_back({"agg","sink",100,true});
 w.push_back({"sink",10,{{"S2","fast"}}});
 CHECK(!estimate_plan(pair,w,{},links,100));
 auto connected=estimate_plan(pair,w,{{{"S1","S2"},"link"}},links,100);
 CHECK(connected&&remaining_seconds(*connected)==111);
 CHECK(!estimate_plan(pair,w,{{{"S2","S1"},"link"}},links,100));
}
void memory() {
 CHECK(hash_build_memory(10,17,8,16,8,100)==468);
 CHECK(aggregation_memory(10,24,100,50)==390);
 CHECK(sort_memory(10,24,8,100)==420);
 CHECK((RestoreMemory{100,20,30,40}.peak())==190);
 const auto max=std::numeric_limits<uint64_t>::max();
 rejects<std::overflow_error>([&]{hash_build_memory(max,8,8,1,8,0);});
 rejects<std::overflow_error>([&]{hash_build_memory(1,max,8,0,0,0);});
 rejects<std::overflow_error>([&]{aggregation_memory(1,max,1);});
 rejects<std::overflow_error>([&]{sort_memory(1,max,1,0);});
 rejects<std::overflow_error>([&]{RestoreMemory{max,1,0,0}.peak();});
 rejects([&]{hash_build_memory(1,8,3,0,0,0);});
 CHECK(hash_build_memory(0,0,8,0,0,0)==0);
}
}
int main(){try{windows();plans();memory();std::cout<<checks<<" estimator checks passed\n";}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
