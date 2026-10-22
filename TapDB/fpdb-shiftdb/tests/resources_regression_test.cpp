#include "shiftdb/resources.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <thread>
#include <vector>
using namespace shiftdb;
static unsigned checks=0;
#define CHECK(expr) do { ++checks; if(!(expr)) throw std::runtime_error( \
 std::string("check failed: ")+#expr+" line "+std::to_string(__LINE__)); } while(false)
template<class F> void invalid(F f) {
 bool threw=false;try {f();}catch(const std::invalid_argument&) {threw=true;}CHECK(threw);
}
void atomic_reservations() {
 ResourceLedger resources({{"cpu",8},{"memory",100}});
 const auto initial=resources.used();
 CHECK(!resources.reserve("unknown",{{"cpu",3},{"network",1}},0,0,10));
 CHECK(resources.used()==initial&&resources.version()==0);
 CHECK(!resources.reserve("excess",{{"cpu",3},{"memory",101}},0,0,10));
 CHECK(resources.used()==initial&&resources.version()==0);
 const auto nan=std::numeric_limits<double>::quiet_NaN();
 const auto inf=std::numeric_limits<double>::infinity();
 invalid([&]{resources.reserve("nan",{{"cpu",nan}},0,0,10);});
 invalid([&]{resources.reserve("inf",{{"memory",inf}},0,0,10);});
 invalid([&]{resources.reserve("negative",{{"cpu",-1}},0,0,10);});
 invalid([&]{resources.reserve("empty-resource",{{"",0}},0,0,10);});
 invalid([&]{resources.reserve("zero-ttl",{{"cpu",1}},0,0,0);});
 invalid([&]{resources.reserve("overflow-ttl",{{"cpu",1}},0,UINT64_MAX,1);});
 CHECK(resources.used()==initial&&resources.version()==0);
 invalid([&]{ResourceLedger invalid_capacity({{"cpu",inf}});});
 CHECK(resources.reserve("a",{{"cpu",3},{"memory",60}},0,0,10));
 const auto held=resources.used();const auto version=resources.version();
 CHECK(resources.reserve("a",{{"cpu",3},{"memory",60}},0,1,100));
 CHECK(resources.used()==held&&resources.version()==version);
 CHECK(!resources.reserve("a",{{"cpu",4}},version,1,10));
 CHECK(!resources.reserve("stale",{{"cpu",1}},0,1,10));
 CHECK(resources.used()==held&&resources.version()==version);
 // A retry does not silently extend the lease; explicit renewal is required.
 CHECK(!resources.valid("a",10));CHECK(resources.used()==initial);
 CHECK(!resources.reserve("a",{{"cpu",3}},resources.version(),10,10));
 CHECK(resources.reserve("b",{{"cpu",8},{"memory",100}},resources.version(),10,10));
 CHECK(resources.renew("b",19,10));CHECK(resources.valid("b",20));
 CHECK(resources.commit("b",28));CHECK(resources.valid("b",UINT64_MAX));
 resources.release("b");resources.release("b");
 CHECK(resources.used()==initial);
 // Retirement must not accumulate fractional residue across ticket churn.
 ResourceLedger fractions({{"memory",1}});
 CHECK(fractions.reserve("x",{{"memory",.1}},0,0,10));
 CHECK(fractions.reserve("y",{{"memory",.2}},fractions.version(),0,10));
 fractions.release("x");fractions.release("y");
 CHECK(fractions.used().at("memory")==0);
 CHECK(fractions.reserve("full",{{"memory",1}},fractions.version(),0,10));
}
void lifecycle_permutations() {
 // Every ordering of completion, sealing, and last-consumer release is valid.
 std::array<int,3> events{{0,1,2}};
 do {
  CreditLedger ledger(100,1);auto id=ledger.admit(4,9,100);CHECK(id);
  CHECK(ledger.add_ref(*id,4,"consumer"));
  bool completed=false;
  for(size_t step=0;step<events.size();++step) {
   switch(events[step]) {
    case 0: ledger.complete(*id,4);completed=true;break;
    case 1: ledger.seal(*id,4);break;
    case 2: ledger.release_ref(*id,4,"consumer");break;
   }
   CHECK(ledger.busy()==(completed?0:1));
   CHECK(ledger.live()==(step==2?0:100));
   CHECK(ledger.route_refs(9)==(step==2?0:1));
  }
  // Replayed events after collection do not affect a new allocation.
  auto next=ledger.admit(5,9,100);CHECK(next&&*next!=*id);
  ledger.complete(*id,4);ledger.seal(*id,4);ledger.release_ref(*id,4,"consumer");
  CHECK(ledger.live()==100&&ledger.busy()==1&&ledger.route_refs(9)==1);
  ledger.seal(*next,5);CHECK(ledger.live()==100);
  ledger.complete(*next,5);CHECK(ledger.live()==0&&ledger.busy()==0);
 } while(std::next_permutation(events.begin(),events.end()));
}
void replay_and_generation_checks() {
 CreditLedger ledger(100,2);auto id=ledger.admit(6,12,40);CHECK(id);
 ledger.release_ref(*id,5,"early"); // Stale events cannot poison a later valid reference.
 CHECK(ledger.add_ref(*id,6,"early"));
 CHECK(!ledger.add_ref(*id,6,"early"));
 ledger.release_ref(*id,6,"early");ledger.release_ref(*id,6,"early");
 CHECK(!ledger.add_ref(*id,6,"early")); // Publication retry cannot resurrect a released reference.
 ledger.release_ref(*id,6,"overtaken");
 CHECK(!ledger.add_ref(*id,6,"overtaken")); // Release overtook publication registration.
 CHECK(!ledger.add_ref(*id,6,""));
 CHECK(!ledger.add_ref(*id,7,"wrong-generation"));
 CHECK(!ledger.extend(*id,7,1));
 ledger.complete(*id,7);ledger.seal(*id,7);
 CHECK(ledger.busy()==1&&ledger.live()==40);
 CHECK(ledger.extend(*id,6,60));CHECK(!ledger.extend(*id,6,1));
 CHECK(ledger.live()==100);
 ledger.seal(*id,6);ledger.seal(*id,6);
 CHECK(ledger.live()==100&&ledger.busy()==1&&ledger.route_refs(12)==1);
 CHECK(!ledger.extend(*id,6,0));CHECK(!ledger.add_ref(*id,6,"after-seal"));
 ledger.complete(*id,6);ledger.complete(*id,6);
 CHECK(ledger.live()==0&&ledger.busy()==0&&ledger.route_refs(12)==0);
 // Execution completion returns the slot, while a retained output holds capacity and its route.
 auto retained=ledger.admit(7,13,80);CHECK(retained);
 CHECK(ledger.add_ref(*retained,7,"downstream"));ledger.complete(*retained,7);ledger.seal(*retained,7);
 CHECK(ledger.busy()==0&&ledger.live()==80&&ledger.route_refs(13)==1);
 auto other=ledger.admit(7,13,20);CHECK(other);CHECK(!ledger.admit(7,13,1));
 ledger.complete(*other,7);ledger.seal(*other,7);
 CHECK(ledger.live()==80&&ledger.route_refs(13)==1);
 ledger.release_ref(*retained,7,"downstream");CHECK(ledger.live()==0&&ledger.route_refs(13)==0);
 CreditLedger maximum(UINT64_MAX,1);auto huge=maximum.admit(1,1,UINT64_MAX-1);CHECK(huge);
 CHECK(maximum.extend(*huge,1,1));CHECK(!maximum.extend(*huge,1,UINT64_MAX));
 CHECK(maximum.live()==UINT64_MAX);maximum.seal(*huge,1);maximum.complete(*huge,1);
 CHECK(maximum.live()==0&&maximum.busy()==0);
 CHECK(!maximum.admit(0,1,1));CHECK(!maximum.admit(1,0,1));
 CreditLedger no_slots(100,0);CHECK(!no_slots.admit(1,1,1));
}
void concurrent_admission() {
 ResourceLedger resources({{"memory",100},{"network",100}});
 std::atomic<unsigned> accepted{0};std::vector<std::thread> workers;
 for(unsigned worker=0;worker<16;++worker)workers.emplace_back([&,worker]{
  for(;;) {
   const auto version=resources.version();
   if(resources.reserve("worker-"+std::to_string(worker),{{"memory",13},{"network",13}},version,0,100)) {
    ++accepted;break;
   }
   if(resources.used().at("memory")+13>100)break;
  }
 });
 for(auto& thread:workers)thread.join();
 CHECK(accepted==7);CHECK(resources.used().at("memory")==91&&resources.used().at("network")==91);
 for(unsigned worker=0;worker<16;++worker)resources.release("worker-"+std::to_string(worker));
 CHECK(resources.used().at("memory")==0&&resources.used().at("network")==0);
 CreditLedger credits(64,64);workers.clear();
 std::atomic<bool> registration_failed{false};
 for(unsigned worker=0;worker<8;++worker)workers.emplace_back([&]{
  for(unsigned iteration=0;iteration<1000;++iteration) {
   auto id=credits.admit(1,3,1);if(!id)continue;
   if(!credits.add_ref(*id,1,"consumer"))registration_failed=true;
   // Alternate schedules exercise completed producers and early consumer release.
   if(iteration%2)credits.complete(*id,1);
   credits.release_ref(*id,1,"consumer");credits.seal(*id,1);credits.complete(*id,1);
  }
 });
 for(auto& thread:workers)thread.join();
 CHECK(!registration_failed);CHECK(credits.live()==0&&credits.busy()==0&&credits.route_refs(3)==0);
}
int main() {
 try {
  atomic_reservations();lifecycle_permutations();replay_and_generation_checks();concurrent_admission();
  std::cout<<"resources regressions: "<<checks<<" checks passed\n";return 0;
 } catch(const std::exception& e) {std::cerr<<e.what()<<'\n';return 1;}
}
