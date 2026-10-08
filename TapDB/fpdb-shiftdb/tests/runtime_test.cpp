#include "shiftdb/planner.hpp"
#include "shiftdb/handoff.hpp"
#include "shiftdb/tapdb_bridge.hpp"
#include <atomic>
#include <cmath>
#include <fstream>
#include <iostream>
#include <memory>
#include <thread>
#include <unistd.h>
using namespace shiftdb;
static int checks=0;
#define CHECK(x) do {++checks;if(!(x))throw std::runtime_error(std::string("check failed: ")+ #x +" line "+std::to_string(__LINE__));}while(false)
template<class F> void throws(F f){bool caught=false;try{f();}catch(const std::exception&){caught=true;}CHECK(caught);}
struct Temp {std::string path;Temp(){char p[]="/tmp/shiftdb-test-XXXXXX";int f=mkstemp(p);if(f<0)throw std::runtime_error("mkstemp");close(f);path=p;}~Temp(){unlink(path.c_str());}};
void planner_tests(){
 Plan p{{{"scan","S1",{"S1"},{{"S1",10}},true},{"join","S1",{"S1","S2","C"},{{"S1",20},{"S2",5},{"C",12}},false}},{{"scan","join",100,true}},{{{"S1","S2"},2},{{"S1","C"},100}}};
 CHECK(remaining_seconds(p)==30);
 auto c=best_move(p,{{"join","S2",1,{}},{"join","C",1,{{"C.mem",4}}}},{{"C.mem",10}},{},0);
 CHECK(c&&c->move.destination=="C"&&c->benefit==6);
 CHECK(!best_move(p,{{"join","S2",0,{}}},{},{},0)); // local CPU gain loses on edge
 CHECK(!best_move(p,{{"scan","S2",0,{}}},{},{},0));
 CHECK(!best_move(p,{{"join","C",1,{{"C.mem",11}}}},{{"C.mem",10}},{},0));
 apply_placement(p,c->move);CHECK(remaining_seconds(p)==23);
 p.edges.push_back({"join","scan",1,true});throws([&]{remaining_seconds(p);});
 Plan branch{{{"a","S1",{"S1","S2"},{{"S1",10},{"S2",1}},false},{"b","S1",{"S1"},{{"S1",20}},true}},{},{}};
 CHECK(!best_move(branch,{{"a","S2",0,{}}},{},{},0)); // noncritical branch
}
void resources_tests(){
 ResourceLedger r({{"mem",100},{"link",10}});
 CHECK(!r.reserve("bad",{{"mem",20},{"link",11}},0,0,10));CHECK(r.used()["mem"]==0);
 CHECK(r.reserve("a",{{"mem",20},{"link",8}},0,0,10));
 CHECK(r.reserve("a",{{"mem",20},{"link",8}},0,1,10)); // idempotent retry
 CHECK(!r.reserve("a",{{"mem",21}},0,1,10));
 CHECK(!r.reserve("b",{{"mem",1}},0,1,10)); // stale version
 CHECK(r.valid("a",9));CHECK(!r.valid("a",10));CHECK(r.used()["mem"]==0);
 CHECK(!r.reserve("a",{{"mem",20}},r.version(),11,10)); // ticket never reused
 CHECK(r.reserve("b",{{"mem",80}},r.version(),11,10));CHECK(r.commit("b",12));CHECK(r.valid("b",999));r.release("b");r.release("b");CHECK(r.used()["mem"]==0);
 CreditLedger c(100,1);auto a=c.admit(1,7,60);CHECK(a);CHECK(!c.admit(1,7,1));
 CHECK(c.add_ref(*a,1,"x"));CHECK(c.add_ref(*a,1,"y"));CHECK(!c.extend(*a,1,41));CHECK(c.extend(*a,1,40));
 c.complete(*a,1);c.complete(*a,1);CHECK(c.busy()==0&&c.live()==100);
 c.release_ref(*a,2,"x");c.release_ref(*a,1,"x");CHECK(c.live()==100);
 c.seal(*a,1);CHECK(!c.add_ref(*a,1,"z"));c.release_ref(*a,1,"y");c.release_ref(*a,1,"y");CHECK(c.live()==0&&c.route_refs(7)==0);
 auto empty=c.admit(2,8,1);CHECK(empty);c.seal(*empty,2);c.complete(*empty,2);CHECK(c.live()==0&&c.busy()==0);
 CreditLedger concurrent(1000,1000);std::vector<std::thread> workers;
 for(int i=0;i<8;++i)workers.emplace_back([&]{for(int j=0;j<1000;++j){auto id=concurrent.admit(1,1,1);if(id){concurrent.seal(*id,1);concurrent.complete(*id,1);}}});
 for(auto& t:workers)t.join();CHECK(concurrent.live()==0&&concurrent.busy()==0);
}
void state_tests(){
 StatePartition s(Kind::Aggregate),t(Kind::Aggregate);s.activate(1);
 CHECK(s.consume("input",1,1,{{1,7,4},{2,7,std::nullopt},{3,7,6}})==Consume::Applied);
 CHECK(s.consume("input",1,1,{{1,7,999}})==Consume::Duplicate);
 CHECK(s.consume("input",3,1,{})==Consume::Gap);
 CHECK(s.groups().at(7).sum==10&&s.groups().at(7).count==2);
 auto image=s.snapshot("a");t.restore(image,"a");
 CHECK(s.consume("input",2,1,{{4,7,3}})==Consume::Applied);
 CHECK(s.consume("input",3,1,{{5,9,10}})==Consume::Applied);
 auto updates=s.updates();CHECK(updates.size()==2&&s.dirty_bytes()>0);
 CHECK(t.apply(updates[1])==1);CHECK(t.apply(updates[0])==3);CHECK(t.apply(updates[0])==3);
 s.acknowledge(3);CHECK(s.dirty_bytes()==0);CHECK(t.groups().at(7).sum==13);
 auto corrupt=image;corrupt.bytes[0]^=1;throws([&]{StatePartition bad(Kind::Aggregate);bad.restore(corrupt,"a");});
 StatePartition tiny(Kind::Aggregate,64);tiny.activate(1);CHECK(tiny.consume("x",1,1,{{1,1,1}})==Consume::Wait);CHECK(tiny.groups().empty()&&tiny.progress().log_sequence==0);
 StatePartition bounded(Kind::Aggregate,4096,100);bounded.activate(1);bounded.consume("x",1,1,{{1,1,1}});bounded.snapshot("bounded");
 CHECK(bounded.consume("x",2,1,{{2,2,1}})==Consume::Wait);CHECK(bounded.progress().log_sequence==1);
 StatePartition overflow(Kind::Aggregate);overflow.activate(1);overflow.consume("x",1,1,{{1,1,INT64_MAX}});throws([&]{overflow.consume("x",2,1,{{2,1,1}});});CHECK(overflow.progress().log_sequence==1);
 StatePartition join(Kind::InnerJoin),join2(Kind::InnerJoin);join.activate(1);join.consume("build",1,1,{{1,4,1},{2,4,2}});join.consume("probe",1,1,{{9,4,0}});
 join2.restore(join.snapshot("join"),"join");join2.activate(2);join2.consume("probe",2,2,{{10,4,0}});CHECK(join2.matches().size()==4);throws([&]{join2.consume("build",2,2,{{3,4,1}});});
 StatePartition sort(Kind::Sort),sort2(Kind::Sort);sort.activate(1);sort.consume("input",1,1,{{1,9,1},{2,3,2},{3,3,std::nullopt}});sort.finalize(1);auto first=sort.next_sorted(1);CHECK(first&&first->first==0&&first->second.id==2);
 sort2.restore(sort.snapshot("sort"),"sort");sort2.activate(2);auto second=sort2.next_sorted(2);CHECK(second&&second->first==1&&second->second.id==3);CHECK(sort2.next_sorted(2)->second.id==1);CHECK(!sort2.next_sorted(2));
}
void ownership_tests(){
 Temp tmp;
 {OwnershipStore o(tmp.path);o.create("q/op","S1");CHECK(o.can_publish("q/op","S1",1));throws([&]{OwnershipStore duplicate(tmp.path);});
 CHECK(!o.fence("q/op",2,"x"));CHECK(o.fence("q/op",1,"x"));CHECK(!o.can_publish("q/op","S1",1));CHECK(!o.commit("q/op",1,"wrong","S2"));CHECK(o.commit("q/op",1,"x","S2"));CHECK(o.commit("q/op",1,"x","S2"));CHECK(!o.abort("q/op","x"));CHECK(o.can_publish("q/op","S2",2));}
 {OwnershipStore reopened(tmp.path);CHECK(reopened.read("q/op").generation==2);CHECK(!reopened.can_publish("q/op","S1",1));CHECK(reopened.fence("q/op",2,"y"));CHECK(reopened.abort("q/op","y"));CHECK(reopened.can_publish("q/op","S2",2));}
 {std::ofstream f(tmp.path,std::ios::app);f<<"partial record";}
 throws([&]{OwnershipStore bad(tmp.path);});
}
void handoff_tests(){
 auto pause=estimate_pause(1000,200,100,0.1,0.1);CHECK(!pause.converges&&std::abs(pause.seconds-10.2)<1e-8);
 auto run=[](bool lost_reply,bool stop_copy){
  Temp tmp;OwnershipStore owners(tmp.path);owners.create("q/agg","S1");ResourceLedger resources({{"target",4096},{"route",100}});
  StatePartition src(Kind::Aggregate),dst(Kind::Aggregate);src.activate(1);src.consume("input",1,1,{{1,3,8}});
  Handoff h("ticket","q/agg","S2",src,dst,owners,resources);
  CHECK(h.begin({{"target",4096},{"route",50}},0,100,stop_copy));
  Handoff duplicate("ticket","q/agg","S2",src,dst,owners,resources);
  CHECK(!duplicate.begin({{"target",4096},{"route",50}},0,100));CHECK(resources.used()["target"]==4096);
  if(!stop_copy){src.consume("input",2,1,{{2,3,7}});CHECK(h.catch_up(1));src.consume("input",3,1,{{3,3,9}});}
  CHECK(!h.cutover(2,1,.01,.01,.001,1)); // positive budget, transfer/drain cost too high for live updates (pre-copy)
  if(lost_reply){throws([&]{h.cutover(2,1e9,.01,.01,1,1,[]{throw std::runtime_error("lost reply");});});h.cancel();}
  else CHECK(h.cutover(2,1e9,.01,.01,1,1));
  CHECK(h.finished());CHECK(owners.can_publish("q/agg","S2",2));CHECK(!owners.can_publish("q/agg","S1",1));CHECK(src.consume("input",4,1,{})==Consume::Fenced);
  CHECK(dst.groups().at(3).sum==(stop_copy?8:24));CHECK(resources.used()["target"]==4096);
  resources.release("ticket");CHECK(resources.used()["target"]==0);
 };
 run(false,false);run(true,false);run(false,true);
 // Cancellation after expiration resumes the source and cannot release live source state.
 Temp tmp;OwnershipStore o(tmp.path);o.create("q/a","S1");ResourceLedger r({{"target",1000}});StatePartition s(Kind::Aggregate),d(Kind::Aggregate);s.activate(1);s.consume("x",1,1,{{1,1,3}});
 Handoff h("abort","q/a","S2",s,d,o,r);CHECK(h.begin({{"target",100}},0,2));CHECK(!h.catch_up(2));h.cancel();CHECK(o.can_publish("q/a","S1",1));CHECK(s.consume("x",2,1,{{2,1,4}})==Consume::Applied);CHECK(s.groups().at(1).sum==7);CHECK(r.used()["target"]==0);
}
struct FakeOp {std::string n;std::string& name(){return n;}std::string getTypeString(){return "Aggregate";}int getNodeId(){return 2;}bool spawnOnRemote(){return true;}std::set<std::string> producers(){return {"scan"};}std::set<std::string> consumers(){return {};}};
struct FakePlan {std::vector<std::shared_ptr<FakeOp>> ops;const auto& getPhysicalOps()const{return ops;}};
void bridge_tests(){FakePlan p{{std::make_shared<FakeOp>(FakeOp{"agg\"x"})}};auto r=tapdb::describe(p);CHECK(r.size()==1&&r[0].node_id==2&&!r[0].active_migration_supported);CHECK(tapdb::quote("a\nb\"")=="\"a\\u000ab\\\"\"");}
int main(int argc,char** argv){try{std::string suite=argc>1?argv[1]:"all";
 if(suite=="planner"||suite=="all")planner_tests();if(suite=="resources"||suite=="all")resources_tests();if(suite=="state"||suite=="all")state_tests();if(suite=="ownership"||suite=="all")ownership_tests();if(suite=="handoff"||suite=="all")handoff_tests();if(suite=="bridge"||suite=="all")bridge_tests();
 std::cout<<suite<<": "<<checks<<" checks passed\n";return 0;
 }catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
