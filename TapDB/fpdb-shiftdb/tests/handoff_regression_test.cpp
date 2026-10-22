#include "shiftdb/handoff.hpp"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <sys/wait.h>
#include <unistd.h>

using namespace shiftdb;
namespace {
unsigned checks=0;
#define CHECK(x) do { ++checks; if(!(x))throw std::runtime_error(std::string("line ")+std::to_string(__LINE__)+": "+#x); } while(false)
template<class F> void rejects(F action) {
 bool caught=false;try{action();}catch(const std::exception&){caught=true;}CHECK(caught);
}
struct Temp {
 std::string path;
 Temp(){char p[]="/tmp/shiftdb-regression-XXXXXX";int fd=mkstemp(p);if(fd<0)throw std::runtime_error("mkstemp");close(fd);path=p;}
 ~Temp(){unlink(path.c_str());}
};
void put(Image& image,size_t offset,uint64_t value) {
 CHECK(offset+8<=image.bytes.size());
 for(unsigned i=0;i<8;++i)image.bytes[offset+i]=uint8_t(value>>(i*8));
 image.checksum=checksum(image.bytes);
}
void output_delivery_precedes_cutover() {
 Temp file;OwnershipStore owners(file.path);owners.create("sort","S1");
 ResourceLedger resources({{"S2.memory",4096},{"path",1000}});
 StatePartition source(Kind::Sort),target(Kind::Sort);source.activate(1);
 CHECK(source.consume("input",1,1,{{1,4,1},{2,1,2},{3,3,3}})==Consume::Applied);
 source.finalize(1);
 auto first=source.next_sorted(1);CHECK(first&&first->first==0&&first->second.id==2);
 Handoff move("sort-move","sort","S2",source,target,owners,resources);
 CHECK(move.begin({{"S2.memory",4096},{"path",100}},0,100));
 CHECK(!move.cutover(1,1e9,0,0,1,1));
 CHECK(owners.can_publish("sort","S1",1));
 CHECK(source.progress().output_cursor==1&&source.progress().output_ack==0);
 CHECK(!source.acknowledge_output(2,1));
 CHECK(!source.acknowledge_output(1,2));
 CHECK(source.acknowledge_output(1,1));
 auto sequence=source.progress().log_sequence;
 CHECK(source.acknowledge_output(1,1));
 CHECK(source.progress().log_sequence==sequence);
 CHECK(move.catch_up(2));
 CHECK(move.cutover(3,1e9,0,0,1,1));
 CHECK(move.finished()&&source.retired());
 CHECK(owners.can_publish("sort","S2",2));
 CHECK(!owners.can_publish("sort","S1",1));
 rejects([&]{source.activate(1);});
 rejects([&]{source.activate(2);});
 CHECK(!target.acknowledge_output(1,1));
 auto second=target.next_sorted(2);CHECK(second&&second->first==1&&second->second.id==3);
 CHECK(target.acknowledge_output(2,2));
 auto third=target.next_sorted(2);CHECK(third&&third->first==2&&third->second.id==1);
 CHECK(!target.next_sorted(2));
 CHECK(resources.used().at("S2.memory")==4096);
 resources.release("sort-move");CHECK(resources.used().at("S2.memory")==0);
}
void replay_never_regresses_progress() {
 StatePartition source(Kind::Aggregate),target(Kind::Aggregate);source.activate(1);
 CHECK(source.consume("input",1,1,{{1,1,2}})==Consume::Applied);
 CHECK(source.consume("input",2,1,{{2,1,3}})==Consume::Applied);
 const auto base=source.snapshot("frontier");target.restore(base,"frontier");
 auto bad=base;put(bad,24,3); // log sequence
 // v2 header: 8 fixed fields, input-count, edge-length, edge, frontier.
 put(bad,80+std::string("input").size(),1);
 rejects([&]{target.apply({"frontier",3,bad});});
 CHECK(target.progress().log_sequence==2&&target.progress().input.at("input")==2);
 CHECK(target.groups().at(1).sum==5);

 StatePartition sort(Kind::Sort),restored(Kind::Sort);sort.activate(1);
 sort.consume("input",1,1,{{1,1,1},{2,2,2}});sort.finalize(1);sort.next_sorted(1);
 auto image=sort.snapshot("cursor");restored.restore(image,"cursor");
 auto regressed=image;put(regressed,24,4);put(regressed,32,0);
 rejects([&]{restored.apply({"cursor",4,regressed});});
 CHECK(restored.progress().output_cursor==1&&restored.progress().log_sequence==3);
 auto inconsistent=image;put(inconsistent,56,2); // ack cannot exceed cursor
 rejects([&]{StatePartition invalid(Kind::Sort);invalid.restore(inconsistent,"invalid");});
 StatePartition tiny(Kind::Aggregate,64);tiny.activate(1);
 rejects([&]{tiny.snapshot("oversize");});
 rejects([&]{tiny.finalize(1);});
 CHECK(tiny.progress().log_sequence==0);
}
void journal_history_and_ticket_identity() {
 Temp file;
 {
  OwnershipStore owner(file.path);owner.create("op","S1");
  CHECK(owner.fence("op",1,"old"));CHECK(owner.abort("op","old"));
  CHECK(owner.fence("op",1,"new"));CHECK(owner.commit("op",1,"new","S2"));
  CHECK(!owner.fence("op",2,"old"));
  CHECK(!owner.fence("op",2,"new"));
  CHECK(owner.fence("op",2,"third"));CHECK(!owner.commit("op",2,"third","S2"));
  CHECK(owner.abort("op","third"));
 }
 {
  OwnershipStore owner(file.path);CHECK(owner.read("op").generation==2);
  CHECK(!owner.fence("op",2,"old"));
  CHECK(!owner.fence("op",2,"new"));
  CHECK(!owner.fence("op",2,"third"));
  CHECK(owner.can_publish("op","S2",2));
 }
 // A valid checksum is insufficient: an old, correctly framed creation record
 // appended after a commit must not roll the owner back during recovery.
 std::ifstream input(file.path);std::string creation;std::getline(input,creation);input.close();
 {std::ofstream output(file.path,std::ios::app);output<<creation<<'\n';}
 rejects([&]{OwnershipStore corrupted(file.path);});
}
void restart_after_durable_commit() {
 Temp file;const auto child=fork();CHECK(child>=0);
 if(child==0) {
  try {
   OwnershipStore owner(file.path);owner.create("op","S1");
   if(!owner.fence("op",1,"crash")||!owner.commit("op",1,"crash","S2"))_exit(2);
   _exit(77); // no graceful teardown after durable ownership commit
  }catch(...){_exit(3);}
 }
 int status=0;CHECK(waitpid(child,&status,0)==child);
 CHECK(WIFEXITED(status)&&WEXITSTATUS(status)==77);
 OwnershipStore recovered(file.path);
 CHECK(recovered.read("op").generation==2&&recovered.read("op").route==2);
 CHECK(recovered.can_publish("op","S2",2));
 CHECK(!recovered.can_publish("op","S1",1));
 CHECK(recovered.commit("op",1,"crash","S2"));
 CHECK(!recovered.abort("op","crash"));
}
void begin_checks_generation_and_rolls_back() {
 Temp file;OwnershipStore owners(file.path);owners.create("op","S1");
 ResourceLedger resources({{"target",4096}});
 StatePartition source(Kind::Aggregate),target(Kind::Aggregate);source.activate(2);
 Handoff stale("stale","op","S2",source,target,owners,resources);
 CHECK(!stale.begin({{"target",100}},0,10));CHECK(resources.used().at("target")==0);
 StatePartition valid(Kind::Aggregate),wrong_kind(Kind::InnerJoin);valid.activate(1);
 CHECK(valid.consume("input",1,1,{{1,1,1}})==Consume::Applied);
 Handoff incompatible("kind","op","S2",valid,wrong_kind,owners,resources);
 rejects([&]{incompatible.begin({{"target",100}},0,10);});
 CHECK(resources.used().at("target")==0&&owners.can_publish("op","S1",1));
 CHECK(valid.consume("input",2,1,{{2,1,1}})==Consume::Applied);
 CHECK(valid.groups().at(1).sum==2);
}
}
int main() {
 try {
  output_delivery_precedes_cutover();replay_never_regresses_progress();
  journal_history_and_ticket_identity();restart_after_durable_commit();
  begin_checks_generation_and_rolls_back();
  std::cout<<"handoff regressions: "<<checks<<" checks passed\n";
 }catch(const std::exception& error){std::cerr<<error.what()<<'\n';return 1;}
}
