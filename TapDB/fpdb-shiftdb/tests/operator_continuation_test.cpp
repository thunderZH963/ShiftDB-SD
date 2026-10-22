#include "shiftdb/handoff.hpp"
#include <algorithm>
#include <iostream>
#include <set>
#include <stdexcept>
#include <unistd.h>
using namespace shiftdb;
namespace {
int checks=0;
#define CHECK(condition) do { ++checks; if(!(condition)) throw std::runtime_error(std::string("check failed: ")+ #condition +" line "+std::to_string(__LINE__)); } while(false)
struct TempJournal {
 std::string path;
 TempJournal() {
  char name[]="/tmp/shiftdb-continuation-XXXXXX";
  const int fd=mkstemp(name);
  if(fd<0) throw std::runtime_error("cannot create ownership journal");
  close(fd); path=name;
 }
 ~TempJournal(){unlink(path.c_str());}
};
template<class Function> void rejects(Function function) {
 bool caught=false;
 try {function();} catch(const std::exception&) {caught=true;}
 CHECK(caught);
}
void same_groups(const std::map<int64_t,Aggregate>& actual,const std::map<int64_t,Aggregate>& expected) {
 CHECK(actual.size()==expected.size());
 for(const auto& [key,value]:expected) {
  const auto found=actual.find(key); CHECK(found!=actual.end());
  const auto& observed=found->second;
  CHECK(observed.sum==value.sum); CHECK(observed.count==value.count);
  CHECK(observed.min==value.min); CHECK(observed.max==value.max);
 }
}
void same_progress(const Progress& actual,const Progress& expected) {
 CHECK(actual.log_sequence==expected.log_sequence); CHECK(actual.input==expected.input);
 CHECK(actual.output_cursor==expected.output_cursor); CHECK(actual.output_ack==expected.output_ack);
}
void check_committed(OwnershipStore& owners,ResourceLedger& resources,const std::string& op,
                     const std::string& target,const std::string& ticket,StatePartition& source) {
 const auto owner=owners.read(op);
 CHECK(owner.node==target); CHECK(owner.generation==2); CHECK(owner.route==2);
 CHECK(!owner.fenced&&owner.decision=="COMMITTED");
 CHECK(!owners.can_publish(op,"S1",1)); CHECK(owners.can_publish(op,target,2));
 CHECK(source.retired());
 CHECK(source.consume("stale",1,1,{})==Consume::Fenced);
 rejects([&]{source.activate(1);});
 CHECK(resources.valid(ticket,1000)); // Committed target holds survive preparation expiry.
 CHECK(resources.used().at("target.mem")==4096);
 CHECK(resources.used().at("route")==10);
}
void check_reopened(const TempJournal& file,const std::string& op,const std::string& destination) {
 OwnershipStore recovered(file.path);
 CHECK(recovered.read(op).node==destination); CHECK(recovered.read(op).generation==2);
 CHECK(!recovered.can_publish(op,"S1",1)); CHECK(recovered.can_publish(op,destination,2));
}
const Demands capacities{{"target.mem",8192},{"route",100}};
const Demands request{{"target.mem",4096},{"route",10}};
const std::vector<std::vector<Row>> aggregation_batches{
 {{1,10,5},{2,10,std::nullopt},{3,20,-4},{4,30,std::nullopt}},
 {{5,10,8},{6,20,12},{7,40,std::nullopt}},
 {{8,10,-2},{9,20,std::nullopt},{10,40,9}},
 {{11,10,0},{12,20,4},{13,30,std::nullopt},{14,50,7}},
 {{15,10,9},{16,20,-8},{17,40,3},{18,50,std::nullopt}},
 {}};
void aggregation(const std::string& destination,bool stop_copy) {
 StatePartition baseline(Kind::Aggregate); baseline.activate(1);
 for(std::size_t index=0;index<aggregation_batches.size();++index)
  CHECK(baseline.consume("input",index+1,1,aggregation_batches[index])==Consume::Applied);
 baseline.finalize(1);
 const auto expected=baseline.groups();
 CHECK(expected.at(10).sum==20&&expected.at(10).count==5&&expected.at(10).min==-2&&expected.at(10).max==9);
 CHECK(expected.at(20).sum==4&&expected.at(20).count==4&&expected.at(20).min==-8&&expected.at(20).max==12);
 CHECK(expected.at(30).count==0); // The all-NULL group must survive snapshot and continuation.
 TempJournal file;
 const std::string name="query/aggregate",ticket="aggregate-transfer";
 {
  OwnershipStore owners(file.path); owners.create(name,"S1"); ResourceLedger resources(capacities);
  StatePartition source(Kind::Aggregate),target(Kind::Aggregate); source.activate(1);
  for(std::size_t index=0;index<2;++index)
   CHECK(source.consume("input",index+1,1,aggregation_batches[index])==Consume::Applied);
  Handoff handoff(ticket,name,destination,source,target,owners,resources);
  CHECK(handoff.begin(request,0,100,stop_copy));
  std::size_t next=2;
  if(stop_copy) CHECK(source.consume("input",3,1,aggregation_batches[2])==Consume::Fenced);
  else {
   CHECK(source.consume("input",3,1,aggregation_batches[2])==Consume::Applied);
   CHECK(handoff.catch_up(1)); CHECK(source.dirty_bytes()==0);
   CHECK(source.consume("input",4,1,aggregation_batches[3])==Consume::Applied);
   CHECK(source.dirty_bytes()>0); // Final cutover must replay changes made after catch-up.
   next=4;
  }
  const auto frontier=source.progress();
  CHECK(handoff.cutover(2,1e9,0.001,0.001,1,1)); CHECK(handoff.finished());
  same_progress(target.progress(),frontier);
  check_committed(owners,resources,name,destination,ticket,source);
  CHECK(target.consume("input",next+1,1,aggregation_batches[next])==Consume::Fenced);
  CHECK(target.consume("input",1,2,aggregation_batches[0])==Consume::Duplicate);
  for(std::size_t index=next;index<aggregation_batches.size();++index)
   CHECK(target.consume("input",index+1,2,aggregation_batches[index])==Consume::Applied);
  target.finalize(2); same_groups(target.groups(),expected);
  CHECK(target.progress().input==baseline.progress().input);
  resources.release(ticket); CHECK(resources.used().at("target.mem")==0);
 }
 check_reopened(file,name,destination);
}
const std::vector<std::vector<Row>> build_batches{
 {{1,7,10},{2,7,std::nullopt},{2,7,std::nullopt},{3,8,11}},
 {{4,7,20},{5,9,std::nullopt}}};
const std::vector<std::vector<Row>> probe_batches{
 {{100,7,1},{101,8,std::nullopt},{102,0,3}},
 {{103,7,4},{103,7,4},{104,9,5}},
 {{105,7,6},{106,8,7},{107,8,8}}};
void join(const std::string& destination,bool stop_copy,bool mid_probe) {
 StatePartition baseline(Kind::InnerJoin); baseline.activate(1);
 for(std::size_t index=0;index<build_batches.size();++index)
  CHECK(baseline.consume("build",index+1,1,build_batches[index])==Consume::Applied);
 for(std::size_t index=0;index<probe_batches.size();++index)
  CHECK(baseline.consume("probe",index+1,1,probe_batches[index])==Consume::Applied);
 const auto reference=baseline.matches();
 const std::multiset<std::pair<int64_t,int64_t>> expected(reference.begin(),reference.end());
 CHECK(expected.size()==20); CHECK(expected.count({103,2})==4);
 TempJournal file;
 const std::string name="query/join",ticket="join-transfer";
 {
  OwnershipStore owners(file.path); owners.create(name,"S1"); ResourceLedger resources(capacities);
  StatePartition source(Kind::InnerJoin),target(Kind::InnerJoin); source.activate(1);
  for(std::size_t index=0;index<build_batches.size();++index)
   CHECK(source.consume("build",index+1,1,build_batches[index])==Consume::Applied);
  std::size_t next=0;
  if(mid_probe) {CHECK(source.consume("probe",1,1,probe_batches[0])==Consume::Applied);next=1;}
  Handoff handoff(ticket,name,destination,source,target,owners,resources);
  CHECK(handoff.begin(request,0,100,stop_copy));
  if(stop_copy) CHECK(source.consume("probe",next+1,1,probe_batches[next])==Consume::Fenced);
  else if(mid_probe) {
   CHECK(source.consume("probe",2,1,probe_batches[1])==Consume::Applied);
   CHECK(handoff.catch_up(1)); next=2;
  }
  const auto frontier=source.progress();
  CHECK(handoff.cutover(2,1e9,0.001,0.001,1,1));
  same_progress(target.progress(),frontier);
  check_committed(owners,resources,name,destination,ticket,source);
  CHECK(target.consume("probe",next+1,1,probe_batches[next])==Consume::Fenced);
  CHECK(target.consume("build",1,2,build_batches[0])==Consume::Duplicate);
  if(next) CHECK(target.consume("probe",1,2,probe_batches[0])==Consume::Duplicate);
  for(std::size_t index=next;index<probe_batches.size();++index)
   CHECK(target.consume("probe",index+1,2,probe_batches[index])==Consume::Applied);
  const auto observed=target.matches();
  CHECK((std::multiset<std::pair<int64_t,int64_t>>(observed.begin(),observed.end())==expected));
  CHECK(observed.size()==reference.size());
  CHECK(target.progress().input==baseline.progress().input);
  rejects([&]{target.consume("build",3,2,{{6,7,30}});}); // Build phase remains closed after migration.
  resources.release(ticket); CHECK(resources.used().at("target.mem")==0);
 }
 check_reopened(file,name,destination);
}
const std::vector<std::vector<Row>> sort_batches{
 {{11,3,1},{12,1,std::nullopt},{13,2,2}},
 {{14,1,8},{15,3,9},{16,2,std::nullopt}},
 {{17,1,-5},{18,2,0},{19,3,2},{20,1,std::nullopt}}};
using Output=std::pair<uint64_t,Row>;
Output emit(StatePartition& state,OwnershipStore& owners,const std::string& name,
            const std::string& node,uint64_t generation,bool ack=true) {
 CHECK(owners.can_publish(name,node,generation));
 auto output=state.next_sorted(generation); CHECK(output.has_value());
 if(ack) CHECK(state.acknowledge_output(generation,output->first+1));
 return *output;
}
void stable_sort(const std::string& destination,bool stop_copy) {
 StatePartition baseline(Kind::Sort); baseline.activate(1);
 for(std::size_t index=0;index<sort_batches.size();++index)
  CHECK(baseline.consume("input",index+1,1,sort_batches[index])==Consume::Applied);
 baseline.finalize(1);
 std::vector<Output> expected;
 while(auto output=baseline.next_sorted(1)) {
  expected.push_back(*output); CHECK(baseline.acknowledge_output(1,output->first+1));
 }
 const std::vector<int64_t> stable_ids{12,14,17,20,13,16,18,11,15,19};
 CHECK(expected.size()==stable_ids.size());
 for(std::size_t index=0;index<expected.size();++index) CHECK(expected[index].second.id==stable_ids[index]);
 TempJournal file;
 const std::string name="query/sort",ticket="sort-transfer";
 {
  OwnershipStore owners(file.path); owners.create(name,"S1"); ResourceLedger resources(capacities);
  StatePartition source(Kind::Sort),target(Kind::Sort); source.activate(1);
  for(std::size_t index=0;index<sort_batches.size();++index)
   CHECK(source.consume("input",index+1,1,sort_batches[index])==Consume::Applied);
  source.finalize(1);
  std::vector<Output> observed;
  observed.push_back(emit(source,owners,name,"S1",1)); observed.push_back(emit(source,owners,name,"S1",1));
  CHECK(source.progress().output_ack==2);
  Handoff handoff(ticket,name,destination,source,target,owners,resources);
  CHECK(handoff.begin(request,0,100,stop_copy));
  CHECK(!target.next_sorted(2));
  if(stop_copy) CHECK(!source.next_sorted(1));
  else {
   observed.push_back(emit(source,owners,name,"S1",1)); observed.push_back(emit(source,owners,name,"S1",1));
   CHECK(handoff.catch_up(1));
   observed.push_back(emit(source,owners,name,"S1",1,false));
   CHECK(source.progress().output_cursor==5&&source.progress().output_ack==4);
   CHECK(!handoff.cutover(2,1e9,0.001,0.001,1,1)); // Cannot move an unconfirmed published cursor.
   CHECK(owners.can_publish(name,"S1",1)); CHECK(!owners.can_publish(name,destination,2));
   CHECK(source.acknowledge_output(1,5)); CHECK(handoff.catch_up(3));
  }
  const auto frontier=source.progress();
  CHECK(handoff.cutover(4,1e9,0.001,0.001,1,1));
  same_progress(target.progress(),frontier);
  check_committed(owners,resources,name,destination,ticket,source);
  CHECK(!source.next_sorted(1)); CHECK(!source.acknowledge_output(1,frontier.output_cursor));
  CHECK(!target.next_sorted(1)); CHECK(!target.acknowledge_output(1,frontier.output_cursor));
  CHECK(target.acknowledge_output(2,frontier.output_cursor)); // Repeated ACK is idempotent.
  CHECK(!target.acknowledge_output(2,expected.size()+1));
  while(target.progress().output_cursor<expected.size()) observed.push_back(emit(target,owners,name,destination,2));
  CHECK(!target.next_sorted(2)); CHECK(observed.size()==expected.size());
  for(std::size_t index=0;index<observed.size();++index) {
   CHECK(observed[index].first==expected[index].first);
   CHECK(observed[index].second.id==expected[index].second.id);
   CHECK(observed[index].second.key==expected[index].second.key);
   CHECK(observed[index].second.value==expected[index].second.value);
  }
  CHECK(target.progress().output_cursor==expected.size()); CHECK(target.progress().output_ack==expected.size());
  resources.release(ticket); CHECK(resources.used().at("target.mem")==0);
 }
 check_reopened(file,name,destination);
}
}
int main() {
 try {
  for(const std::string destination:{"S3","C"}) for(bool stop_copy:{false,true}) {
   aggregation(destination,stop_copy);
   join(destination,stop_copy,false); join(destination,stop_copy,true);
   stable_sort(destination,stop_copy);
  }
  std::cout<<"operator continuation: "<<checks<<" checks passed across 16 handoffs\n";
  return 0;
 } catch(const std::exception& error) {std::cerr<<error.what()<<'\n';return 1;}
}
