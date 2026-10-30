// Requires the full TapDB dependency stack. Exercises real GroupPOp and
// POpActor handlers; this in-process test is not a multi-host benchmark.
#include "shiftdb/tapdb_migration_client.hpp"
#include <fpdb/executor/physical/POpActor.h>
#include <fpdb/executor/physical/group/GroupPOp.h>
#include <fpdb/executor/physical/aggregate/function/Sum.h>
#include <fpdb/expression/gandiva/Column.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <iostream>
#include <caf/init_global_meta_objects.hpp>
using namespace fpdb::executor::physical;
using namespace fpdb::executor::message;
using namespace fpdb::tuple;
void require(bool value){if(!value)throw std::runtime_error("native actor migration assertion");}
int main(){
 ::caf::init_global_meta_objects<::caf::id_block::POpActor>();
 ::caf::init_global_meta_objects<::caf::id_block::POpContext>();
 ::caf::init_global_meta_objects<::caf::id_block::Envelope>();
 ::caf::actor_system_config cfg;::caf::actor_system system(cfg);::caf::scoped_actor ctl(system);
 auto client=::caf::actor_cast<::caf::actor>(ctl);
 auto make=[&](std::string name,int node){
  auto op=std::make_shared<group::GroupPOp>(name,std::vector<std::string>{"k","sum"},node,
   std::vector<std::string>{"k"},std::vector<std::shared_ptr<aggregate::AggregateFunction>>{
    std::make_shared<aggregate::Sum>("sum",fpdb::expression::gandiva::col("v"))});
  op->setQueryId(1);op->setSpawnOnRemote(false);
  op->create(std::make_shared<POpContext>(client,::caf::actor{}));return op;
 };
 auto input=make("input",0),output=make("output",0);
 auto sourceOp=make("group",0),targetOp=make("group",1);
 for(auto op:{sourceOp,targetOp}){op->consume(input);op->produce(output);}
 auto ctx=sourceOp->ctx();
 require(bool(ctx->operatorMap().insert(LocalPOpDirectoryEntry("input",client,POpRelationshipType::Producer,false))));
 require(bool(ctx->operatorMap().insert(LocalPOpDirectoryEntry("output",client,POpRelationshipType::Consumer,false))));
 ctx->spawnAddr()["output"]=0;ctx->activeAddr()["output"]=0;
 auto source=system.spawn<POpActor>(sourceOp),target=system.spawn<POpActor>(targetOp);
 auto batch=[&]{
  arrow::Int64Builder k,v;require(k.AppendValues({1,1,2}).ok());require(v.AppendValues({10,20,30}).ok());
  auto t=TupleSet::make(arrow::Table::Make(arrow::schema({arrow::field("k",arrow::int64()),arrow::field("v",arrow::int64())}),
   {k.Finish().ValueOrDie(),v.Finish().ValueOrDie()}));
  ctl->send(source,Envelope(std::make_shared<TupleMessage>(t,"input")));
 };
 ctl->send(source,Envelope(std::make_shared<StartMessage>("test")));batch();
 // Freeze first, then queue a batch before running the retryable client.
 bool frozen=false;
 ctl->request(source,std::chrono::seconds(5),std::string("shiftdb.freeze.v1"),std::string("move"),std::string("group-sum-v1")).receive(
  [&](const ShiftBytes&,const POpContext&,const ShiftCounts&,int64_t count){frozen=count==1;},
  [&](const ::caf::error&){require(false);});require(frozen);
 batch();
 auto moved=shiftdb::native::migrate_active(ctl,source,target,"move","group-sum-v1");require(moved.active);
 require(shiftdb::native::migrate_active(ctl,source,target,"move","group-sum-v1").active);
 batch();ctl->send(source,Envelope(std::make_shared<CompleteMessage>("input")));
 bool got=false;std::map<int64_t,int64_t> totals;
 while(!got)ctl->receive([&](const Envelope& e){
  if(e.message().type()==MessageType::TUPLE){
   auto t=dynamic_cast<const TupleMessage&>(e.message()).tuples()->table();
   for(int64_t i=0;i<t->num_rows();++i)
    totals[std::static_pointer_cast<arrow::Int64Scalar>(t->column(0)->GetScalar(i).ValueOrDie())->value]=
     std::static_pointer_cast<arrow::Int64Scalar>(t->column(1)->GetScalar(i).ValueOrDie())->value;
   got=true;
  }
 },::caf::after(std::chrono::seconds(5))>>[&]{require(false);});
 require((totals==std::map<int64_t,int64_t>{{1,90},{2,90}}));
 ctl->send_exit(source,::caf::exit_reason::user_shutdown);ctl->send_exit(target,::caf::exit_reason::user_shutdown);
 std::cout<<"Native aggregation survives freeze, queued input, commit retry and continuation\n";
}
