#include "shiftdb/tapdb_migration_client.hpp"
#include <fpdb/executor/physical/POpActor.h>
namespace shiftdb::native {
MigrationResult migrate_active(caf::scoped_actor& ctl,const caf::actor& source,
 const caf::actor& target,const std::string& ticket,const std::string& fingerprint,
 std::chrono::milliseconds timeout) {
 MigrationResult result;
 if(!source||!target||source==target||ticket.empty()||fingerprint.empty()||timeout.count()<=0){
  result.error="invalid migration arguments";return result;
 }
 auto error=[&](const caf::error& e){result.error=caf::to_string(e);};
 std::string status;
 result.stage="status";
 ctl->request(source,timeout,std::string("shiftdb.status.v1"),ticket).receive(
  [&](const std::string& s){status=s;},error);
 if(!result.error.empty())return result;
 if(status!="COMMITTED"){
  ShiftBytes image;fpdb::executor::physical::POpContext context;ShiftCounts received;int64_t count=0;
  result.stage="freeze";
  ctl->request(source,timeout,std::string("shiftdb.freeze.v1"),ticket,fingerprint).receive(
   [&](const ShiftBytes& b,const fpdb::executor::physical::POpContext& c,const ShiftCounts& r,int64_t n){image=b;context=c;received=r;count=n;},error);
  if(!result.error.empty())return result;
  result.stage="install";bool installed=false;
  ctl->request(target,timeout,std::string("shiftdb.install.v1"),ticket,fingerprint,image,context,received,count).receive(
   [&](bool ok){installed=ok;},error);
  if(!result.error.empty())return result;
  if(!installed){result.error="target rejected install";return result;}
 }
 // Also send commit on retries: the source checks that the peer is unchanged.
 result.stage="commit";bool committed=false;
 ctl->request(source,timeout,std::string("shiftdb.commit.v1"),ticket,target).receive(
  [&](bool ok){committed=ok;},error);
 if(!result.error.empty())return result;
 if(!committed){result.error="source rejected commit";return result;}
 result.stage="activate";
 ctl->request(target,timeout,std::string("shiftdb.activate.v1"),ticket).receive(
  [&](const std::string& s){result.active=(s=="ACTIVE");},error);
 return result;
}
}
