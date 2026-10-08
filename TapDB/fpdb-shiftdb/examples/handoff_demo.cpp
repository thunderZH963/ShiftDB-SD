#include "shiftdb/handoff.hpp"
#include <iostream>
#include <unistd.h>
using namespace shiftdb;
int main(){
 char path[]="/tmp/shiftdb-demo-XXXXXX";int fd=mkstemp(path);if(fd<0)return 1;close(fd);
 try {
  OwnershipStore owners(path);owners.create("query/aggregate/partition0","storage-1");
  ResourceLedger resources({{"storage-3.memory",1048576},{"route.S1.S3",1000000}});
  StatePartition source(Kind::Aggregate),target(Kind::Aggregate),reference(Kind::Aggregate);source.activate(1);reference.activate(1);
  for(int batch=1;batch<=40;++batch){std::vector<Row> rows={{batch,batch%3,batch}};source.consume("scan",batch,1,rows);reference.consume("scan",batch,1,rows);}
  Handoff handoff("move-1","query/aggregate/partition0","storage-3",source,target,owners,resources);
  if(!handoff.begin({{"storage-3.memory",1048576},{"route.S1.S3",500000}},0,100))return 2;
  for(int batch=41;batch<=47;++batch){std::vector<Row> rows={{batch,batch%3,batch}};source.consume("scan",batch,1,rows);reference.consume("scan",batch,1,rows);}
  if(!handoff.catch_up(1)||!handoff.cutover(2,1000000,.001,.001,.1,1))return 3;
  for(int batch=48;batch<=100;++batch){std::vector<Row> rows={{batch,batch%3,batch}};target.consume("scan",batch,2,rows);reference.consume("scan",batch,1,rows);}
  auto expected=reference.groups();for(auto [key,g]:target.groups())if(g.sum!=expected.at(key).sum||g.count!=expected.at(key).count)return 4;
  if(target.groups().size()!=expected.size())return 5;
  auto owner=owners.read("query/aggregate/partition0");
  std::cout<<"Snapshot frontier=40; cutover frontier=47; resume=48\nOwner="<<owner.node<<" generation="<<owner.generation<<" route="<<owner.route<<"\n100 batches match uninterrupted aggregation.\n";
  resources.release("move-1");unlink(path);return 0;
 }catch(const std::exception& e){std::cerr<<e.what()<<'\n';unlink(path);return 1;}
}
