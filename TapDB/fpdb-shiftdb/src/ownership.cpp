#include "shiftdb/ownership.hpp"
#include "shiftdb/state.hpp"
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#include <cerrno>
#include <iomanip>
#include <sstream>
#include <stdexcept>
namespace shiftdb {
namespace {
void valid_name(const std::string& x) {
 if(x.empty())throw std::invalid_argument("empty metadata identifier");
 for(unsigned char c:x)if(c<32||c==127)throw std::invalid_argument("invalid metadata identifier");
}
uint64_t hash(const std::string& s){return checksum(std::vector<uint8_t>(s.begin(),s.end()));}
std::string encode(const OwnerRecord& r){std::ostringstream o;o<<std::quoted(r.op)<<' '<<std::quoted(r.node)<<' '<<r.generation<<' '<<r.route<<' '<<r.fenced<<' '<<std::quoted(r.ticket)<<' '<<r.decision;return o.str();}
bool same_record(const OwnerRecord& a,const OwnerRecord& b) {
 return a.op==b.op&&a.node==b.node&&a.ticket==b.ticket&&a.generation==b.generation&&
        a.route==b.route&&a.fenced==b.fenced&&a.decision==b.decision;
}
void validate_record(const OwnerRecord& r,const OwnerRecord* old,
                     const std::set<std::string>& tickets) {
 valid_name(r.op);valid_name(r.node);
 if(!r.generation||!r.route)throw std::runtime_error("invalid ownership version");
 if(!old) {
  if(r.generation!=1||r.route!=1||r.fenced||!r.ticket.empty()||r.decision!="NONE")
   throw std::runtime_error("journal does not begin with an owner creation");
  return;
 }
 if(same_record(r,*old))return; // Replayed duplicate never rolls metadata back.
 valid_name(r.ticket);
 if(r.decision=="PREPARED"&&!old->fenced&&r.fenced&&
    r.node==old->node&&r.generation==old->generation&&r.route==old->route&&
    !tickets.count(r.ticket))return;
 if(r.decision=="ABORTED"&&old->fenced&&!r.fenced&&old->decision=="PREPARED"&&
    r.ticket==old->ticket&&r.node==old->node&&r.generation==old->generation&&
    r.route==old->route)return;
 if(r.decision=="COMMITTED"&&old->fenced&&!r.fenced&&old->decision=="PREPARED"&&
    r.ticket==old->ticket&&r.node!=old->node&&old->generation!=UINT64_MAX&&
    old->route!=UINT64_MAX&&r.generation==old->generation+1&&r.route==old->route+1)return;
 throw std::runtime_error("invalid ownership journal transition");
}
}
OwnershipStore::OwnershipStore(const std::string& path) {
 fd_=::open(path.c_str(),O_RDWR|O_CREAT|O_APPEND|O_CLOEXEC,0600);
 if(fd_<0)throw std::runtime_error("cannot open ownership journal");
 try {
  if(flock(fd_,LOCK_EX|LOCK_NB)!=0)throw std::runtime_error("ownership journal already has a writer");
  // Persist directory entry too; caller creates the containing directory.
  auto slash=path.find_last_of('/');auto dir=slash==std::string::npos?".":path.substr(0,slash);if(dir.empty())dir="/";
  int d=::open(dir.c_str(),O_RDONLY);if(d<0)throw std::runtime_error("cannot open journal directory");
  int sync=fsync(d);close(d);if(sync!=0)throw std::runtime_error("cannot sync journal directory");
  std::string data;char b[4096];ssize_t n;
  while((n=::read(fd_,b,sizeof(b)))>0)data.append(b,size_t(n));
  if(n<0)throw std::runtime_error("journal read failed");
  if(!data.empty()&&data.back()!='\n')throw std::runtime_error("torn journal tail; recovery required");
  std::istringstream lines(data);std::string line;
  while(std::getline(lines,line)) {
   auto tab=line.rfind('\t');if(tab==std::string::npos)throw std::runtime_error("invalid journal framing");
   auto payload=line.substr(0,tab);size_t used=0;auto expected=std::stoull(line.substr(tab+1),&used);
   if(used!=line.size()-tab-1||hash(payload)!=expected)throw std::runtime_error("journal checksum mismatch");
   std::istringstream in(payload);OwnerRecord r;
   if(!(in>>std::quoted(r.op)>>std::quoted(r.node)>>r.generation>>r.route>>r.fenced>>std::quoted(r.ticket)>>r.decision))throw std::runtime_error("invalid journal record");
   in>>std::ws;if(!in.eof()||!r.generation||!r.route)throw std::runtime_error("invalid journal tail");
   auto previous=records_.find(r.op);
   validate_record(r,previous==records_.end()?nullptr:&previous->second,tickets_[r.op]);
   if(r.decision=="PREPARED")tickets_[r.op].insert(r.ticket);
   records_[r.op]=r;
  }
 } catch(...) {close(fd_);fd_=-1;throw;}
}
OwnershipStore::~OwnershipStore(){if(fd_>=0)close(fd_);}
void OwnershipStore::persist(const OwnerRecord& r) {
 if(uncertain_)throw std::runtime_error("journal outcome unresolved; reopen before serving");
 auto previous=records_.find(r.op);
 validate_record(r,previous==records_.end()?nullptr:&previous->second,tickets_[r.op]);
 auto data=encode(r);data+='\t'+std::to_string(hash(data))+'\n';
 size_t offset=0;
 while(offset<data.size()) {
  auto n=::write(fd_,data.data()+offset,data.size()-offset);
  if(n<0&&errno==EINTR)continue;
  if(n<=0){uncertain_=true;throw std::runtime_error("journal append uncertain");}offset+=size_t(n);
 }
 if(fsync(fd_)!=0){uncertain_=true;throw std::runtime_error("journal sync uncertain");}
 if(r.decision=="PREPARED")tickets_[r.op].insert(r.ticket);
 records_[r.op]=r;
}
void OwnershipStore::create(const std::string& op,const std::string& node) {
 valid_name(op);valid_name(node);std::lock_guard<std::mutex> l(mutex_);
 if(records_.count(op))throw std::logic_error("operator already registered");persist(OwnerRecord{op,node,"",1,1,false,"NONE"});
}
OwnerRecord OwnershipStore::read(const std::string& op)const {
 std::lock_guard<std::mutex> l(mutex_);if(uncertain_)throw std::runtime_error("unresolved journal");return records_.at(op);
}
bool OwnershipStore::fence(const std::string& op,uint64_t gen,const std::string& ticket) {
 valid_name(ticket);std::lock_guard<std::mutex> l(mutex_);auto r=records_.at(op);
 if(r.generation!=gen)return false;
 if(r.fenced)return r.ticket==ticket;
 if(tickets_[op].count(ticket))return false; // all prior tickets remain retired
 r.fenced=true;r.ticket=ticket;r.decision="PREPARED";persist(r);return true;
}
bool OwnershipStore::commit(const std::string& op,uint64_t gen,const std::string& ticket,const std::string& dest) {
 valid_name(ticket);valid_name(dest);std::lock_guard<std::mutex> l(mutex_);auto r=records_.at(op);
 if(gen==UINT64_MAX)return false;
 if(r.ticket==ticket&&r.decision=="COMMITTED")return r.node==dest&&r.generation==gen+1;
 if(r.generation!=gen||r.ticket!=ticket||!r.fenced||r.route==UINT64_MAX||r.node==dest)return false;
 r.node=dest;++r.generation;++r.route;r.fenced=false;r.decision="COMMITTED";persist(r);return true;
}
bool OwnershipStore::abort(const std::string& op,const std::string& ticket) {
 std::lock_guard<std::mutex> l(mutex_);auto r=records_.at(op);
 if(r.ticket!=ticket||r.decision=="COMMITTED")return false;
 if(r.decision=="ABORTED")return true;
 r.fenced=false;r.decision="ABORTED";persist(r);return true;
}
bool OwnershipStore::can_publish(const std::string& op,const std::string& node,uint64_t gen)const {
 std::lock_guard<std::mutex> l(mutex_);auto i=records_.find(op);
 return !uncertain_&&i!=records_.end()&&!i->second.fenced&&i->second.node==node&&i->second.generation==gen;
}
}
