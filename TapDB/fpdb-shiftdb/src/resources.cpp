#include "shiftdb/resources.hpp"
#include <cmath>
#include <limits>
#include <stdexcept>
namespace shiftdb {
static uint64_t expiry(uint64_t now,uint64_t ttl) {
 if(!ttl||ttl>std::numeric_limits<uint64_t>::max()-now) throw std::invalid_argument("invalid lease lifetime");
 return now+ttl;
}
ResourceLedger::ResourceLedger(Demands c):capacity_(std::move(c)) {
 for(const auto& [r,v]:capacity_) {
  if(r.empty()||!std::isfinite(v)||v<0) throw std::invalid_argument("invalid capacity");
  used_.emplace(r,0);
 }
}
uint64_t ResourceLedger::version()const {std::lock_guard<std::mutex> l(mutex_);return version_;}
void ResourceLedger::rebuild_usage_locked() {
 // Recompute after retirement so floating-point subtraction cannot leave phantom usage.
 for(auto& [resource,value]:used_) {
  long double total=0;
  for(const auto& [id,hold]:holds_) {
   auto demand=hold.demand.find(resource);
   if(demand!=hold.demand.end())total+=demand->second;
  }
  value=static_cast<double>(total);
 }
}
void ResourceLedger::expire_locked(uint64_t now) {
 bool changed=false;
 for(auto i=holds_.begin();i!=holds_.end();) {
  if(!i->second.committed&&i->second.expires<=now) {
   changed=true;
   retired_.insert(i->first);i=holds_.erase(i);
   if(version_!=std::numeric_limits<uint64_t>::max())++version_;
  } else ++i;
 }
 if(changed)rebuild_usage_locked();
}
bool ResourceLedger::reserve(const std::string& id,const Demands& d,uint64_t expected,uint64_t now,uint64_t ttl) {
 auto end=expiry(now,ttl);
 for(const auto& [r,v]:d)
  if(r.empty()||!std::isfinite(v)||v<0)throw std::invalid_argument("invalid reservation");
 std::lock_guard<std::mutex> l(mutex_);expire_locked(now);
 if(id.empty()||retired_.count(id)) return false;
 auto existing=holds_.find(id);
 if(existing!=holds_.end()) return existing->second.demand==d; // retry cannot change a ticket
 if(expected!=version_||version_==std::numeric_limits<uint64_t>::max())return false;
 // Validate the entire joint demand without inserting keys or consuming capacity.
 for(const auto& [r,v]:d) {
  auto c=capacity_.find(r);
  if(c==capacity_.end()||v>c->second-used_.at(r))return false;
 }
 holds_.emplace(id,Hold{id,d,version_+1,end,false});
 for(const auto& [r,v]:d)used_.at(r)+=v;
 ++version_;return true;
}
bool ResourceLedger::valid(const std::string& id,uint64_t now) {
 std::lock_guard<std::mutex> l(mutex_);expire_locked(now);return holds_.count(id)>0;
}
bool ResourceLedger::renew(const std::string& id,uint64_t now,uint64_t ttl) {
 auto end=expiry(now,ttl);std::lock_guard<std::mutex> l(mutex_);expire_locked(now);
 auto i=holds_.find(id);if(i==holds_.end())return false;i->second.expires=end;return true;
}
bool ResourceLedger::commit(const std::string& id,uint64_t now) {
 std::lock_guard<std::mutex> l(mutex_);expire_locked(now);auto i=holds_.find(id);
 if(i==holds_.end())return false;i->second.committed=true;return true;
}
void ResourceLedger::release(const std::string& id) {
 std::lock_guard<std::mutex> l(mutex_);auto i=holds_.find(id);if(i==holds_.end())return;
 holds_.erase(i);retired_.insert(id);rebuild_usage_locked();
 if(version_!=std::numeric_limits<uint64_t>::max())++version_;
}
Demands ResourceLedger::used()const {std::lock_guard<std::mutex> l(mutex_);return used_;}
std::optional<uint64_t> CreditLedger::admit(uint64_t gen,uint64_t route,uint64_t bytes) {
 std::lock_guard<std::mutex> l(mutex_);
 if(!gen||!route||busy_>=slots_||bytes>limit_-live_||next_==UINT64_MAX)return std::nullopt;
 auto id=next_++;allocations_.emplace(id,Allocation{id,gen,route,bytes,{},false,false,true,{}});
 live_+=bytes;++busy_;++route_refs_[route];return id;
}
bool CreditLedger::extend(uint64_t id,uint64_t gen,uint64_t bytes) {
 std::lock_guard<std::mutex> l(mutex_);auto i=allocations_.find(id);
 if(i==allocations_.end()||i->second.generation!=gen||i->second.sealed||i->second.done||!i->second.credit||bytes>limit_-live_)return false;
 i->second.bytes+=bytes;live_+=bytes;return true;
}
bool CreditLedger::add_ref(uint64_t id,uint64_t gen,const std::string& ref) {
 std::lock_guard<std::mutex> l(mutex_);auto i=allocations_.find(id);
 if(i==allocations_.end()||i->second.generation!=gen||i->second.sealed||
    !i->second.credit||ref.empty()||i->second.released_refs.count(ref))return false;
 return i->second.refs.insert(ref).second;
}
void CreditLedger::collect(Allocation& a) {
 if(a.done&&a.sealed&&a.refs.empty()&&a.credit) {
  live_-=a.bytes;a.credit=false;
  auto route=route_refs_.find(a.route);
  if(--route->second==0)route_refs_.erase(route);
 }
}
void CreditLedger::seal(uint64_t id,uint64_t gen) {
 std::lock_guard<std::mutex> l(mutex_);auto i=allocations_.find(id);
 if(i==allocations_.end()||i->second.generation!=gen)return;
 i->second.sealed=true;collect(i->second);
 if(i->second.done&&!i->second.credit)allocations_.erase(i);
}
void CreditLedger::complete(uint64_t id,uint64_t gen) {
 std::lock_guard<std::mutex> l(mutex_);auto i=allocations_.find(id);
 if(i==allocations_.end()||i->second.generation!=gen)return;
 if(!i->second.done){i->second.done=true;--busy_;}
 collect(i->second);
 if(i->second.sealed&&!i->second.credit)allocations_.erase(i);
}
void CreditLedger::release_ref(uint64_t id,uint64_t gen,const std::string& ref) {
 std::lock_guard<std::mutex> l(mutex_);auto i=allocations_.find(id);
 if(i==allocations_.end()||i->second.generation!=gen||ref.empty())return;
 // Release may overtake reference registration on another event stream.
 i->second.released_refs.insert(ref);
 i->second.refs.erase(ref);collect(i->second);
 if(i->second.done&&!i->second.credit)allocations_.erase(i);
}
uint64_t CreditLedger::live()const{std::lock_guard<std::mutex> l(mutex_);return live_;}
uint64_t CreditLedger::busy()const{std::lock_guard<std::mutex> l(mutex_);return busy_;}
uint64_t CreditLedger::route_refs(uint64_t r)const{std::lock_guard<std::mutex> l(mutex_);auto i=route_refs_.find(r);return i==route_refs_.end()?0:i->second;}
}
