#pragma once
#include "planner.hpp"
#include <cstdint>
#include <mutex>
#include <optional>
namespace shiftdb {
// Monotonic time is supplied by the caller; tests never sleep.
struct Hold { std::string id; Demands demand; uint64_t version=0, expires=0; bool committed=false; };
class ResourceLedger {
 mutable std::mutex mutex_;
 Demands capacity_,used_;
 std::map<std::string,Hold> holds_;
 std::set<std::string> retired_;
 uint64_t version_=0;
 void expire_locked(uint64_t now);
 void rebuild_usage_locked();
public:
 explicit ResourceLedger(Demands capacities);
 uint64_t version() const;
 bool reserve(const std::string&,const Demands&,uint64_t expected,uint64_t now,uint64_t ttl);
 bool valid(const std::string&,uint64_t now);
 bool renew(const std::string&,uint64_t now,uint64_t ttl);
 bool commit(const std::string&,uint64_t now);
 void release(const std::string&);
 Demands used() const;
};
struct Allocation {
 uint64_t id=0,generation=0,route=0,bytes=0;
 std::set<std::string> refs;
 bool sealed=false,done=false,credit=true;
 // A released reference cannot be re-registered by a delayed publication retry.
 std::set<std::string> released_refs;
};
class CreditLedger {
 mutable std::mutex mutex_;
 uint64_t limit_,live_=0,next_=1,slots_,busy_=0;
 std::map<uint64_t,Allocation> allocations_;
 std::map<uint64_t,uint64_t> route_refs_;
 // Physical execution, publication, and all consumer references must close.
 void collect(Allocation&);
public:
 CreditLedger(uint64_t bytes,uint64_t slots):limit_(bytes),slots_(slots){}
 std::optional<uint64_t> admit(uint64_t generation,uint64_t route,uint64_t bytes);
 bool extend(uint64_t id,uint64_t generation,uint64_t bytes);
 bool add_ref(uint64_t id,uint64_t generation,const std::string& ref);
 void seal(uint64_t id,uint64_t generation);
 void complete(uint64_t id,uint64_t generation);
 void release_ref(uint64_t id,uint64_t generation,const std::string& ref);
 uint64_t live() const; uint64_t busy() const; uint64_t route_refs(uint64_t) const;
};
}
