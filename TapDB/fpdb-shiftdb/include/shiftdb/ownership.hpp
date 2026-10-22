#pragma once
#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
namespace shiftdb {
struct OwnerRecord {
 std::string op,node,ticket;
 uint64_t generation=1,route=1;
 bool fenced=false;
 std::string decision="NONE";
};
// Single-writer POSIX journal, fsync before acknowledgement. A distributed
// deployment must implement the same CAS/fence contract with durable metadata.
class OwnershipStore {
 int fd_=-1; mutable std::mutex mutex_;bool uncertain_=false;
 std::map<std::string,OwnerRecord> records_;
 std::map<std::string,std::set<std::string>> tickets_;
 void persist(const OwnerRecord&);
public:
 explicit OwnershipStore(const std::string& path);
 ~OwnershipStore();
 OwnershipStore(const OwnershipStore&)=delete;
 OwnershipStore& operator=(const OwnershipStore&)=delete;
 void create(const std::string& op,const std::string& node);
 OwnerRecord read(const std::string& op) const;
 bool fence(const std::string& op,uint64_t generation,const std::string& ticket);
 bool commit(const std::string& op,uint64_t generation,const std::string& ticket,const std::string& destination);
 bool abort(const std::string& op,const std::string& ticket);
 bool can_publish(const std::string& op,const std::string& node,uint64_t generation)const;
};
}
