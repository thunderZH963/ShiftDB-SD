#pragma once
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
namespace shiftdb::tapdb {
struct OperatorRecord {
 std::string name,type;int node_id=0;bool remote=false;
 std::vector<std::string> producers,consumers;
 bool active_migration_supported=false; // requires an explicit native adapter
};
// Instantiated directly with upstream PhysicalPlan. This header avoids pulling
// CAF/Arrow into the independently compiled protocol core.
template<class PhysicalPlan> std::vector<OperatorRecord> describe(const PhysicalPlan& p) {
 std::vector<OperatorRecord> result;
 for(const auto& op:p.getPhysicalOps()) {
  if(!op)throw std::invalid_argument("null TapDB operator");
  auto producers=op->producers(),consumers=op->consumers();
  result.push_back({op->name(),op->getTypeString(),op->getNodeId(),op->spawnOnRemote(),
    {producers.begin(),producers.end()},{consumers.begin(),consumers.end()},false});
 }
 return result;
}
inline std::string quote(const std::string& s) {
 std::ostringstream o;o<<'"';const char* h="0123456789abcdef";
 for(unsigned char c:s) {
  if(c=='"'||c=='\\')o<<'\\'<<char(c);
  else if(c<32)o<<"\\u00"<<h[c>>4]<<h[c&15];else o<<char(c);
 }
 o<<'"';return o.str();
}
inline void strings(std::ostream& o,const std::vector<std::string>& values) {
 o<<'[';bool first=true;for(auto& v:values){if(!first)o<<',';first=false;o<<quote(v);}o<<']';
}
// Safe opt-in plan observation only: does not move actors or reinterpret the
// upstream node ID as a storage role. One output file per query ID.
template<class PhysicalPlan> void trace_if_enabled(const PhysicalPlan& p,long query_id) {
 const char* prefix=std::getenv("SHIFTDB_PLAN_TRACE");if(!prefix||!*prefix)return;
 auto records=describe(p);std::ofstream out(std::string(prefix)+"-"+std::to_string(query_id)+".json");
 if(!out)throw std::runtime_error("cannot create ShiftDB plan trace");
 out<<"{\"query_id\":"<<query_id<<",\"operators\":[";bool first=true;
 for(const auto& r:records) {
  if(!first)out<<',';first=false;
  out<<"{\"name\":"<<quote(r.name)<<",\"type\":"<<quote(r.type)<<",\"node_id\":"<<r.node_id
     <<",\"spawn_remote\":"<<(r.remote?"true":"false")<<",\"active_migration_supported\":false,\"producers\":";
  strings(out,r.producers);out<<",\"consumers\":";strings(out,r.consumers);out<<'}';
 }
 out<<"]}\n";out.close();if(!out)throw std::runtime_error("ShiftDB plan trace write failed");
}
}
