#include "shiftdb/estimator.hpp"
#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
namespace shiftdb {
namespace {
void positive(double x) { if(!std::isfinite(x)||x<=0) throw std::invalid_argument("invalid service rate/time"); }
void nonnegative(double x) { if(!std::isfinite(x)||x<0) throw std::invalid_argument("invalid work"); }
uint64_t add(uint64_t a,uint64_t b) {
 if(b>std::numeric_limits<uint64_t>::max()-a) throw std::overflow_error("memory sum overflow");
 return a+b;
}
uint64_t mul(uint64_t a,uint64_t b) {
 if(a&&b>std::numeric_limits<uint64_t>::max()/a) throw std::overflow_error("memory product overflow");
 return a*b;
}
}
RateEstimator::RateEstimator(RateConfig c):config_(c) {
 if(!c.window_ms||c.stale_ms<c.window_ms||!std::isfinite(c.alpha)||c.alpha<=0||c.alpha>1)
  throw std::invalid_argument("invalid estimator configuration");
}
void RateEstimator::advance(Series& s,uint64_t now) {
 if(!s.initialized) {s.initialized=true;s.window_start=s.last_call=now;return;}
 if(now<s.last_call) throw std::invalid_argument("clock regressed");
 if(now-s.window_start>=config_.window_ms) {
  if(s.seconds>0) {
   const double sample=s.work/s.seconds;
   nonnegative(sample);
   s.rate=s.have_rate?config_.alpha*sample+(1-config_.alpha)*s.rate:sample;
   s.have_rate=true;
   s.last_sample=s.pending_sample;
  }
  s.work=s.seconds=0;
  // Skip empty windows; they neither decay nor rejuvenate the estimate.
  s.window_start=now-(now-s.window_start)%config_.window_ms;
 }
 s.last_call=now;
}
void RateEstimator::calibrate(const std::string& key,const std::vector<double>& rates,uint64_t now) {
 if(key.empty()||rates.size()!=5) throw std::invalid_argument("five calibration runs required");
 auto values=rates;for(double x:values) positive(x);std::sort(values.begin(),values.end());
 const auto it=series_.find(key);
 if(it!=series_.end()&&now<it->second.last_call) throw std::invalid_argument("clock regressed");
 Series s;s.initialized=s.have_rate=true;s.rate=values[2];s.last_sample=s.last_call=s.window_start=now;
 series_[key]=s;
}
void RateEstimator::observe(const std::string& key,double work,double seconds,uint64_t now) {
 if(key.empty()) throw std::invalid_argument("empty telemetry key");
 nonnegative(work);positive(seconds);
 Series next=series_[key];advance(next,now);
 const double w=next.work+work,t=next.seconds+seconds;
 if(!std::isfinite(w)||!std::isfinite(t)||!std::isfinite(w/t)) throw std::overflow_error("telemetry overflow");
 next.work=w;next.seconds=t;next.pending_sample=now;series_[key]=next;
}
std::optional<double> RateEstimator::rate(const std::string& key,uint64_t now) {
 auto it=series_.find(key);if(it==series_.end())return std::nullopt;
 advance(it->second,now);const auto& s=it->second;
 if(!s.have_rate||s.rate<=0||now-s.last_sample>config_.stale_ms)return std::nullopt;
 return s.rate;
}
std::optional<Plan> estimate_plan(Plan p,const std::vector<WorkEstimate>& work,
 const std::map<std::pair<std::string,std::string>,std::string>& links,
 RateEstimator& rates,uint64_t now) {
 std::map<std::string,const WorkEstimate*> by_op;
 for(const auto& w:work) {
  nonnegative(w.remaining_rows);
  if(!by_op.emplace(w.op,&w).second)throw std::invalid_argument("duplicate work estimate");
 }
 if(by_op.size()!=p.ops.size())throw std::invalid_argument("incomplete work estimates");
 for(auto& op:p.ops) {
  auto it=by_op.find(op.id);if(it==by_op.end())throw std::invalid_argument("missing work estimate");
  const auto& w=*it->second;op.service_seconds.clear();
  for(const auto& [node,key]:w.service_keys) {
   if(!op.legal.count(node))continue;
   auto rate=rates.rate(key,now);
   if(rate) { const double cost=w.remaining_rows/ *rate;
    if(!std::isfinite(cost))return std::nullopt;
    op.service_seconds[node]=cost;
   }
  }
  if(!op.service_seconds.count(op.owner))return std::nullopt;
 }
 p.bandwidth.clear();
 for(const auto& [path,key]:links) {auto rate=rates.rate(key,now);if(rate)p.bandwidth[path]=*rate;}
 // Validate structural defects even if no moves will be attempted.
 for(const auto& edge:p.edges) {
  auto from=std::find_if(p.ops.begin(),p.ops.end(),[&](const auto& o){return o.id==edge.from;});
  auto to=std::find_if(p.ops.begin(),p.ops.end(),[&](const auto& o){return o.id==edge.to;});
  if(from==p.ops.end()||to==p.ops.end())throw std::invalid_argument("unknown edge endpoint");
  if(edge.bytes>0&&from->owner!=to->owner&&!p.bandwidth.count({from->owner,to->owner}))return std::nullopt;
 }
 remaining_seconds(p);return p;
}
uint64_t RestoreMemory::peak()const{return add(add(state,decode_scratch),add(receive_buffers,pending_updates));}
uint64_t hash_build_memory(uint64_t n,uint64_t width,uint64_t alignment,uint64_t buckets,uint64_t bucket,uint64_t buffers) {
 if(!alignment||(alignment&(alignment-1)))throw std::invalid_argument("alignment must be power of two");
 const auto remainder=width%alignment;
 const auto aligned=remainder?add(width,alignment-remainder):width;
 return add(add(mul(n,aligned),mul(buckets,bucket)),buffers);
}
uint64_t aggregation_memory(uint64_t groups,uint64_t width,uint64_t buffers,uint64_t distinct) {
 return add(add(mul(groups,width),buffers),distinct);
}
uint64_t sort_memory(uint64_t rows,uint64_t width,uint64_t index,uint64_t buffers) {
 return add(mul(rows,add(width,index)),buffers);
}
}
