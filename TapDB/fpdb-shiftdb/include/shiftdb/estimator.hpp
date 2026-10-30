#pragma once
#include "planner.hpp"
#include <cstdint>
#include <optional>
namespace shiftdb {
// Single-controller API. Timestamps are monotonic milliseconds; work is rows
// (operator service) or bytes (directed link service), seconds are active time.
struct RateConfig { uint64_t window_ms=100, stale_ms=500; double alpha=0.2; };
class RateEstimator {
 struct Series {
  double rate=0, work=0, seconds=0;
  uint64_t last_call=0, last_sample=0, pending_sample=0, window_start=0;
  bool initialized=false, have_rate=false;
 };
 RateConfig config_;
 std::map<std::string,Series> series_;
 void advance(Series&,uint64_t);
public:
 explicit RateEstimator(RateConfig config={});
 // Exactly five rates from matched isolated runs, not synthetic paper results.
 void calibrate(const std::string&,const std::vector<double>& rates,uint64_t now);
 void observe(const std::string&,double work,double active_seconds,uint64_t now);
 std::optional<double> rate(const std::string&,uint64_t now);
};
struct WorkEstimate {
 std::string op;
 double remaining_rows=0;
 // Destination -> telemetry key (operator class/node/calibration configuration).
 std::map<std::string,std::string> service_keys;
};
// Return no plan when any current-owner or required remote-edge rate is stale.
// Uncalibrated alternative nodes are removed, rather than assigned zero cost.
std::optional<Plan> estimate_plan(Plan,const std::vector<WorkEstimate>&,
 const std::map<std::pair<std::string,std::string>,std::string>& link_keys,
 RateEstimator&,uint64_t now);
struct RestoreMemory {
 uint64_t state=0,decode_scratch=0,receive_buffers=0,pending_updates=0;
 uint64_t peak() const;
};
// Encoded/native layout sizes must come from adapters, not sizeof(pointer).
uint64_t hash_build_memory(uint64_t rows,uint64_t row_bytes,uint64_t alignment,
                           uint64_t buckets,uint64_t bucket_bytes,uint64_t buffers);
uint64_t aggregation_memory(uint64_t groups,uint64_t group_bytes,uint64_t buffers,
                            uint64_t distinct_value_bytes=0);
uint64_t sort_memory(uint64_t resident_rows,uint64_t row_bytes,uint64_t index_bytes,
                     uint64_t merge_buffers);
}
