#pragma once
#include <map>
#include <set>
#include <string>
#include <vector>
#include <optional>
namespace shiftdb {
using Demands = std::map<std::string,double>;
struct OperatorCost {
  std::string id, owner;
  std::set<std::string> legal;
  std::map<std::string,double> service_seconds;
  bool pinned = false; // scans or unsupported active-state adapters
};
struct EdgeCost { std::string from,to; double bytes=0; bool blocking=true; };
struct Plan {
  std::vector<OperatorCost> ops;
  std::vector<EdgeCost> edges;
  // Effective directed bytes/s: absence forbids a route. Same-node is free.
  std::map<std::pair<std::string,std::string>,double> bandwidth;
};
struct Move {
  std::string op,destination;
  double exposed_transition_seconds=0;
  Demands additional_demand;
};
struct Choice { Move move; double before=0,after=0,benefit=0,score=0; };
// Blocking edges wait for input. Streaming edges use max(input, service),
// explicitly a coarse overlap estimator, not an engine trace simulator.
double remaining_seconds(const Plan& plan);
std::optional<Choice> best_move(const Plan&,const std::vector<Move>&,
                                const Demands& capacity,const Demands& used,
                                double minimum_gain=0);
void apply_placement(Plan&,const Move&);
}
