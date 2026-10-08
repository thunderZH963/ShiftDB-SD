//
// Created by Yifei Yang on 11/23/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_EXECUTION_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_EXECUTION_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/PhysicalPlan.h>
#include <fpdb/executor/physical/POpDirectory.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/executor/physical/collate/CollatePOp2.h>
#include <fpdb/executor/physical/s3/S3SelectScanAbstractPOp.h>
#include <fpdb/tuple/TupleSet.h>
#include <caf/all.hpp>
#include <memory>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <utility>
#include <vector>
#include <map>
#define FILESCANPRIORITY 1
using namespace fpdb::executor::physical;
using namespace std;

namespace fpdb::executor {

inline constexpr const char *ExecutionRootActorName = "execution_root";
#if FILESCANPRIORITY >0
extern std::map<std::string,int> probevec_;
static bool compActorName(std::pair<std::string, ::caf::actor> p1,std::pair<std::string, ::caf::actor> p2){
        std::string name1=p1.first;
        std::string name2=p2.first;
        if(name1.size()==0||name2.size()==0){
          std::cout<<"Sort nullptr"<<std::endl;
          return false;
        }
        if(probevec_[name1]>probevec_[name2])
            return true;
        else
            return false;
}
#endif
/**
 * Execution of a single query
 */
class Execution {

public:
  Execution(long queryId,
            const shared_ptr<::caf::actor_system> &actorSystem,
            const vector<::caf::node_id> &nodes,
            const ::caf::actor &segmentCacheActor,
            const shared_ptr<PhysicalPlan> &physicalPlan,
            bool isDistributed);
  ~Execution();

  long getQueryId() const;

  shared_ptr<TupleSet> execute(std::string queryName);

  long getElapsedTime();
  shared_ptr<PhysicalOp> getPhysicalOp(const std::string &name);
  physical::s3::S3SelectScanStats getAggregateS3SelectScanStats();
  std::tuple<size_t, size_t, size_t> getFilterTimeNSInputOutputBytes();
  string showMetrics(bool showOpTimes = true, bool showScanMetrics = true);

  void write_graph(const string &file);

private:
  void boot();
  void start();
  void join();
  void close();

  ::caf::actor localSpawn(const shared_ptr<PhysicalOp> &op);
  ::caf::actor remoteSpawn(const shared_ptr<PhysicalOp> &op, int nodeId);

  long queryId_;
  shared_ptr<::caf::actor_system> actorSystem_;
  vector<::caf::node_id> nodes_;
  shared_ptr<::caf::scoped_actor> rootActor_;
  ::caf::actor segmentCacheActor_;
  shared_ptr<PhysicalPlan> physicalPlan_;
  bool isDistributed_;

  POpDirectory opDirectory_;
  [[maybe_unused]] physical::collate::CollateActor collateActorHandle_;
  shared_ptr<physical::collate::CollatePOp> legacyCollateOperator_;

  // for execution time
  chrono::steady_clock::time_point startTime_;
  chrono::steady_clock::time_point stopTime_;
#if FILESCANPRIORITY >0
  std::unordered_map<int, std::vector<std::pair<std::string, ::caf::actor> > > fileHandleSet_;
#else
  std::unordered_map<int, std::unordered_set<::caf::actor>> fileHandleSet_;
#endif
  std::unordered_map<std::string, int> name2Loc_;

  std::string queryName_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_EXECUTION_H
