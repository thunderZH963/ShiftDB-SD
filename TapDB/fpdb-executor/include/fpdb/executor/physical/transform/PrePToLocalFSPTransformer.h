//
// Created by Partho on 3/26/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORMER_PREPTOLOCALFSPTRANSFORMER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORMER_PREPTOLOCALFSPTRANSFORMER_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/plan/prephysical/FilterableScanPrePOp.h>
#include <fpdb/plan/Mode.h>
#include <fpdb/catalogue/Partition.h>

#include <unordered_map>
#include <vector>

using namespace fpdb::plan;
using namespace fpdb::plan::prephysical;
using namespace fpdb::expression::gandiva;
using namespace fpdb::catalogue;

namespace fpdb::executor::physical {

class PrePToLocalFSPTransformer {
public:
  PrePToLocalFSPTransformer(uint prePOpId,
                       int numNodes);

  /**
   * Transform filterable scan prephysical op to physical op
   * @param prePOp: prephysical op
   * @return a pair of connect physical ops (to producer) and current all (cumulative) physical ops
   */
  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp);

#if PRINT_PHY_PLAN > 0
  std::unordered_map<std::string, std::string> getOp2Name();
  std::unordered_map<std::string, std::string> getProducer2Consumer();
#endif //PRINT_PHY_PLAN > 0

private:
  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScan_(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates,
                                const vector<string> &projectColumnNames);
  uint prePOpId_;
  int numNodes_;

#if PRINT_PHY_PLAN > 0
  std::unordered_map<std::string, std::string> op2name;
  std::unordered_map<std::string, std::string> producer2consumer;
#endif //PRINT_PHY_PLAN > 0
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORMER_PREPTOLOCALFSPTRANSFORMER_H
