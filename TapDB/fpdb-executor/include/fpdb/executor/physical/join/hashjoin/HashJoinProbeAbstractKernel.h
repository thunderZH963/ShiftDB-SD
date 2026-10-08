//
// Created by Yifei Yang on 12/15/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEABSTRACTKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEABSTRACTKERNEL_H

#include <fpdb/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/tuple/TupleSet.h>
#include <fpdb/tuple/TupleSetIndex.h>
#include <fpdb/executor/physical/POpContext.h>
#include <set>
#include <vector>
#include <memory>

using namespace fpdb::tuple;
using namespace std;

namespace fpdb::executor::physical::join {

/**
 * Abstract class for kernel of hash join, derived class includes HashJoinProbeKernel
 */
class HashJoinProbeAbstractKernel {
  
public:
  HashJoinProbeAbstractKernel(HashJoinPredicate pred, set<string> neededColumnNames, std::string name);
  HashJoinProbeAbstractKernel() = default;
  HashJoinProbeAbstractKernel(const HashJoinProbeAbstractKernel&) = default;
  HashJoinProbeAbstractKernel& operator=(const HashJoinProbeAbstractKernel&) = default;
  virtual ~HashJoinProbeAbstractKernel() = default;

  virtual bool isSemi() const = 0;
  virtual tl::expected<void, string> joinBuildTupleSetIndex(const shared_ptr<TupleSetIndex>& tupleSetIndex, bool readyToRun) = 0;
  virtual tl::expected<void, string> joinProbeTupleSet(const shared_ptr<TupleSet>& tupleSet, bool readyToRun) = 0;
  virtual tl::expected<void, string> finalize() = 0;

  const std::optional<shared_ptr<fpdb::tuple::TupleSet>> &getBuffer() const;
  const std::optional<shared_ptr<::arrow::Schema>> &getOutputSchema() const;

  void setOp(std::shared_ptr<PhysicalOp>& op);
  void setName(const std::string name);

  virtual void clear();
  void clearBuffer();
  unsigned long long exectime_=0;
protected:
  tl::expected<void, string> putBuildTupleSetIndex(const shared_ptr<TupleSetIndex>& tupleSetIndex);
  tl::expected<void, string> putProbeTupleSet(const shared_ptr<TupleSet>& tupleSet);
  tl::expected<void, string> buffer(const shared_ptr<TupleSet>& tupleSet);
  void bufferOutputSchema(const shared_ptr<TupleSetIndex> &tupleSetIndex, const shared_ptr<TupleSet> &tupleSet);
  tl::expected<void, string> validateColumnNames(const shared_ptr<arrow::Schema> &schema,
                                                 const vector<string> &columnNames);


  HashJoinPredicate pred_;
  set<string> neededColumnNames_;
  std::string name_;
  unsigned long long curNumRows_ = 0;

  vector<shared_ptr<pair<bool, int>>> neededColumnIndice_;   // <true/false, i> -> ith column in build/probe table
  std::optional<shared_ptr<::arrow::Schema>> outputSchema_;
  std::optional<shared_ptr<TupleSetIndex>> buildTupleSetIndex_;
  std::vector<shared_ptr<TupleSet>> probeTupleSet_;
  std::optional<shared_ptr<fpdb::tuple::TupleSet>> buffer_;
  std::shared_ptr<PhysicalOp> probePOp_;
  bool spawnOnRemote_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEABSTRACTKERNEL_H
