//
// Created by matt on 12/12/19.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/HttpGetPostMethod.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/PredicateMessage.h>
#include <fpdb/executor/message/ScanMessage.h>
#include <fpdb/executor/message/cache/LoadResponseMessage.h>

#include <fpdb/expression/gandiva/Expression.h>

#include <string>

using namespace fpdb::executor::message;
using namespace fpdb::tuple;
using namespace fpdb::expression::gandiva;

namespace fpdb::executor::physical::file {

class FileScanPOp : public PhysicalOp {

public:
  FileScanPOp(std::string name,
       std::vector<std::string> columnNames,
       int nodeId,
		   const std::string &filePath,
		   std::shared_ptr<Table> table,
       std::unordered_map<std::string, std::vector<int>> linesToRead,
		   bool scanOnStart = false);
  FileScanPOp() = default;
  FileScanPOp(const FileScanPOp&) = default;
  FileScanPOp& operator=(const FileScanPOp&) = default;

  [[nodiscard]] bool isScanOnStart() const;

  void onReceive(const Envelope &message) override;
  void clear() override;
  std::string getTypeString() const override;
  void setPredicate(std::shared_ptr<fpdb::expression::gandiva::Expression> predicate);

private:
  void onStart();
  void onDebug();
  void onCacheLoadResponse(const ScanMessage &Message);
  void onComplete(const CompleteMessage &message);
  void onPredicate(const PredicateMessage &message);
  int fsend2model(std::string opName, uint64_t inputSize, int location, uint64_t inputRow, 
                uint64_t sampleTime, double percent); //FileScan->Filter
  uint64_t sampleFilter(std::shared_ptr<TupleSet> sampleSet);
  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet> &tupleSet);
  void readAndSendTuples(const std::vector<std::string> &columnNames);
  std::shared_ptr<TupleSet> readTuples();
  std::shared_ptr<TupleSet> readCSVFile(std::shared_ptr<arrow::io::ReadableFile> &arrowInputStream);
  std::shared_ptr<TupleSet> readCSVFile_randomAcross();
  std::shared_ptr<arrow::Table> strings_to_table(const std::vector<std::string>& strings);
  std::string readWithOffsets(const std::string& filePath, const std::vector<int>& offsets, int len);

  bool scanOnStart_;
  std::string filePath_;
  std::shared_ptr<Table> table_;

  shared_ptr<fpdb::expression::gandiva::Expression> predicate_;
  std::unordered_map<std::string, std::vector<int>> linesToRead_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, FileScanPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("predictor", op.predictors_),
                               f.field("samplers", op.samplers_),
                               f.field("Pred2link", op.Pred2link),
                               f.field("scanOnStart", op.scanOnStart_),
                               f.field("filePath", op.filePath_),
                               f.field("table", op.table_),
                               f.field("spawnOnRemote", op.shouldSpawnOnRemote_),
                               f.field("receiveTupleSet", op.receiveTupleSet),
                               f.field("sendToPred", op.sendToPred),
                               f.field("recvFromPred", op.recvFromPred),
                               f.field("link2Pred", op.link2Pred),
                               f.field("recvAllCompleteMsg", op.recvAllCompleteMsg_),
                               f.field("nextOpLocation", op.nextOpLocation_),
                               f.field("fileBeginIdx", op.fileBeginIdx_),
                               f.field("fileEndIdx", op.fileEndIdx_),
                               f.field("tupleBeginIdx", op.tupleBeginIdx_),
                               f.field("tupleEndIdx", op.tupleEndIdx_),
                               f.field("factor", op.factor_),
                               f.field("consumerfactor", op.consumerFactor_),
                               f.field("predicate", op.predicate_),
                               f.field("linesToRead", op.linesToRead_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANPOP_H
