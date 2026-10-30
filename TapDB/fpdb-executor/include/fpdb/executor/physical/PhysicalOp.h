//
// Created by matt on 5/12/19.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PHYSICALOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PHYSICALOP_H

#include <fpdb/executor/physical/Forward.h>
#include <fpdb/executor/physical/POpContext.h>
#include <fpdb/executor/physical/POpType.h>
#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/executor/message/Envelope.h>
#include <caf/all.hpp>
#include <string>
#include <memory>
#include <map>
#include <atomic>
#include <mutex>

#include <arrow/csv/options.h> 
#include <arrow/csv/reader.h>
#include <arrow/io/buffered.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/type_fwd.h>            // for default_memory_pool
#include <arrow/csv/api.h>
#include <thread>
#define PREDICATE_EN 0
#define FINE_GRAIN 0
#define PRINT_PHY_PLAN 0
#define RANDOM_FILESCAN 0
#define OP_INFO 1
#define RUNTIME_SAMPLE_EN 1
namespace shiftdb::native { class TapDBStateCodec; }

namespace fpdb::executor::physical {
extern std::mutex readwrite_mtx;
extern std::vector<std::pair<std::string, unsigned long long> > OpExecTimes;
extern uint64_t drop_bytes;
extern std::vector<std::string> cardinality_log;
extern std::mutex cardinality_mtx;
/**
 * Base class for physical operators
 */
class PhysicalOp {

public:
#ifdef FPDB_ENABLE_SHIFTDB
  void shiftReceivedInput(bool value) { receiveTupleSet = value; }
  bool shiftPredicateManaged() const { return Pred2link || link2Pred; }
#endif

  explicit PhysicalOp(std::string name,
                      POpType type,
                      std::vector<std::string> projectColumnNames,
                      int nodeId,
                      bool shouldSpawnOnRemote = false);
  PhysicalOp() = default;
  PhysicalOp(const PhysicalOp&) = default;
  PhysicalOp& operator=(const PhysicalOp&) = default;
  virtual ~PhysicalOp() = default;

  // getters
  std::string &name();
  POpType getType() const;
  virtual std::string getTypeString() const = 0;
  const std::vector<std::string> &getProjectColumnNames() const;
  int getNodeId() const;
  long getQueryId() const;
  bool spawnOnRemote() const;
  std::set<std::string> producers();
  std::set<std::string> consumers();
  std::set<std::string> predictors();
  std::set<std::string> samplers();
  std::shared_ptr<POpContext> ctx();
  unsigned long long getTime();

  // setters
  void setName(const std::string &Name);
  void setProjectColumnNames(const std::vector<std::string> &projectColumnNames);
  void setQueryId(long queryId);
  void setSpawnOnRemote(bool shouldSpawnOnRemote);
  virtual void produce(const std::shared_ptr<PhysicalOp> &op);
  virtual void consume(const std::shared_ptr<PhysicalOp> &op);
  virtual void predict(const std::shared_ptr<PhysicalOp> &op);
  virtual void sample(const std::shared_ptr<PhysicalOp> &op);
  void create(const std::shared_ptr<POpContext>& ctx);
  void setTime(unsigned long long beginTime);

  virtual void onReceive(const fpdb::executor::message::Envelope &msg) = 0;
  virtual void clear() = 0;
  void destroyActor();
  virtual void send(bool force);

  //Save received tuple sets in disk.
  std::shared_ptr<TupleSet> readTuple();
  void removeFile(const int idx);
  void dumpToFile(const std::shared_ptr<TupleSet>& tupleSet);

  //Save the tuple sets to be sent to disk.
  virtual void sendSavedTuple();
  void saveTuple(const std::shared_ptr<TupleSet>& tupleSet);
  void dropSavedTuple();
  const int getFileBeginIdx() const;
  const int getFileEndIdx() const;

  bool recvAllCompleteMsg();
  bool readAllFile();
  bool readAllSavedTuple();
  
  void setFactor(int factor);
  int getFactor();

  void setLoopJoinFactor(int factor);
  int getLoopJoinFactor();

  void setNextFactor(int factor);
  int getNextFactor();

  void setConsumerFactor(int factor);
  int getConsumerFactor();

  long receivedTupleSet; //收到的TupleSet数目
  void appendOpTime();
  std::map<std::string,int> receivedTupleSetMap;
  std::set<int> getNodeSet() const;
  bool iflock();
  bool ifunlock(std::string sender);
  int maxReceivedFormProducer();
  void sendSampleTime();

protected:
  friend class ::shiftdb::native::TapDBStateCodec;
  std::string name_;
  POpType type_;
  std::vector<std::string> projectColumnNames_;
  int nodeId_;
  long queryId_;
  std::shared_ptr<POpContext> opContext_;
  std::set<std::string> producers_;
  std::set<std::string> consumers_;
  std::set<std::string> predictors_;
  std::set<std::string> samplers_;
  bool shouldSpawnOnRemote_;
  unsigned long long startTime_;
  int delta_=0;

  bool receiveTupleSet = false;
  bool sendToPred = false;
  bool recvFromPred = false;
  bool link2Pred = false;
  bool sendSampleTimeToPred=false;
  bool sendSampleTuple=false;
  bool Pred2link = false;//此op是由上游predicate op决定位置的

  bool recvAllCompleteMsg_ = false;

  int nextOpLocation_ = -1; //The location of the next operator, default value is -1, namely client.

  int fileBeginIdx_ = 0;
  int fileEndIdx_ = -1; //[fileBeginIdx_, fileEndIdx_]

  int tupleBeginIdx_ = 0;
  int tupleEndIdx_ = -1; //[tupleEndIdx_, tupleBeginIdx_]

  int factor_; //预期接收的TupleSet数目
  int nestLoopJoin_left_factor_; //nestLoopJoin预期接收的小表TupleSet数目
  int nextFactor_; //总的发送TupleSet数目
  int consumerFactor_;// 下游将会接收的TupleSet数目

  std::unordered_map<std::string, std::shared_ptr<::arrow::DataType>> columnTypes_;
  unsigned long long exectime_=0;
  unsigned long long input_row=0;
  unsigned long long input_size=0;
  unsigned long long hashInput_row=0;
  unsigned long long hashInput_size=0;
  unsigned long long op_start_time_=0;
  unsigned long long op_stop_time_=0;
  unsigned long long output_row=0;
  unsigned long long output_size=0;
  unsigned long long input_row_first=0;// default on left
  unsigned long long right_input_row_first=0;
  long right_receivedTupleSet=0; //收到的right TupleSet数目
  std::set<int> nodeSet_ = {0, -1};// -1 local client,0 remote
  std::shared_ptr<TupleSet> SampleDataResult_;
};

} // namespace

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PHYSICALOP_H
