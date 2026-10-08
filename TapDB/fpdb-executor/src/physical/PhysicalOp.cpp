//
// Created by matt on 5/12/19.
//

#include <fpdb/executor/physical/PhysicalOp.h>
#include <spdlog/spdlog.h>
#include <cassert>               // for assert
#include <utility>               // for move

#include <arrow/csv/options.h> 
#include <arrow/csv/reader.h>
#include <arrow/io/buffered.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/type_fwd.h>            // for default_memory_pool
#include <arrow/csv/api.h>
#include <algorithm>
using namespace fpdb::executor::message;

namespace fpdb::executor::physical {
std::mutex readwrite_mtx;
std::vector<std::pair<std::string, unsigned long long> > OpExecTimes;
uint64_t drop_bytes=0;
std::vector<std::string> cardinality_log;
std::mutex cardinality_mtx;
PhysicalOp::PhysicalOp(std::string name,
                       POpType type,
                       std::vector<std::string> projectColumnNames,
                       int nodeId,
                       bool shouldSpawnOnRemote) :
  name_(std::move(name)),
  type_(type),
  projectColumnNames_(std::move(projectColumnNames)),
  nodeId_(nodeId),
  shouldSpawnOnRemote_(shouldSpawnOnRemote) {
    receiveTupleSet = false;
  }

std::string mytoupper(std::string s){
  string res = "";
  size_t len = s.size();
  for(size_t i = 0; i < len; i++){
    if(s[i] >= 'a' && s[i] <= 'z') {
      s[i]-=32;
      res += s[i];
    } else {
    res += s[i];
    }
  }
  return res;
}

POpType PhysicalOp::getType() const {
  return type_;
}

std::string &PhysicalOp::name() {
  return name_;
}

const std::vector<std::string> &PhysicalOp::getProjectColumnNames() const {
  return projectColumnNames_;
}

int PhysicalOp::getNodeId() const {
  return nodeId_;
}

void PhysicalOp::produce(const std::shared_ptr<PhysicalOp> &op) {
  consumers_.emplace(op->name());
}

void PhysicalOp::consume(const std::shared_ptr<PhysicalOp> &op) {
  producers_.emplace(op->name());
}

void PhysicalOp::predict(const std::shared_ptr<PhysicalOp> &op) {
  link2Pred = true;
  predictors_.emplace(op->name());
}
void PhysicalOp::sample(const std::shared_ptr<PhysicalOp> &op) {//op的上游predicate op
  std::cout<<name()<<" sample "<<op->name()<<std::endl;
  Pred2link=true;
  samplers_.emplace(op->name());
}

std::set<std::string>PhysicalOp::consumers() {
  return consumers_;
}

std::set<std::string> PhysicalOp::producers() {
  return producers_;
}

std::set<std::string> PhysicalOp::predictors() {
  return predictors_;
}

std::set<std::string> PhysicalOp::samplers() {
  return samplers_;
}

std::shared_ptr<POpContext> PhysicalOp::ctx() {
  return opContext_;
}

unsigned long long PhysicalOp::getTime() {
  return startTime_;
}

const int PhysicalOp::getFileBeginIdx() const {
  return fileBeginIdx_;
}
const int PhysicalOp::getFileEndIdx() const {
  return fileEndIdx_;
}

bool PhysicalOp::recvAllCompleteMsg() {
  return !ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer);
}

bool PhysicalOp::readAllFile() {
  return fileBeginIdx_ > fileEndIdx_;
}

bool PhysicalOp::readAllSavedTuple() {
  return tupleBeginIdx_ > tupleEndIdx_;
}

void PhysicalOp::create(const std::shared_ptr<POpContext>& ctx) {
  assert (ctx);
  SPDLOG_DEBUG("Creating operator  |  name: '{}'", this->name_);
  opContext_ = ctx;
}

void PhysicalOp::setTime(unsigned long long beginTime) {
  startTime_ = beginTime;
}

void PhysicalOp::setName(const std::string &Name) {
  name_ = Name;
}

void PhysicalOp::setProjectColumnNames(const std::vector<std::string> &projectColumnNames) {
  projectColumnNames_ = projectColumnNames;
}

void PhysicalOp::destroyActor() {
  opContext_->destroyActorHandles();
}

long PhysicalOp::getQueryId() const {
  return queryId_;
}

void PhysicalOp::setQueryId(long queryId) {
  queryId_ = queryId;
}

bool PhysicalOp::spawnOnRemote() const {
  return shouldSpawnOnRemote_;
}

void PhysicalOp::setSpawnOnRemote(bool shouldSpawnOnRemote) {
  shouldSpawnOnRemote_ = shouldSpawnOnRemote;
}

} // namespace
void PhysicalOp::appendOpTime(){
#if OP_INFO>0
    std::string opname=name();
    if(exectime_==0&&input_row==0&&input_size==0)
      return ;
    std::lock_guard<std::mutex> lock(readwrite_mtx);

    std::string file_path = "/home/huazhang/Work/FPDB-build-4242/OpExecTimes.txt";
    std::ofstream output_file(file_path, std::ios::app);
    if (!output_file.is_open()) {
        std::cerr << "Failed to open file for writing." << std::endl;
        return;
    }
    if (output_file.tellp() == 0) {
        output_file << "Operator,Execution Time/ms,inputRow,inputSize/Byte,outputRow,outputSize/Byte,startTimepoint/ns,completeTimePoint/ns,completeTimePoint-startTimepoint/ns,input_row_first,receivedTupleSet,estimate_input_row,hashtable_inputRow,hashtable_inputSize/Byte,right_input_row_first,right_receivedTupleSet,right_estimate_input_row" << std::endl;
    }
    if(hashInput_row>0){
      if(opname.find("NestedLoopJoin")!=-1){
        receivedTupleSet=receivedTupleSet-right_receivedTupleSet;
      }
      if(opname.find("FileScan")!=-1){
        receivedTupleSet=std::max(receivedTupleSet,(long)(1));
      }
      output_file << opname << "," << exectime_ << "," <<input_row<< "," <<input_size<<"," <<output_row<< "," <<output_size<< "," <<op_start_time_<< "," <<op_stop_time_<< "," <<op_stop_time_-op_start_time_<<"," <<input_row_first<<","<<receivedTupleSet<<","<<input_row_first*receivedTupleSet<<","<<hashInput_row<< "," <<hashInput_size<<","<<right_input_row_first<<","<<right_receivedTupleSet<<","<<right_input_row_first*right_receivedTupleSet<<std::endl;
    }
    else{
      output_file << opname << "," << exectime_ << "," <<input_row<< "," <<input_size<<"," <<output_row<< "," <<output_size<< "," <<op_start_time_<< "," <<op_stop_time_<< "," <<op_stop_time_-op_start_time_<<","<<input_row_first<<","<<receivedTupleSet<<","<<input_row_first*receivedTupleSet<<std::endl;
    }
    
    output_file.close();
#endif
}
// void PhysicalOp::dumpOpTime(){
//   readwrite_mtx.lock();
//   std::string file_path = "/home/huazhang/Work/FPDB-build-4242/OpExecTimes.txt";
//   std::ofstream output_file(file_path);
//   if (!output_file.is_open()) {
//       std::cerr << "Failed to open file for writing." << std::endl;
//   }
//   output_file << "Operator, Execution Time/ms" << std::endl;
//   for (const auto& entry : OpExecTimes) {
//       output_file << entry.first << "," << entry.second << std::endl;
//   }
//   output_file.close();
//   readwrite_mtx.unlock();
// }
void PhysicalOp::dumpToFile(const shared_ptr<TupleSet>& tupleSet) {
  if (fileEndIdx_ == -1) {
    auto columnNames = tupleSet->columnNames();
    for (auto columnName : columnNames) {
      columnTypes_.emplace(mytoupper(columnName), tupleSet->schema()->GetFieldByName(columnName)->type());
    }
  }
  // std::cout << name_ + "dumpToFile begin" << std::endl;
  std::unordered_map<std::string, std::string> pathConfig = readConfig("exec.conf");
  auto filePath = std::filesystem::absolute("/home/huazhang/Work/FPDB-build-"+pathConfig["CAF_SERVER_PORT"]+"/tmp/" + name_ + ".tmp." + std::to_string(++fileEndIdx_));
  auto output = arrow::io::FileOutputStream::Open(filePath);
  auto write_options = arrow::csv::WriteOptions::Defaults();
  if (!arrow::csv::WriteCSV(*(tupleSet->table()), write_options, output->get()).ok()) {
    //Nothing happen
  }
  // std::cout << name_ + "dumpToFile end" << std::endl;
}

void PhysicalOp::removeFile(const int idx) {
  // std::cout << name_ + "removeFile begin" << std::endl;
  std::unordered_map<std::string, std::string> pathConfig = readConfig("exec.conf");
  std::string path = "/home/huazhang/Work/FPDB-build-"+pathConfig["CAF_SERVER_PORT"]+"/tmp/" + name_ + ".tmp." + std::to_string(idx);
  std::remove(path.c_str());
  // std::cout << name_ + "removeFile end" << std::endl;
}

shared_ptr<TupleSet> PhysicalOp::readTuple() {
  std::unordered_map<std::string, std::string> pathConfig = readConfig("exec.conf");
  auto filePath = std::filesystem::absolute("/home/huazhang/Work/FPDB-build-"+pathConfig["CAF_SERVER_PORT"]+"/tmp/" + name_ + ".tmp." + std::to_string(fileBeginIdx_));
  auto expectedInFile = arrow::io::ReadableFile::Open(filePath);
  auto inputStream = *expectedInFile;
  auto ioContext = arrow::io::IOContext();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto read_options = arrow::csv::ReadOptions::Defaults();
  read_options.use_threads = false;
  auto convert_options = arrow::csv::ConvertOptions::Defaults();
  convert_options.column_types = columnTypes_;

  // Instantiate TableReader from input stream and options
  auto makeReaderResult = arrow::csv::TableReader::Make(ioContext,
                                                        inputStream,
                                                        read_options,
                                                        parse_options,
                                                        convert_options);
  if (!makeReaderResult.ok()) {
    std::cout << "Could not create a table reader in" + name_ << std::endl;
  }
  std::shared_ptr<arrow::csv::TableReader> tableReader = *makeReaderResult;
  // Parse the payload and create the tupleset
  auto expTupleSet = TupleSet::make(tableReader);
  inputStream->Close();
  removeFile(fileBeginIdx_++);
  if (!expTupleSet.has_value()) {
      std::cout << "Unable to make TupleSet in " + name_ << std::endl;
      return nullptr;
  }

  return expTupleSet.value();
}

void PhysicalOp::sendSavedTuple() {
  while (tupleBeginIdx_ <= tupleEndIdx_) {
    std::unordered_map<std::string, std::string> pathConfig = readConfig("exec.conf");
    auto filePath = std::filesystem::absolute("/home/huazhang/Work/FPDB-build-"+pathConfig["CAF_SERVER_PORT"]+"/tmp/" + name_ + ".tuple." + std::to_string(tupleBeginIdx_));
    auto expectedInFile = arrow::io::ReadableFile::Open(filePath);
    auto inputStream = *expectedInFile;
    auto ioContext = arrow::io::IOContext();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto read_options = arrow::csv::ReadOptions::Defaults();
    read_options.use_threads = false;
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    // Instantiate TableReader from input stream and options
    auto makeReaderResult = arrow::csv::TableReader::Make(ioContext,
                                                          inputStream,
                                                          read_options,
                                                          parse_options,
                                                          convert_options);
    if (!makeReaderResult.ok()) {
      std::cout << "Could not create a table reader in" + name_ << std::endl;
    }
    std::shared_ptr<arrow::csv::TableReader> tableReader = *makeReaderResult;
    // Parse the payload and create the tupleset
    auto expTupleSet = TupleSet::make(tableReader);
    inputStream->Close();
    std::string delPath = "/home/huazhang/Work/FPDB-build-"+pathConfig["CAF_SERVER_PORT"]+"/tmp/" + name_ + ".tuple." + std::to_string(tupleBeginIdx_++);
    std::remove(delPath.c_str());
    if (!expTupleSet.has_value()) {
        std::cout << "Unable to make TupleSet in " + name_ << std::endl;
    }

    std::shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(expTupleSet.value(), name());
    ctx()->tell(tupleMessage, nextOpLocation_);
  }
}
void PhysicalOp::dropSavedTuple() {
  std::cout<<name()<<" not selected, drop result tuples"<<std::endl;
  while (tupleBeginIdx_ <= tupleEndIdx_) {
    std::unordered_map<std::string, std::string> pathConfig = readConfig("exec.conf");
    std::string delPath = "/home/huazhang/Work/FPDB-build-"+pathConfig["CAF_SERVER_PORT"]+"/tmp/" + name_ + ".tuple." + std::to_string(tupleBeginIdx_++);
    std::remove(delPath.c_str());
  }
  std::cout<<name()<<" clear SampleDataResult_"<<std::endl;
  if(SampleDataResult_)
    SampleDataResult_->clear();
  std::cout<<name()<<" clear SampleDataResult_ end"<<std::endl;
  int64_t filesNum=fileEndIdx_-fileBeginIdx_+1;
  if(filesNum>0){
    for(int i=fileBeginIdx_;i<=fileEndIdx_;i++){
    removeFile(i);
    }
  }
  std::cout<<name()<<" drop tuple file"<<std::endl;

}
void PhysicalOp::saveTuple(const shared_ptr<TupleSet>& tupleSet) {
  std::unordered_map<std::string, std::string> pathConfig = readConfig("exec.conf");
  auto filePath = std::filesystem::absolute("/home/huazhang/Work/FPDB-build-"+pathConfig["CAF_SERVER_PORT"]+"/tmp/" + name_ + ".tuple." + std::to_string(++tupleEndIdx_));
  auto output = arrow::io::FileOutputStream::Open(filePath);
  auto write_options = arrow::csv::WriteOptions::Defaults();
  if (!arrow::csv::WriteCSV(*(tupleSet->table()), write_options, output->get()).ok()) {
    //Nothing happen
  }
}

void PhysicalOp::send(bool force) {
  //Noop
}

void PhysicalOp::setFactor(int factor) {
  factor_ = factor;
}

void PhysicalOp::setNextFactor(int factor) {
  nextFactor_ = factor;
}
int PhysicalOp::getNextFactor() {
  return nextFactor_;
}

int PhysicalOp::getFactor() {
  return factor_;
}

void PhysicalOp::setConsumerFactor(int factor) {
  consumerFactor_=factor;
}

int PhysicalOp::getConsumerFactor() {
  return consumerFactor_;
}

void PhysicalOp::setLoopJoinFactor(int factor) {
  nestLoopJoin_left_factor_ = factor;
}
int PhysicalOp::getLoopJoinFactor() {
  return nestLoopJoin_left_factor_;
}
std::set<int> PhysicalOp::getNodeSet() const{
  return nodeSet_;
}
bool PhysicalOp::iflock(){
    return true;
}
bool PhysicalOp::ifunlock(std::string sender){
  return true;
}
int PhysicalOp::maxReceivedFormProducer(){
  int max_value=-1;
  for (const auto& pair : receivedTupleSetMap) {
        if (pair.second > max_value) {
            max_value = pair.second;
        }
    }
  return max_value;
}
void PhysicalOp::sendSampleTime(){
#if (PREDICATE_EN>0) || (RUNTIME_SAMPLE_EN>0)
//采样join结果未必有数据
  std::cout<<name()<<" HashJoinProbePOp::sendSampleTime(), maxReceivedFormProducer()="<<maxReceivedFormProducer()<<",Pred2link="<<Pred2link<<",link2Pred="<<link2Pred<<std::endl;
  if(maxReceivedFormProducer()==1&&!sendSampleTimeToPred&&Pred2link){
    sendSampleTimeToPred=true;
    std::cout<<name()<<" consumer2predictor "<<std::endl;
    auto msg = std::make_shared<PredicateMessage>(name(),true,exectime_,nodeId_);
    ctx()->consumer2predictor(msg);
  }
#endif
}