//
// Created by Yifei Yang on 11/20/21.
//

#include <fpdb/executor/physical/sort/SortPOp.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/executor/physical/Globals.h>
#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>
#include <arrow/io/api.h>
#include <arrow/csv/options.h> 
#include <arrow/result.h>
#include <arrow/status.h>
#include <memory>                      // for make_unique, unique_ptr, __sha...
#include <utility>
#include <filesystem>
#include <arrow/csv/reader.h>
#include <arrow/io/buffered.h>
#include <arrow/io/memory.h>
#include <cstdlib>
#include "arrow/record_batch.h"
#include <fpdb/util/Util.h>
namespace fpdb::executor::physical::sort {

SortPOp::SortPOp(const string &name,
                 const vector<string> &projectColumnNames,
                 int nodeId,
                 const vector<SortKey> &sortKeys) :
  PhysicalOp(name, SORT, projectColumnNames, nodeId),
  sortKeys_(sortKeys) {}

std::string SortPOp::getTypeString() const {
  return "SortPOp";
}

void SortPOp::onReceive(const Envelope &message) {
  if (message.message().type() == MessageType::START) {
    this->onStart();
  } else if (message.message().type() == MessageType::TUPLE) {
    auto tupleMessage = dynamic_cast<const TupleMessage &>(message.message());
    this->onTuple(tupleMessage);
  } else if (message.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + message.message().getTypeString());
  }
}

void SortPOp::onStart() {
  for (std::string producer : this->producers()) {
   // if (producer.find("HashJoinBuild") == 0) {
      totBuild_++;
      ////std::cout<<"##### one producer"<<std::endl;
    //}
  }
  ////std::cout<<"#####all producer num: "<<totBuild_<<std::endl;
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void SortPOp::onComplete(const CompleteMessage &) {
  // std::cout<<"SortPOp::onComplete"<<std::endl;
  if(recvAllCompleteMsg()){
    if (!receiveTupleSet) {
#if OP_INFO>0
        if(op_stop_time_==0){
        op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
      }
#endif
      ctx()->notifyComplete();appendOpTime();
    } else {
#if RUNTIME_SAMPLE_EN > 0
      if(Pred2link&&(receivedTupleSet<2)){
        std::cout<<name()<<" dropSavedTuple"<<std::endl;
        dropSavedTuple();
      }
#endif
      const auto &sortedTupleSet = sort(true);
      send(true);
#if OP_INFO>0
        if(op_stop_time_==0){
        op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
      }
#endif
      ctx()->notifyComplete();appendOpTime();
    }
  }
}

void SortPOp::onTuple(const TupleMessage &message) {
#if OP_INFO>0
  	if(op_start_time_==0){
		op_start_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
	}
#endif
  receiveTupleSet = true;
  arrivedBuild_++;
#if OP_INFO>0
  input_row+=message.tuples()->numRows();
	input_size+=message.tuples()->size();
  if(input_row_first==0){
    input_row_first=message.tuples()->numRows();
  }
#endif
  buffer(message.tuples());
}

void SortPOp::makeArrowSortOptions() {
  if (!arrowSortOptions_.has_value()) {
    vector<arrow::compute::SortKey> arrowSortKeys;
    for (const auto &sortKey: sortKeys_) {
      const auto &name = sortKey.getName();
      auto direction = sortKey.getOrder() == plan::prephysical::ASCENDING ?
                       arrow::compute::SortOrder::Ascending : arrow::compute::SortOrder::Descending;
      arrowSortKeys.emplace_back(arrow::compute::SortKey(name, direction));
    }
    arrowSortOptions_ = arrow::compute::SortOptions(arrowSortKeys);
  }
}

void SortPOp::buffer(const shared_ptr<TupleSet> &tupleSet) {
#if RUNTIME_SAMPLE_EN > 0
  std::cout<<name()<<" tupleSet->numRows() before sort"<<tupleSet->numRows()<<std::endl;
  std::cout<<name()<<" SortPOp::onTuple, receivedTupleSet="<<receivedTupleSet<<",Pred2link="<<Pred2link<<",link2Pred="<<link2Pred<<std::endl;
  if(receivedTupleSet==1&&!sendSampleTimeToPred&&Pred2link){
    std::cout<<name()<<" SortPOp::onTuple, sample sort"<<std::endl;
    sendSampleTimeToPred=true;
    makeArrowSortOptions();
    uint64_t stTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
      const auto table = tupleSet->table();
      if (!arrowSortOptions_.has_value()) {
        ctx()->notifyError("Arrow SortOptions not set yet");
      }
      if (table->num_rows() > 0) {
        const auto &expSortIndices = arrow::compute::SortIndices(table, *arrowSortOptions_);
        if (!expSortIndices.ok()) {
          ctx()->notifyError(expSortIndices.status().message());
        }
        const auto &sortIndices = *expSortIndices;
        const auto expSortedTable = arrow::compute::Take(table, sortIndices);
        if (!expSortedTable.ok()) {
          ctx()->notifyError(expSortedTable.status().message());
        }
        // tupleSet=TupleSet::make((*expSortedTable).table());
      }
    uint64_t enTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    exectime_+=enTime-stTime;
    std::cout<<name()<<" consumer2predictor "<<std::endl;
    auto msg = std::make_shared<PredicateMessage>(name(),true,exectime_,nodeId_);
    ctx()->consumer2predictor(msg);
  }
std::cout<<name()<<" tupleSet->numRows() after sort"<<tupleSet->numRows()<<std::endl;
#endif
  // std::cout<<"SortPOp::buffer"<<std::endl;
  //行数为0的不进行处理, 因为拼接前后不变
  if (tupleSet->numRows() == 0) {
    if (!buffer_.has_value()) {
      buffer_ = tupleSet;
    }
    return;
  }
  if (!buffer_.has_value()) {
    buffer_ = tupleSet;
  } else {
    const auto &concatenateResult = TupleSet::concatenate({buffer_.value(), tupleSet});
    if (!concatenateResult.has_value()) {
      ctx()->notifyError(concatenateResult.error());
    }
    buffer_ = concatenateResult.value();
  }
  int64_t row=buffer_.value()->numRows();
  std::unordered_map<std::string, std::string> SortConfig = readConfig("sort.conf");
  std::string p=SortConfig["MYMEMORYSIZE"];
  int tmpPSize=std::stoi(p);
  MYMEMORYSIZE=tmpPSize*1024*1024*1024;//b
  if(LINESIZE==0){
    int linesize=(buffer_.value()->size())/(buffer_.value()->numRows());
    // std::cout<<"buffer size:"<<buffer_.value()->size()<<std::endl;
    // std::cout<<"buffer rows:"<<buffer_.value()->numRows()<<std::endl;
    // std::cout<<"linesize:"<<linesize<<" byte"<<std::endl;
    if(linesize==0)
      linesize=1;
    LINESIZE=linesize;
  }
  
  int64_t MEMORYLINE=MYMEMORYSIZE/(LINESIZE);
  // std::cout<<"MEMORYLINE:"<<MEMORYLINE<<" line"<<std::endl;
  // std::cout<<buffer_.value()->table()->ToString()<<std::endl;
  delta_=row;
  while(row>=MEMORYLINE){
    // std::cout<<name_+" slice and save to file begin,rows= "<<row<<std::endl;
    dumpToFile(buffer_.value()->slice(row-MEMORYLINE,MEMORYLINE));
    row-=MEMORYLINE;
    allRows+=MEMORYLINE;
    // std::cout<<name_+" slice and save to file end,rows= "<<row<<std::endl;
    
  }
  delta_-=row;
  buffer_=buffer_.value()->slice(0,row);
  // std::cout<<"buffer size:"<<buffer_.value()->size()<<std::endl;
  // std::cout<<buffer_.value()->table()->ToString()<<std::endl;
}

shared_ptr<TupleSet> SortPOp::sort(bool readyToRun) {
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
  uint64_t stTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif
  if(!readyToRun)
  {
    std::cout<<"not ready to Run"<<std::endl;
    return *buffer_;
  }
  // std::cout<<"SortPOp::sort"<<std::endl;
  int64_t filesNum=fileEndIdx_-fileBeginIdx_+1;
  if(filesNum==0){
      // std::cout<<"pure in memory sort"<<std::endl;
      if (!buffer_.has_value()) {
      return TupleSet::makeWithEmptyTable();
      }
      makeArrowSortOptions();
      const auto table = (*buffer_)->table();
      if (!arrowSortOptions_.has_value()) {
        ctx()->notifyError("Arrow SortOptions not set yet");
      }
      if (table->num_rows() == 0) {
        return *buffer_;
      }
      const auto &expSortIndices = arrow::compute::SortIndices(table, *arrowSortOptions_);
      if (!expSortIndices.ok()) {
        ctx()->notifyError(expSortIndices.status().message());
      }
      const auto &sortIndices = *expSortIndices;
      const auto expSortedTable = arrow::compute::Take(table, sortIndices);
      if (!expSortedTable.ok()) {
        ctx()->notifyError(expSortedTable.status().message());
      }
      buffer_=TupleSet::make((*expSortedTable).table());
      return *buffer_;
  }
  int64_t row=buffer_.value()->numRows();
  if(row>0){
    dumpToFile(buffer_.value()->slice(0,row));
    allRows+=row;
    row=0;
    buffer_=buffer_.value()->slice(0,row);
  }
  sort_single_file();
  if(filesNum>1){
  int64_t i=fileBeginIdx_;
  std::map<int,std::shared_ptr<arrow::csv::StreamingReader>> SRvec;
  SRvec.clear();
  std::map<int,std::shared_ptr<arrow::RecordBatch>> Batchvec;
  Batchvec.clear();
  int32_t block_size=1024;
  for(i=fileBeginIdx_;i<=fileEndIdx_;i++){
    std::unordered_map<std::string, std::string> pathConfig = readConfig("exec.conf");
    auto filePath = std::filesystem::absolute("/home/huazhang/Work/FPDB-build-"+pathConfig["CAF_SERVER_PORT"]+"/tmp/" + name_ + ".tmp." + std::to_string(i));
    auto expectedInFile = arrow::io::ReadableFile::Open(filePath);
    std::shared_ptr<arrow::io::InputStream> inputStream = *expectedInFile;
    auto ioContext = arrow::io::IOContext();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto read_options = arrow::csv::ReadOptions::Defaults();
    
    read_options.use_threads = false;
    read_options.block_size=block_size;//字节数
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    // Instantiate TableReader from input stream and options
    auto makeReaderResult = arrow::csv::StreamingReader::Make(ioContext,
                                                          inputStream,
                                                          read_options,
                                                          parse_options,
                                                          convert_options);
    if (!makeReaderResult.ok()) {
        //std::cout<<"StreamingReader fail"<<std::endl;
        return *buffer_;
    }
    ////std::cout<<"SRvec add pointer"<<std::endl;
    SRvec.insert(std::make_pair(i,*makeReaderResult));
  }
  std::vector<int> waitToclean;
  waitToclean.clear();
  while(filesNum>0){
    //std::cout<<"filesNum="<<filesNum<<std::endl;
    std::map<int,std::shared_ptr<arrow::csv::StreamingReader> >::iterator it;
    std::vector<std::shared_ptr<arrow::RecordBatch>> OneRowBatchVector;
    OneRowBatchVector.clear();
    for(it=SRvec.begin();it!=SRvec.end();it++){
      ////std::cout<<"traverse SRvec"<<std::endl;
      int j=it->first;
      std::shared_ptr<arrow::csv::StreamingReader> Reader=it->second;
      if(Batchvec.find(j)==Batchvec.end()||(Batchvec[j]->num_columns())==0){
        ////std::cout<<"fill Batchvec buffer"<<std::endl;
        std::shared_ptr<arrow::RecordBatch> Batch;
        arrow::Status status = Reader->ReadNext(&Batch);
        if (!status.ok()) {
          std::cout << "Failed to read RecordBatch. Exiting."<<std::endl;;
          continue;
        }
        if (Batch == NULL) {
            std::cout << "No more rows to read. Exiting."<<std::endl;;
            removeFile(j);
            filesNum--;
            waitToclean.push_back(j);
            continue;
        }
        Batchvec[j]=Batch;//override
        //std::cout << "Batchvec[j]:"<<Batch->ToString()<<std::endl;  
        
      }
      if(Batchvec.find(j)!=Batchvec.end()&&(Batchvec[j]->num_columns())!=0){
        std::shared_ptr<arrow::RecordBatch> OneRowBatch=Batchvec[j]->Slice(0,1);
        OneRowBatchVector.emplace_back(OneRowBatch);
      }
      // find minminal ,but how to compare according to sql?
      // better use arrow sort,only sort every first one in batch
      // findminnal and write to buffer_,decide whether to send
      // slice Batch,make the selected batch smaller
    }
    std::vector<int> ::iterator it2;
    for(it2=waitToclean.begin();it2!=waitToclean.end();it2++){
      SRvec.erase(*it2);
      //std::cout<<"waitToclean file:"<<*it2;
    }
    //std::cout<<" " <<std::endl;
    waitToclean.clear();
    if(filesNum==1){
    std::cout << "SRvec size qual to 1:"<<SRvec.size()<<std::endl;  
    for(it=SRvec.begin();it!=SRvec.end();it++){
      int j=it->first;
      std::shared_ptr<arrow::csv::StreamingReader> Reader=it->second;
      if(Batchvec.find(j)!=Batchvec.end()&&(Batchvec[j]->num_columns())!=0){
        //already read
        std::cout << "OneRowBatchVector size qual to 1:"<<OneRowBatchVector.size()<<std::endl;  
        OneRowBatchVector.clear();
        OneRowBatchVector.emplace_back(Batchvec[j]);
        Batchvec.erase(j);
        //not read
        while(1){
          std::shared_ptr<arrow::RecordBatch> Batch;
          arrow::Status status = Reader->ReadNext(&Batch);
          if (!status.ok()) {
            std::cout << "Failed to read RecordBatch. Exiting."<<std::endl;
            continue;
          }
          if (Batch == NULL) {
              std::cout << "in the last file No more rows to read. Exiting."<<std::endl;
              removeFile(j);
              filesNum--;
              break;
          }
          OneRowBatchVector.emplace_back(Batch);
        }
        shared_ptr<TupleSet> jointupleSet=TupleSet::make(*(arrow::Table::FromRecordBatches(OneRowBatchVector)));
        if (!buffer_.has_value()) {
              buffer_ = jointupleSet;
        } 
        else {
              const auto &concatenateResult = TupleSet::concatenate({buffer_.value(), jointupleSet});
              if (!concatenateResult.has_value()) {
                ctx()->notifyError(concatenateResult.error());
              }
              buffer_ = concatenateResult.value();
        }
        send(false);
      }
    }
    // std::cout << "return"<<std::endl;;
      return *buffer_;
    }
    // find min
    if(!OneRowBatchVector.empty()){
      arrow::Result<std::shared_ptr<arrow::Table> > oneRowTable= arrow::Table::FromRecordBatches(OneRowBatchVector);
      makeArrowSortOptions();
      const auto table=*oneRowTable;
      if (!arrowSortOptions_.has_value()) {
        ctx()->notifyError("Arrow SortOptions not set yet");
      }
      if (table->num_rows() == 0) {
        continue;
      }
      ////std::cout<<"oneRowTable"<<std::endl;
      ////std::cout<<table->ToString()<<std::endl;
      // Compute sort indices
      const auto &expSortIndices = arrow::compute::SortIndices(table, *arrowSortOptions_);
      if (!expSortIndices.ok()) {
        ctx()->notifyError(expSortIndices.status().message());
      }
      const auto &sortIndices = *expSortIndices;

      // Make sorted table using sort indices
      const auto expSortedTable = arrow::compute::Take(table, sortIndices);
      if (!expSortedTable.ok()) {
        ctx()->notifyError(expSortedTable.status().message());
      }

      shared_ptr<TupleSet> finTuple=TupleSet::make((*expSortedTable).table());
      ////std::cout<<"after OneRowTable sort once"<<std::endl;
      ////std::cout<<finTuple->table()->ToString()<<std::endl;
      std::shared_ptr<arrow::Table> finTable=finTuple->table();
      finTable=finTable->Slice(0,1);
      ////std::cout<<"after finTable Slice"<<std::endl;
      ////std::cout<<finTable->ToString()<<std::endl;
      for(it=SRvec.begin();it!=SRvec.end();it++){
        int j=it->first;
        if(Batchvec.find(j)!=Batchvec.end()&&(Batchvec[j]->num_columns())!=0){
          std::vector<std::shared_ptr<arrow::RecordBatch>> TmpBatchVector;
          TmpBatchVector.clear();
          TmpBatchVector.push_back(Batchvec[j]->Slice(0,1));
          arrow::Result<std::shared_ptr<arrow::Table> > TmpTable= arrow::Table::FromRecordBatches(TmpBatchVector);
          std::shared_ptr<arrow::Table> compTable=*TmpTable;
          if(compTable->Equals(*finTable,true)){
            ////std::cout<<"Table equal for file "<<j<<std::endl;
            //write to buffer_ and send
            shared_ptr<TupleSet> jointupleSet=TupleSet::make(finTable);
            if (!buffer_.has_value()) {
              buffer_ = jointupleSet;
            } else {
              const auto &concatenateResult = TupleSet::concatenate({buffer_.value(), jointupleSet});
              if (!concatenateResult.has_value()) {
                ctx()->notifyError(concatenateResult.error());
              }
              buffer_ = concatenateResult.value();
            }
            send(false);
            if(Batchvec[j]->num_rows()==1){
              Batchvec.erase(j);
            }
            else{
                Batchvec[j]=Batchvec[j]->Slice(1,Batchvec[j]->num_rows());
            }
          }

        }
      }
    }
    
  }
  }
  else{
    auto expTupleSet = readTuple();
    buffer_=expTupleSet;
    send(false);
  }
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
  uint64_t enTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  exectime_+=enTime-stTime;
#endif
  return *buffer_;
}

void SortPOp::clear() {
  buffer_ = nullopt;
  //? opContext_ = nullptr;
}

void SortPOp::send(bool force) {
    // std::cout<<"SortPOp::send"<<std::endl;
    if (buffer_!=nullopt && buffer_.has_value()) {
      auto numRows = buffer_.value()->numRows();
      std::unordered_map<std::string, std::string> SortConfig = readConfig("sort.conf");
      std::string p=SortConfig["MYMEMORYSIZE"];
      //std::cout<<"MYMEMORYSIZE:"<<p<<std::endl;
      MYMEMORYSIZE=std::stoi(p);
      // std::cout<<"LINESIZE:"<<LINESIZE<<std::endl;
      // std::cout<<"numRows:"<<numRows<<std::endl;
      if(LINESIZE==0) return ;
      if ((force && numRows > 0)|| numRows>=(MYMEMORYSIZE/(2*LINESIZE)) ) {
        auto sortedTupleSet = TupleSet::make(buffer_.value()->table());
        // Project using projectColumnNames
        auto expProjectTupleSet = sortedTupleSet->projectExist(getProjectColumnNames());
        if (!expProjectTupleSet) {
          throw std::runtime_error(expProjectTupleSet.error());
        }
        //std::cout<<"send TupleMessage"<<force<<std::endl;
        ////std::cout<<buffer_.value()->table()->ToString()<<std::endl;
        p=SortConfig["LOG"];
        int LOG=std::stoi(p);
        if(LOG==1){
          //for debug to check ans start
          std::unordered_map<std::string, std::string> pathConfig = readConfig("exec.conf");
          auto filePath = std::filesystem::absolute("/home/huazhang/Work/FPDB-build-"+pathConfig["CAF_SERVER_PORT"]+"/tmp/" + name_ + ".tmp.sortans");
          auto output = arrow::io::FileOutputStream::Open(filePath,true);
          auto write_options = arrow::csv::WriteOptions::Defaults();
          if (!arrow::csv::WriteCSV(*(buffer_.value()->table()), write_options, output->get()).ok()) {
            //Nothing happen
          }
          //for debug to check ans end
        }
#if RUNTIME_SAMPLE_EN > 0
    if(receivedTupleSet>=2){
      auto tupleSet=expProjectTupleSet.value();
      uint64_t numRows = tupleSet->numRows();
      uint64_t size = tupleSet->size();
      if (link2Pred&&!recvFromPred) {
        std::string target = "";
        for (auto consumer : consumers_) {
          if (consumer.find("[0]") != std::string::npos) {
            target = consumer;
            break;
          }
        }
        if (!recvFromPred && !sendToPred && numRows > 0&&iflock()) {
          auto msg = std::make_shared<PredicateMessage>(name(), target, nodeId_, numRows * consumerFactor_, size * consumerFactor_,consumerFactor_);
          ctx()->producer2predictor(msg);
          sendToPred = true;
        }
         if(!sendSampleTuple){//切一块数据作为采样数据
            std::cout<<name()<<" send sample 2 "<<std::endl;
            sendSampleTuple=true;
            std::unordered_map<std::string, std::string> pathConfig = readConfig("filescan.conf");
            uint16_t PROPORTION=std::stoi(pathConfig["PROPORTION"]);
            double sample_ratio = (1.0*PROPORTION)/100;
            uint64_t splitRows = (uint64_t)(sample_ratio * numRows);
            splitRows=std::max((uint64_t)1,splitRows);
            auto sampleTupleSet=tupleSet->slice(0, splitRows);
            tupleSet=tupleSet->slice(splitRows,numRows-splitRows);
            // send sample tuple to -1 & 0
            auto nodeSet=getNodeSet();
            for (const int& element : nodeSet) {	
              auto tmpTuple=sampleTupleSet->slice(0, sampleTupleSet->numRows());
			        std::shared_ptr<Message> sampleTupleMessage = std::make_shared<TupleMessage>(tmpTuple, this->name());		
              ctx()->tell(sampleTupleMessage, element);
            }
        }
      }
      if (link2Pred && !recvFromPred) {
        std::cout<<name()<<" save "<<std::endl;
        saveTuple(tupleSet);
      } else {
        std::cout<<name()<<" send "<<std::endl;
        shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(tupleSet, this->name());
        ctx()->tell(tupleMessage, nextOpLocation_);
      }
    }     
#else
      shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(expProjectTupleSet.value(), name());
      ctx()->tell(tupleMessage, nextOpLocation_);
#endif
        //buffer_ = nullopt;
        buffer_=buffer_.value()->slice(0,0);
      }
  }
}
void SortPOp::sort_single_file(){
  int unsortEnd=fileEndIdx_;
  while (fileBeginIdx_ <= unsortEnd) {
  auto expTupleSet = readTuple();
  // sort and save
  makeArrowSortOptions();
  // Check input buffer, arrow api will throw error if table has no rows, so we need a check
  const auto table = expTupleSet->table();
  if (!arrowSortOptions_.has_value()) {
    ctx()->notifyError("Arrow SortOptions not set yet");
  }
  if (table->num_rows() == 0) {
    continue;
  }

  // Compute sort indices
  const auto &expSortIndices = arrow::compute::SortIndices(table, *arrowSortOptions_);
  if (!expSortIndices.ok()) {
    ctx()->notifyError(expSortIndices.status().message());
  }
  const auto &sortIndices = *expSortIndices;

  // Make sorted table using sort indices
  const auto expSortedTable = arrow::compute::Take(table, sortIndices);
  if (!expSortedTable.ok()) {
    ctx()->notifyError(expSortedTable.status().message());
  }

  shared_ptr<TupleSet> finTuple=TupleSet::make((*expSortedTable).table());
  dumpToFile(finTuple);
  
  }
}

}
