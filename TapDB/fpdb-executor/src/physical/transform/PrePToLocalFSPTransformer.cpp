//
// Created by Partho on 3/26/22.
//

#include <fpdb/executor/physical/transform/PrePToLocalFSPTransformer.h>
#include <fpdb/executor/physical/prune/PartitionPruner.h>
#include <fpdb/executor/physical/file/FileScanPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/predicate/PredicatePOp.h>
#include <fpdb/executor/physical/cache/CacheLoadPOp.h>
#include <fpdb/executor/physical/merge/MergePOp.h>
#include <fpdb/catalogue/local-fs/LocalFSTable.h>
#include <fpdb/util/Util.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <filesystem>
#include <algorithm>
#include <random>
#include <unordered_map>
#include <numeric>
using namespace fpdb::catalogue::local_fs;
using namespace fpdb::util;

namespace fpdb::executor::physical {

PrePToLocalFSPTransformer::PrePToLocalFSPTransformer(uint prePOpId,
                                           int numNodes) :
  prePOpId_(prePOpId),
  numNodes_(numNodes) {}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToLocalFSPTransformer::transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp) {
  const auto &localFsTable = std::static_pointer_cast<LocalFSTable>(filterableScanPrePOp->getTable());
  const auto &partitions = (const vector<shared_ptr<Partition>> &) localFsTable->getLocalFSPartitions();
  const auto &partitionPredicates = PartitionPruner::prune(partitions, filterableScanPrePOp->getPredicate());
  vector<string> projectColumnNames{filterableScanPrePOp->getProjectColumnNames().begin(),
                                    filterableScanPrePOp->getProjectColumnNames().end()};

  return transformFilterableScan_(filterableScanPrePOp, partitionPredicates, projectColumnNames);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToLocalFSPTransformer::transformFilterableScan_(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                                    const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates,
                                                    const vector<string> &projectColumnNames) {
  vector<shared_ptr<PhysicalOp>> scanPOps, filterPOps, predPOps;
  const auto &table = filterableScanPrePOp->getTable();

#if RANDOM_FILESCAN>0
  uint64_t stTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  uint64_t prepare_time=0;
  // std::vector<std::string> filePaths; filePaths.clear();
  uint64_t numParts=partitionPredicates.size();
  std::unordered_map<std::string, std::string> pathConfig = readConfig("filescan.conf");
  uint16_t LENGTH=std::stoi(pathConfig["LENGTH"]);
  std::vector<std::unordered_map<std::string, std::vector<int> >> parts(numParts);
  std::unordered_map<std::string, std::string> SizeConfig = readConfig("hefeiFileSize.conf");
  
  for (const auto &partitionPredicateIt: partitionPredicates){
    const auto &localFsPartition = static_pointer_cast<LocalFSPartition>(partitionPredicateIt.first);
    const auto &localFsObject = localFsPartition->getObject();
    // filePaths.push_back(localFsObject);
    uint64_t filesize=std::stoi(SizeConfig[localFsObject]);
    std::vector<uint64_t> offsets;
    uint64_t len=LENGTH;
    uint64_t index=0;
    for (uint64_t offset = 0; offset < filesize; offset += len) {
        parts[index % numParts][localFsObject].push_back(offset);
        index++;
    }
  }
  //shuffle method1
  // std::random_device rd;
  // std::mt19937 g(rd());
  // std::shuffle(lineNumbers.begin(), lineNumbers.end(), g);

  //shuffle method2
  // std::random_device rd;
  // std::mt19937 g(rd());
  // for (int i = lineNumbers.size() - 1; i > 0; --i) {
  //     std::uniform_int_distribution<int> distribution(0, i);
  //     int j = distribution(g);
  //     std::swap(lineNumbers[i], lineNumbers[j]);
  // }

  // MOD hash function

  uint64_t enTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  prepare_time+=(enTime-stTime)/1e6;
  std::cout<<"transformFilterableScan random filescan prepare_time/ms="<<prepare_time<<std::endl;
#else
  uint64_t numParts=partitionPredicates.size();
  std::vector<std::unordered_map<std::string, std::vector<int> >> parts(numParts);
#endif
  /**
   * For each partition, construct:
   * a FileScan, a Filter if needed
   */
  uint partitionId = 0;
  for (const auto &partitionPredicateIt: partitionPredicates) {
    const auto &localFsPartition = static_pointer_cast<LocalFSPartition>(partitionPredicateIt.first);
    const auto &predicate = partitionPredicateIt.second;
    const auto &localFsObject = localFsPartition->getObject();
    const auto &localFsObjectName = localFsPartition->getObjectName();

    // project column names and its union with project column names
    vector<string> predicateColumnNames;
    if (predicate) {
      const auto predicateColumnNameSet = predicate->involvedColumnNames();
      predicateColumnNames.assign(predicateColumnNameSet.begin(), predicateColumnNameSet.end());
    }
    const auto &projPredColumnNames = union_(projectColumnNames, predicateColumnNames);
    // file scan
    std::pair<bool, std::string> remoteSpawnInfo = make_pair(true, "_onRemote");
    const auto &scanPOp = make_shared<file::FileScanPOp>(fmt::format("FileScan[{}]-{}-pre[{}]", partitionId % numNodes_, localFsObjectName, prePOpId_),
                                                   projPredColumnNames,
                                                   partitionId % numNodes_,
                                                   localFsObject,
                                                   table,
                                                   parts[partitionId],
                                                   true);
    
    scanPOp->setSpawnOnRemote(remoteSpawnInfo.first);  
    scanPOps.emplace_back(scanPOp);

#if PRINT_PHY_PLAN > 0
    std::string prettyName;
    auto loc = localFsObjectName.find_last_of(".tbl");
    if (loc == localFsObjectName.length() - 1) {
      prettyName = fmt::format("FileScan-{}", localFsObjectName.substr(0, loc - 3));
    } else {
      prettyName = fmt::format("FileScan-{}", localFsObjectName.substr(0, loc - 4));
    }
    op2name[scanPOp->name()] = prettyName;
#endif

    // filter
    if (predicate) {
#if (PREDICATE_EN>0) || (RUNTIME_SAMPLE_EN>0)
    auto pred = make_shared<predicate::PredicatePOp>(
          fmt::format("Predicate[{}]-{}-pre[{}]", -1, "Filter-"+localFsObjectName, prePOpId_),
          projectColumnNames,
          -1,
          POpType::FILTER);
    predPOps.emplace_back(pred);
#endif
      for (int i = -1; i < numNodes_; i++) {
        const auto &filterPOp = make_shared<filter::FilterPOp>(fmt::format("Filter[{}]-{}-pre[{}]", i, localFsObjectName, prePOpId_),
                                                              projectColumnNames,
                                                              i,
                                                              predicate,
                                                              table);
        filterPOp->setSpawnOnRemote(remoteSpawnInfo.first); 
        filterPOps.emplace_back(filterPOp);
        scanPOp->produce(filterPOp);
        filterPOp->consume(scanPOp);
#if (PREDICATE_EN>0) || (RUNTIME_SAMPLE_EN>0)
        scanPOp->predict(pred);
        pred->consume(scanPOp);
#endif
#if RUNTIME_SAMPLE_EN>0
        filterPOp->sample(pred);
#endif
        scanPOp->setPredicate(predicate);

#if PRINT_PHY_PLAN > 0
        std::string prettyName;
        if (loc == localFsObjectName.length() - 1) {
          prettyName = fmt::format("Filter-{}", localFsObjectName.substr(0, loc - 3));
        } else {
          prettyName = fmt::format("Filter-{}", localFsObjectName.substr(0, loc - 4));
        }
        op2name[filterPOp->name()] = prettyName;
        producer2consumer[op2name[scanPOp->name()]] = prettyName;
#endif
      }
    }

    ++partitionId;
  }
  

  //Set factor_
  for (auto op : scanPOps) {
    op->setFactor(0); //预期接收0个TupleSet
    op->setNextFactor(1);
  }
  if (!filterPOps.empty()) {
    for (auto op : filterPOps) {
      op->setFactor(1); //预期接收1个TupleSet
      op->setNextFactor(1); //与FileScan的保持一致
    }
  }

  if (filterPOps.empty()) {
    return make_pair(scanPOps, scanPOps);
  } else {
    vector<shared_ptr<PhysicalOp>> allPOps;
    allPOps.insert(allPOps.end(), scanPOps.begin(), scanPOps.end());
    allPOps.insert(allPOps.end(), filterPOps.begin(), filterPOps.end());
    allPOps.insert(allPOps.end(), predPOps.begin(), predPOps.end());
    return make_pair(filterPOps, allPOps);
  }
}

#if PRINT_PHY_PLAN > 0
std::unordered_map<std::string, std::string> PrePToLocalFSPTransformer::getOp2Name() {
  return op2name;
}
std::unordered_map<std::string, std::string> PrePToLocalFSPTransformer::getProducer2Consumer() {
  return producer2consumer;
}
#endif //PRINT_PHY_PLAN > 0
}
