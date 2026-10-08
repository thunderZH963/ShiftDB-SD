//
// Created by Yifei Yang on 11/20/21.
//

#include <fpdb/executor/physical/transform/PrePToPTransformer.h>
#include <fpdb/executor/physical/transform/PrePToLocalFSPTransformer.h>
#include <fpdb/executor/physical/sort/SortPOp.h>
#include <fpdb/executor/physical/limitsort/LimitSortPOp.h>
#include <fpdb/executor/physical/aggregate/AggregatePOp.h>
#include <fpdb/executor/physical/aggregate/function/Sum.h>
#include <fpdb/executor/physical/aggregate/function/Count.h>
#include <fpdb/executor/physical/aggregate/function/MinMax.h>
#include <fpdb/executor/physical/aggregate/function/Avg.h>
#include <fpdb/executor/physical/aggregate/function/AvgReduce.h>
#include <fpdb/executor/physical/predicate/PredicatePOp.h>
#include <fpdb/executor/physical/POpType.h>
#include <fpdb/executor/physical/group/GroupPOp.h>
#include <fpdb/executor/physical/project/ProjectPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinBuildPOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinProbePOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <fpdb/executor/physical/join/nestedloopjoin/NestedLoopJoinPOp.h>
#include <fpdb/executor/physical/shuffle/ShufflePOp.h>
#include <fpdb/executor/physical/split/SplitPOp.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/executor/physical/test/ReceivePOp.h>
#include <fpdb/executor/physical/test/SendPOp.h>
#include <fpdb/expression/gandiva/Column.h>

#include <unordered_set>
#include <unordered_map>

namespace fpdb::executor::physical {

PrePToPTransformer::PrePToPTransformer(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                       const shared_ptr<AWSClient> &awsClient,
                                       const shared_ptr<Mode> &mode,
                                       int parallelDegree,
                                       int numNodes) :
  prePhysicalPlan_(prePhysicalPlan),
  awsClient_(awsClient),
  mode_(mode),
  parallelDegree_(parallelDegree),
  numNodes_(numNodes) {}

shared_ptr<PhysicalPlan> PrePToPTransformer::transform() {
  // transform from root in dfs
  auto rootTransRes = transformDfs(prePhysicalPlan_->getRootOp());
  // auto rootTransRes = transformDebug();
  auto upConnPOps = rootTransRes.first;
  auto allPOps = rootTransRes.second;

  // make a collate operator
  shared_ptr<PhysicalOp> collatePOp = make_shared<collate::CollatePOp>(
          "collate",
          ColumnName::canonicalize(prePhysicalPlan_->getOutputColumnNames()),
          -1); //spawn on local
  allPOps.emplace_back(collatePOp);
  connectManyToOne(upConnPOps, collatePOp);
#if PRINT_PHY_PLAN > 0
  std::unordered_map<std::string, std::shared_ptr<PhysicalOp>> name2op;
  for (auto op : allPOps) {
    name2op[op->name()] = op;
  }
  ofstream outfile("/home/huazhang/Work/tmp/physical_plan", ios::out );
  for (auto op : allPOps) {
    if (op->getType() == POpType::FILE_SCAN || op->getNodeId() == 0) {
      outfile << "Head: " + op->name() <<"\n";
      for (auto consumer : op->consumers()) {
        if (name2op[consumer]->getNodeId() == 0) {
          outfile << "\tSub: " + name2op[consumer]->name() << "\n";
        }
      }
    }
  }
/*
  for (auto &it : producer2consumer) {
    std::cout << it.first + "->" + it.second << std::endl;
  }
*/
#endif

  return make_shared<PhysicalPlan>(allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformDebug() {
  vector<shared_ptr<PhysicalOp>> selfPops;
  vector<shared_ptr<PhysicalOp>> allPops;
  std::vector<std::string> columnNames;
  for (int i = 0; i < parallelDegree_; i++) {
    const auto &receivePOp = make_shared<test::ReceivePOp>(fmt::format("Receive{}[{}]-{}", "", -1, i),
                                                          columnNames,
                                                          -1);
    const auto &sendPOp = make_shared<test::SendPOp>(fmt::format("Send{}[{}]-{}", "_onRemote", -1, i),
                                                          columnNames,
                                                          -1);
    receivePOp->setSpawnOnRemote(false);
    sendPOp->setSpawnOnRemote(true);
    receivePOp->consume(sendPOp);
    sendPOp->produce(receivePOp);
    selfPops.emplace_back(receivePOp);
    allPops.emplace_back(sendPOp), allPops.emplace_back(receivePOp);
  }
  return make_pair(selfPops, allPops);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformDfs(const shared_ptr<PrePhysicalOp> &prePOp) {
  std::cout<<"transformDfs "<<prePOp->getTypeString()<<std::endl;
  switch (prePOp->getType()) {
    case PrePOpType::SORT: {
      const auto &sortPrePOp = std::static_pointer_cast<SortPrePOp>(prePOp);
      return transformSort(sortPrePOp);
    }
    case PrePOpType::LIMIT_SORT: {
      const auto &limitSortPrePOp = std::static_pointer_cast<LimitSortPrePOp>(prePOp);
      return transformLimitSort(limitSortPrePOp);
    }
    case PrePOpType::AGGREGATE: {
      const auto &aggregatePrePOp = std::static_pointer_cast<AggregatePrePOp>(prePOp);
      return transformAggregate(aggregatePrePOp);
    }
    case PrePOpType::GROUP: {
      const auto &groupPrePOp = std::static_pointer_cast<GroupPrePOp>(prePOp);
      return transformGroup(groupPrePOp);
    }
    case PrePOpType::PROJECT: {
      const auto &projectPrePOp = std::static_pointer_cast<ProjectPrePOp>(prePOp);
      return transformProject(projectPrePOp);
    }
    case PrePOpType::FILTER: {
      const auto &filterPrePOp = std::static_pointer_cast<FilterPrePOp>(prePOp);
      return transformFilter(filterPrePOp);
    }
    case PrePOpType::HASH_JOIN: {
      const auto &hashJoinPrePOp = std::static_pointer_cast<HashJoinPrePOp>(prePOp);
      return transformHashJoin(hashJoinPrePOp);
    }
    case PrePOpType::NESTED_LOOP_JOIN: {
      const auto &nestedLoopJoinPrePOp = std::static_pointer_cast<NestedLoopJoinPrePOp>(prePOp);
      return transformNestedLoopJoin(nestedLoopJoinPrePOp);
    }
    case PrePOpType::FILTERABLE_SCAN: {
      const auto &filterableScanPrePOp = std::static_pointer_cast<FilterableScanPrePOp>(prePOp);
      return transformFilterableScan(filterableScanPrePOp);
    }
    default: {
      std::cout<<"not processing "<<prePOp->getTypeString()<<std::endl;
      throw runtime_error(fmt::format("Unsupported prephysical operator type: {}", prePOp->getTypeString()));
    }
  }
}

vector<pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>>
PrePToPTransformer::transformProducers(const shared_ptr<PrePhysicalOp> &prePOp) {
  vector<pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>> transformRes;
  for (const auto &producer: prePOp->getProducers()) {
    transformRes.emplace_back(transformDfs(producer));
  }
  return transformRes;
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformSort(const shared_ptr<SortPrePOp> &sortPrePOp) {
  // id
  auto prePOpId = sortPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(sortPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for sort, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<string> projectColumnNames{sortPrePOp->getProjectColumnNames().begin(),
                                    sortPrePOp->getProjectColumnNames().end()};

#if (PREDICATE_EN>0) || (RUNTIME_SAMPLE_EN>0)
  auto pred = make_shared<predicate::PredicatePOp>(
          fmt::format("Predicate[{}]-{}-pre[{}]", -1, "SORT", prePOpId),
          projectColumnNames,
          -1,
          POpType::SORT);
  allPOps.emplace_back(pred);
  for (auto op : upConnPOps) {
    op->predict(pred);
    pred->consume(op);
  }
#endif

  vector<shared_ptr<PhysicalOp>> sortPOps;
  int nextFactor = 0;
  for (auto op : upConnPOps) {
    if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){//排除-1上的op，只使用一侧的op拓扑
      continue;
    }
    nextFactor+=op->getNextFactor();
    std::cout<<op->name()<<"Sort,nextfactor="<<op->getNextFactor()<<std::endl;
  }
  for (int i = -1; i < numNodes_; i++) {
    auto sortPOp = make_shared<sort::SortPOp>(fmt::format("Sort[{}]-pre[{}]", i, prePOpId),
                                                      projectColumnNames,
                                                      i,
                                                      sortPrePOp->getSortKeys());
    sortPOps.emplace_back(sortPOp);
    if (upConnPOps[0]->getType() == POpType::FILE_SCAN || upConnPOps[0]->getType() == POpType::FILTER ||
        upConnPOps[0]->getType() == POpType::AGGREGATE) {
      //Set factor_
      sortPOp->setFactor(nextFactor);
      sortPOp->setNextFactor(1);
    } else if (upConnPOps[0]->getType() == POpType::HASH_JOIN_PROBE || upConnPOps[0]->getType() == POpType::GROUP) {
      sortPOp->setFactor(nextFactor); //会汇总HashJoinProbe/Aggregate/Group
      sortPOp->setNextFactor(1);
    }
    else{
      sortPOp->setFactor(nextFactor);
      sortPOp->setNextFactor(1);
    }
#if PRINT_PHY_PLAN > 0
    auto prettyName = "Sort";
    op2name[sortPOp->name()] = prettyName;
    producer2consumer[op2name[upConnPOps[0]->name()]] = prettyName;
#endif
#if RUNTIME_SAMPLE_EN > 0
  sortPOp->sample(pred);
#endif
  }

  allPOps.insert(allPOps.end(), sortPOps.begin(), sortPOps.end());

  // connect to upstream
  connectManyToMany(upConnPOps, sortPOps);

  return make_pair(sortPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformLimitSort(const shared_ptr<LimitSortPrePOp> &limitSortPrePOp) {
  // id
  auto prePOpId = limitSortPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(limitSortPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for limitSort, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<string> projectColumnNames{limitSortPrePOp->getProjectColumnNames().begin(),
                                    limitSortPrePOp->getProjectColumnNames().end()};

  vector<shared_ptr<PhysicalOp>> sortPOps;
  int nextFactor = 0;
  for (auto op : upConnPOps) {
    if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
      continue;
    }
    nextFactor+=op->getNextFactor();
  }
  for (int i = -1; i < numNodes_; i++) {
    auto sortPOp = make_shared<limitsort::LimitSortPOp>(fmt::format("LimitSort[{}]-pre[{}]", i, prePOpId),
                                                        projectColumnNames,
                                                        i,
                                                        limitSortPrePOp->getK(),
                                                        limitSortPrePOp->getSortKeys());
    sortPOps.emplace_back(sortPOp);
    if (upConnPOps[0]->getType() == POpType::FILE_SCAN || upConnPOps[0]->getType() == POpType::FILTER ||
        upConnPOps[0]->getType() == POpType::AGGREGATE) {
      //Set factor_
      sortPOp->setFactor(nextFactor);
      sortPOp->setNextFactor(1);
    } else if (upConnPOps[0]->getType() == POpType::HASH_JOIN_PROBE || upConnPOps[0]->getType() == POpType::GROUP) {
      sortPOp->setFactor(nextFactor); //会汇总HashJoinProbe/Aggregate/Group
      sortPOp->setNextFactor(1);
    }
    else{
      sortPOp->setFactor(nextFactor);
      sortPOp->setNextFactor(1);
    }
  }

  allPOps.insert(allPOps.end(), sortPOps.begin(), sortPOps.end());

  // connect to upstream
  connectManyToMany(upConnPOps, sortPOps);

  return make_pair(sortPOps, allPOps);
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformSplit(vector<shared_ptr<PhysicalOp>> upConnPOps, uint prePOpId) {
  //TODO: factor_与nextFactor_的设置暂时还没想好
  auto type = upConnPOps[0]->getType();
  vector<shared_ptr<PhysicalOp>> allSplitPOps;
  int nextFactor = 0;
  for (auto op : upConnPOps) {
    if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
      continue;
    }
    nextFactor+=op->getNextFactor();
    break;
  }
  if (type == POpType::FILE_SCAN) {
    for (size_t i = 0; i < upConnPOps.size(); i++) {
      vector<shared_ptr<PhysicalOp>> curSplitPOps;
      for (int j = -1; j < numNodes_; j++) {
        auto splitPOp = make_shared<split::SplitPOp>(
                          fmt::format("Split[{}]-{}-pre[{}]", j, upConnPOps[i]->name(), prePOpId),
                          upConnPOps[i]->getProjectColumnNames(),
                          j);
        curSplitPOps.emplace_back(splitPOp);
        splitPOp->setFactor(nextFactor);
        splitPOp->setNextFactor(nextFactor);
      }
      connectOneToMany(upConnPOps[i], curSplitPOps);
      allSplitPOps.insert(allSplitPOps.end(), curSplitPOps.begin(), curSplitPOps.end());
    }
  } else if (type == POpType::FILTER || type == POpType::PROJECT) {
    for (size_t i = 0; i < upConnPOps.size(); i += (numNodes_ + 1)) {
      vector<shared_ptr<PhysicalOp>> curSplitPOps;
      for (int j = -1; j < numNodes_; j++) {
        auto splitPOp = make_shared<split::SplitPOp>(
                          fmt::format("Split[{}]-{}-pre[{}]", j, upConnPOps[i]->name(), prePOpId),
                          upConnPOps[i]->getProjectColumnNames(),
                          j);
        curSplitPOps.emplace_back(splitPOp);
        splitPOp->setFactor(nextFactor);
        splitPOp->setNextFactor(nextFactor);
      }
      vector<shared_ptr<PhysicalOp>> curFilterPOps;
      for (int j = i; j < i + numNodes_ + 1; j++) {
        curFilterPOps.emplace_back(upConnPOps[j]);
      }
      connectManyToMany(curFilterPOps, curSplitPOps);
      allSplitPOps.insert(allSplitPOps.end(), curSplitPOps.begin(), curSplitPOps.end());
    }
  } else if (type == POpType::HASH_JOIN_PROBE) {
    //位于不同节点上, 相同parallel后缀的Probe需要连接到相同的Split上
    for (int i = 0; i < parallelDegree_; i++) {
      vector<shared_ptr<PhysicalOp>> curSplitPOps, curProbePOps;
      for (int j = i * (numNodes_ + 1); j < (i + 1) * (numNodes_ + 1); j++) {
        curProbePOps.emplace_back(upConnPOps[j]);
      }
      for (int j = -1; j < numNodes_; j++) {
        auto splitPOp = make_shared<split::SplitPOp>(
                          fmt::format("Split[{}]-{}-{}-pre[{}]", j, curProbePOps[0]->name(), i, prePOpId),
                          curProbePOps[0]->getProjectColumnNames(),
                          j);
        curSplitPOps.emplace_back(splitPOp);
        splitPOp->setFactor(nextFactor);
        splitPOp->setNextFactor(nextFactor);
      }
      connectManyToMany(curProbePOps, curSplitPOps);
      allSplitPOps.insert(allSplitPOps.end(), curSplitPOps.begin(), curSplitPOps.end());
    }
  } else if (type == POpType::NESTED_LOOP_JOIN) {
    //位于不同节点上, 相同parallel后缀的Group需要连接到相同的Split上. 连接关系与Group类似
    for (int i = 0; i < parallelDegree_; i++) {
      vector<shared_ptr<PhysicalOp>> curSplitPOps, curJoinPOps;
      for (int j = i * (numNodes_ + 1); j < (i + 1) * (numNodes_ + 1); j++) {
        curJoinPOps.emplace_back(upConnPOps[j]);
      }
      for (int j = -1; j < numNodes_; j++) {
        auto splitPOp = make_shared<split::SplitPOp>(
                          fmt::format("Split[{}]-{}-{}-pre[{}]", j, curJoinPOps[0]->name(), i, prePOpId),
                          curJoinPOps[0]->getProjectColumnNames(),
                          j);
        curSplitPOps.emplace_back(splitPOp);
        splitPOp->setFactor(nextFactor);
        splitPOp->setNextFactor(nextFactor);
      }
      connectManyToMany(curJoinPOps, curSplitPOps);
      allSplitPOps.insert(allSplitPOps.end(), curSplitPOps.begin(), curSplitPOps.end());
    }
  } else if (type == POpType::GROUP) {
    //位于不同节点上, 相同parallel后缀的Group需要连接到相同的Split上
    for (int i = 0; i < parallelDegree_; i++) {
      vector<shared_ptr<PhysicalOp>> curSplitPOps, curGroupPOps;
      for (int j = i * (numNodes_ + 1); j < (i + 1) * (numNodes_ + 1); j++) {
        curGroupPOps.emplace_back(upConnPOps[j]);
      }
      for (int j = -1; j < numNodes_; j++) {
        auto splitPOp = make_shared<split::SplitPOp>(
                          fmt::format("Split[{}]-{}-{}-pre[{}]", j, curGroupPOps[0]->name(), i, prePOpId),
                          curGroupPOps[0]->getProjectColumnNames(),
                          j);
        curSplitPOps.emplace_back(splitPOp);
        splitPOp->setFactor(nextFactor);
        splitPOp->setNextFactor(nextFactor);
      }
      connectManyToMany(curGroupPOps, curSplitPOps);
      allSplitPOps.insert(allSplitPOps.end(), curSplitPOps.begin(), curSplitPOps.end());
    }
  } else if (type == POpType::AGGREGATE) {
    vector<shared_ptr<PhysicalOp>> curSplitPOps;
    for (int j = -1; j < numNodes_; j++) {
      auto splitPOp = make_shared<split::SplitPOp>(
                        fmt::format("Split[{}]-{}-pre[{}]", j, upConnPOps[0]->name(), prePOpId),
                        upConnPOps[0]->getProjectColumnNames(),
                        j);
      curSplitPOps.emplace_back(splitPOp);
      splitPOp->setFactor(nextFactor);
      splitPOp->setNextFactor(nextFactor);
    }
    connectManyToMany(upConnPOps, curSplitPOps);
    allSplitPOps.insert(allSplitPOps.end(), curSplitPOps.begin(), curSplitPOps.end());
  } else {
    std::cout << "Got type: " << type << std::endl; 
    //Error!
  }
  return allSplitPOps;
}

vector<shared_ptr<PhysicalOp>>
PrePToPTransformer::transformShuffle(vector<shared_ptr<PhysicalOp>> upConnPOps, const vector<string> &columnNames, uint prePOpId) {
  auto type = upConnPOps[0]->getType();
  int nextFactor = 0;
  for (auto op : upConnPOps) {
    if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
      continue;
    }
    nextFactor+=op->getNextFactor();
    break;
  }
  vector<shared_ptr<PhysicalOp>> allShufflePOps;
  if (type == POpType::FILE_SCAN) {
    for (size_t i = 0; i < upConnPOps.size(); i++) {
      vector<shared_ptr<PhysicalOp>> curShufflePOps;
      for (int j = -1; j < numNodes_; j++) {
        auto shufflePOp = make_shared<shuffle::ShufflePOp>(
                          fmt::format("Shuffle[{}]-{}-pre[{}]", j, upConnPOps[i]->name(), prePOpId),
                          upConnPOps[i]->getProjectColumnNames(),
                          j,
                          columnNames);
        curShufflePOps.emplace_back(shufflePOp);
        shufflePOp->setFactor(nextFactor); //只会收到FileScan发送的1次
        shufflePOp->setNextFactor(nextFactor);
      }
      connectOneToMany(upConnPOps[i], curShufflePOps);
      allShufflePOps.insert(allShufflePOps.end(), curShufflePOps.begin(), curShufflePOps.end());
    }
  } else if (type == POpType::FILTER || type == POpType::PROJECT) {
    for (size_t i = 0; i < upConnPOps.size(); i += (numNodes_ + 1)) {
      vector<shared_ptr<PhysicalOp>> curShufflePOps;
      for (int j = -1; j < numNodes_; j++) {
        auto shufflePOp = make_shared<shuffle::ShufflePOp>(
                          fmt::format("Shuffle[{}]-{}-pre[{}]", j, upConnPOps[i]->name(), prePOpId),
                          upConnPOps[i]->getProjectColumnNames(),
                          j,
                          columnNames);
        curShufflePOps.emplace_back(shufflePOp);
        shufflePOp->setFactor(nextFactor); 
        shufflePOp->setNextFactor(nextFactor);
      }
      vector<shared_ptr<PhysicalOp>> curFilterPOps;
      for (int j = i; j < i + numNodes_ + 1; j++) {
        curFilterPOps.emplace_back(upConnPOps[j]);
      }
      connectManyToMany(curFilterPOps, curShufflePOps);
      allShufflePOps.insert(allShufflePOps.end(), curShufflePOps.begin(), curShufflePOps.end());
    }
  } else if (type == POpType::HASH_JOIN_PROBE) {
    //位于不同节点上, 相同parallel后缀的Probe需要连接到相同的Shuffle上
    for (int i = 0; i < parallelDegree_; i++) {
      vector<shared_ptr<PhysicalOp>> curShufflePOps, curProbePOps;
      for (int j = i * (numNodes_ + 1); j < (i + 1) * (numNodes_ + 1); j++) {
        curProbePOps.emplace_back(upConnPOps[j]);
      }
      for (int j = -1; j < numNodes_; j++) {
        auto shufflePOp = make_shared<shuffle::ShufflePOp>(
                          fmt::format("Shuffle[{}]-{}-{}-pre[{}]", j, curProbePOps[0]->name(), i, prePOpId),
                          curProbePOps[0]->getProjectColumnNames(),
                          j,
                          columnNames);
        curShufflePOps.emplace_back(shufflePOp);
        shufflePOp->setFactor(nextFactor); 
        shufflePOp->setNextFactor(nextFactor);
      }
      connectManyToMany(curProbePOps, curShufflePOps);
      allShufflePOps.insert(allShufflePOps.end(), curShufflePOps.begin(), curShufflePOps.end());
    }
  } else if (type == POpType::NESTED_LOOP_JOIN) {
    //位于不同节点上, 相同parallel后缀的Group需要连接到相同的Shuffle上. 连接关系与Group类似
    for (int i = 0; i < parallelDegree_; i++) {
      vector<shared_ptr<PhysicalOp>> curShufflePOps, curJoinPOps;
      for (int j = i * (numNodes_ + 1); j < (i + 1) * (numNodes_ + 1); j++) {
        curJoinPOps.emplace_back(upConnPOps[j]);
      }
      for (int j = -1; j < numNodes_; j++) {
        auto shufflePOp = make_shared<shuffle::ShufflePOp>(
                          fmt::format("Shuffle[{}]-{}-{}-pre[{}]", j, curJoinPOps[0]->name(), i, prePOpId),
                          curJoinPOps[0]->getProjectColumnNames(),
                          j,
                          columnNames);
        curShufflePOps.emplace_back(shufflePOp);
        //TODO: factor_与nextFactor_的设置暂时还没想好
        shufflePOp->setFactor(nextFactor); 
        shufflePOp->setNextFactor(nextFactor);
      }
      connectManyToMany(curJoinPOps, curShufflePOps);
      allShufflePOps.insert(allShufflePOps.end(), curShufflePOps.begin(), curShufflePOps.end());
    }
  } else if (type == POpType::GROUP) {
    //位于不同节点上, 相同parallel后缀的Group需要连接到相同的Shuffle上
    for (int i = 0; i < parallelDegree_; i++) {
      vector<shared_ptr<PhysicalOp>> curShufflePOps, curGroupPOps;
      for (int j = i * (numNodes_ + 1); j < (i + 1) * (numNodes_ + 1); j++) {
        curGroupPOps.emplace_back(upConnPOps[j]);
      }
      for (int j = -1; j < numNodes_; j++) {
        auto shufflePOp = make_shared<shuffle::ShufflePOp>(
                          fmt::format("Shuffle[{}]-{}-{}-pre[{}]", j, curGroupPOps[0]->name(), i, prePOpId),
                          curGroupPOps[0]->getProjectColumnNames(),
                          j,
                          columnNames);
        curShufflePOps.emplace_back(shufflePOp);
        //TODO: factor_与nextFactor_的设置暂时还没想好
        shufflePOp->setFactor(nextFactor); 
        shufflePOp->setNextFactor(nextFactor);
      }
      connectManyToMany(curGroupPOps, curShufflePOps);
      allShufflePOps.insert(allShufflePOps.end(), curShufflePOps.begin(), curShufflePOps.end());
    }
  } else if (type == POpType::AGGREGATE) {
    vector<shared_ptr<PhysicalOp>> curShufflePOps;
    for (int j = -1; j < numNodes_; j++) {
      auto shufflePOp = make_shared<shuffle::ShufflePOp>(
                        fmt::format("Shuffle[{}]-{}-pre[{}]", j, upConnPOps[0]->name(), prePOpId),
                        upConnPOps[0]->getProjectColumnNames(),
                        j,
                        columnNames);
      curShufflePOps.emplace_back(shufflePOp);
      //TODO: factor_与nextFactor_的设置暂时还没想好
      shufflePOp->setFactor(nextFactor); 
      shufflePOp->setNextFactor(nextFactor);
    }
    connectManyToMany(upConnPOps, curShufflePOps);
    allShufflePOps.insert(allShufflePOps.end(), curShufflePOps.begin(), curShufflePOps.end());
  } else {
    std::cout << "Got type: " << type << std::endl; 
    //Error!
  }
  return allShufflePOps;
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformAggregate(const shared_ptr<AggregatePrePOp> &aggregatePrePOp) {
  // id
  auto prePOpId = aggregatePrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(aggregatePrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for aggregate, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }

  std::vector<std::pair<bool, std::string>> aggRemoteSpawnInfos;
  for (const auto &producer: aggregatePrePOp->getProducers()) {
    if(producer->getType() == PrePOpType::FILTERABLE_SCAN) {
      aggRemoteSpawnInfos.emplace_back(true, "_onRemote");
    } else {
      aggRemoteSpawnInfos.emplace_back(false, "");
    }
  }

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;
  bool isParallel = true;

  vector<shared_ptr<PhysicalOp>> selfPOps, selfConnUpPOps, selfConnDownPOps;
  vector<string> projectColumnNames{aggregatePrePOp->getProjectColumnNames().begin(),
                                    aggregatePrePOp->getProjectColumnNames().end()};
  vector<string> parallelAggProjectColumnNames;

  for (uint i = 0; i < aggregatePrePOp->getFunctions().size(); ++i) {
    const auto prePFunction = aggregatePrePOp->getFunctions()[i];
    const auto aggOutputColumnName = aggregatePrePOp->getAggOutputColumnNames()[i];
    parallelAggProjectColumnNames.emplace_back(aggOutputColumnName);
  }

#if (PREDICATE_EN>0) || (RUNTIME_SAMPLE_EN>0)
  auto pred = make_shared<predicate::PredicatePOp>(
          fmt::format("Predicate[{}]-{}-pre[{}]", -1, "AGGREGATE", prePOpId),
          projectColumnNames,
          -1,
          POpType::AGGREGATE);
  allPOps.emplace_back(pred);
  for (auto op : upConnPOps) {
    op->predict(pred);
    pred->consume(op);
  }
#endif

  vector<shared_ptr<PhysicalOp>> curAggPOps;
  int nextFactor = 0;
  for (auto op : upConnPOps) {
    if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
      continue;
    }
    nextFactor+=op->getNextFactor();
  }
  for (int j = -1; j < numNodes_; j++) {
    // aggregate functions, better to let each operator has its own copy of aggregate functions
    vector<shared_ptr<aggregate::AggregateFunction>> aggFunctions;
    for (size_t k = 0; k < aggregatePrePOp->getFunctions().size(); ++k) {
      const auto &prePFunction = aggregatePrePOp->getFunctions()[k];
      const auto &aggOutputColumnName = aggregatePrePOp->getAggOutputColumnNames()[k];
      const auto &transAggFunctions = transformAggFunction(aggOutputColumnName,
                                                          prePFunction,
                                                          false);
      aggFunctions.insert(aggFunctions.end(), transAggFunctions.begin(), transAggFunctions.end());
    }
    auto aggPOp = make_shared<aggregate::AggregatePOp>(
                    fmt::format("Aggregate[{}]-pre[{}]", j, prePOpId),
                    parallelAggProjectColumnNames,
                    j,
                    aggFunctions);
    curAggPOps.emplace_back(aggPOp);
    curAggPOps.back()->setSpawnOnRemote(aggRemoteSpawnInfos[0].first);
    //Set factor_
    if (upConnPOps[0]->getType() == POpType::HASH_JOIN_PROBE) {
      aggPOp->setFactor(nextFactor); //此时会汇总Probe所有TupleSet
    } else {
      aggPOp->setFactor(nextFactor);
    }
    aggPOp->setNextFactor(1); //只会发送1次
#if PRINT_PHY_PLAN > 0
    auto prettyName = "Aggregate";
    op2name[aggPOp->name()] = prettyName;
    producer2consumer[op2name[upConnPOps[0]->name()]] = prettyName;
#endif
#if RUNTIME_SAMPLE_EN>0
    aggPOp->sample(pred);
#endif
  }
  connectManyToMany(upConnPOps, curAggPOps);
  selfPOps.insert(selfPOps.end(), curAggPOps.begin(), curAggPOps.end());

  selfConnUpPOps = selfPOps;
  selfConnDownPOps = selfPOps;

  // collect all physical ops
  allPOps.insert(allPOps.end(), selfPOps.begin(), selfPOps.end());

  return make_pair(selfConnDownPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformGroup(const shared_ptr<GroupPrePOp> &groupPrePOp) {
  // id
  auto prePOpId = groupPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(groupPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for group, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }

  std::vector<std::pair<bool, std::string>> shuffleRemoteSpawnInfos;
  for (const auto &producer: groupPrePOp->getProducers()) {
    if(producer->getType() == PrePOpType::FILTERABLE_SCAN) {
      shuffleRemoteSpawnInfos.emplace_back(true, "_onRemote");
    } else {
      shuffleRemoteSpawnInfos.emplace_back(false, "");
    }
  }

  // transform self
  auto producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<shared_ptr<PhysicalOp>> groupPOps;
  vector<string> projectColumnNames{groupPrePOp->getProjectColumnNames().begin(),
                                    groupPrePOp->getProjectColumnNames().end()};
std::cout<<"PrePToPTransformer::transformGroup "<<upConnPOps.size()<<std::endl;
#if (PREDICATE_EN>0) || (RUNTIME_SAMPLE_EN>0)
  auto pred = make_shared<predicate::PredicatePOp>(
          fmt::format("Predicate[{}]-{}-pre[{}]", -1, "GROUP", prePOpId),
          projectColumnNames,
          -1,
          POpType::GROUP);
  allPOps.emplace_back(pred);
  for (auto op : producerTransRes.first) {
    op->predict(pred);
    pred->consume(op);
  }
#endif

  int nextFactor = 0;
  for (auto op : upConnPOps) {
    if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
      continue;
    }
    nextFactor+=op->getNextFactor();
  }
  for (size_t k = 0; k < parallelDegree_; k++) {
    for (int i = -1; i < numNodes_; ++i) {
      // aggregate functions, better to let each operator has its own copy of aggregate functions
      vector<shared_ptr<aggregate::AggregateFunction>> aggFunctions;
      for (size_t j = 0; j < groupPrePOp->getFunctions().size(); ++j) {
        const auto &prePFunction = groupPrePOp->getFunctions()[j];
        const auto &aggOutputColumnName = groupPrePOp->getAggOutputColumnNames()[j];
        const auto &transAggFunctions = transformAggFunction(aggOutputColumnName,
                                                            prePFunction,
                                                            false);
        aggFunctions.insert(aggFunctions.end(), transAggFunctions.begin(), transAggFunctions.end());
      }
      auto groupPOp = make_shared<group::GroupPOp>(fmt::format("Group[{}]-{}-pre[{}]", i, k, prePOpId),
                                                          projectColumnNames,
                                                          i,
                                                          groupPrePOp->getGroupColumnNames(),
                                                          aggFunctions);
      groupPOps.emplace_back(groupPOp);
  #if PRINT_PHY_PLAN > 0
      auto prettyName = "Group";
      op2name[groupPOp->name()] = prettyName;
      producer2consumer[op2name[producerTransRes.first[0]->name()]] = prettyName;
#endif
#if RUNTIME_SAMPLE_EN>0
      groupPOp->sample(pred);
#endif
    }
  }
  allPOps.insert(allPOps.end(), groupPOps.begin(), groupPOps.end());
  if (parallelDegree_ == 1) {
    connectManyToMany(producerTransRes.first, groupPOps);
    for (auto groupPOp : groupPOps) { 
      //Set factor_
      groupPOp->setFactor(nextFactor);
      groupPOp->setNextFactor(1);
    }
  } else {
    auto shufflePOps = transformShuffle(producerTransRes.first, groupPrePOp->getGroupColumnNames(), prePOpId);
    allPOps.insert(allPOps.end(), shufflePOps.begin(), shufflePOps.end());
    connectManyToMany(shufflePOps, groupPOps);
    nextFactor = 0;
    for (auto op : shufflePOps) {
      if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
        continue;
      }
      nextFactor+=op->getNextFactor();
    }
    for (auto groupPOp : groupPOps) {
      //Set factor_
      groupPOp->setFactor(nextFactor);
      groupPOp->setNextFactor(1);
    }
  }

  return make_pair(groupPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformProject(const shared_ptr<ProjectPrePOp> &projectPrePOp) {
  // id
  auto prePOpId = projectPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(projectPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for project, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<shared_ptr<PhysicalOp>> selfPOps;
  vector<string> projectColumnNames{projectPrePOp->getProjectColumnNames().begin(),
                                    projectPrePOp->getProjectColumnNames().end()};

  int nextFactor = 0;
  std::cout<<"project begin"<<std::endl;
  for (auto op : upConnPOps) {
    if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
      continue;
    }
    nextFactor+=op->getNextFactor();
    std::cout<<op->name()<<" nextfactor="<<op->getNextFactor()<<std::endl;
    break;
  }
  std::cout<<"project end"<<std::endl;
  auto type = upConnPOps[0]->getType();
  if (type == POpType::FILE_SCAN){//special as only [0] FileScan exists
    for (size_t i = 0; i < upConnPOps.size(); ++i) {
      vector<shared_ptr<PhysicalOp>> curProjectPOps;
      for (int j = -1; j < numNodes_; j++) {
        auto projectPOp = make_shared<project::ProjectPOp>(fmt::format("Project[{}]-{}-pre[{}]", j, upConnPOps[i]->name(), prePOpId),
                                                            projectColumnNames,
                                                            j,
                                                            projectPrePOp->getExprs(),
                                                            projectPrePOp->getExprNames(),
                                                            projectPrePOp->getProjectColumnNamePairs());
        curProjectPOps.emplace_back(projectPOp);
        //Set factor_
        projectPOp->setFactor(nextFactor); 
        projectPOp->setNextFactor(nextFactor);
      }
      connectOneToMany(upConnPOps[i], curProjectPOps);
      selfPOps.insert(selfPOps.end(), curProjectPOps.begin(), curProjectPOps.end());
    }
  }
  else{
    for (size_t i = 0; i < upConnPOps.size(); i=i+numNodes_+1){
      vector<shared_ptr<PhysicalOp>> curUpConnPOps, curProjectPOps;
      for (int j = i ; j < i+numNodes_+1; j++) {
          curUpConnPOps.emplace_back(upConnPOps[j]);
          std::cout<<"push upconn:"<<upConnPOps[j]->name()<<std::endl;
      }
      std::cout<<"---------"<<std::endl;
      for (int j = -1; j < numNodes_; j++) {
        auto projectPOp = make_shared<project::ProjectPOp>(fmt::format("Project[{}]-{}-pre[{}]", j, curUpConnPOps[0]->name(), prePOpId),
                                                            projectColumnNames,
                                                            j,
                                                            projectPrePOp->getExprs(),
                                                            projectPrePOp->getExprNames(),
                                                            projectPrePOp->getProjectColumnNamePairs());
        curProjectPOps.emplace_back(projectPOp);
        //Set factor_
        projectPOp->setFactor(nextFactor); 
        projectPOp->setNextFactor(nextFactor);
      }
      connectManyToMany(curUpConnPOps, curProjectPOps);
      selfPOps.insert(selfPOps.end(), curProjectPOps.begin(), curProjectPOps.end());
    }
  }
  
  
  

  // collect all physical ops
  allPOps.insert(allPOps.end(), selfPOps.begin(), selfPOps.end());

  return make_pair(selfPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformFilter(const shared_ptr<FilterPrePOp> &filterPrePOp) {//这个是有用的，位于physical plan中间的Filter的生成
  // id
  auto prePOpId = filterPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(filterPrePOp);
  if (producersTransRes.size() != 1) {
    throw runtime_error(fmt::format("Unsupported number of producers for filter, should be {}, but get {}",
                                    1, producersTransRes.size()));
  }

  // transform self
  const auto &producerTransRes = producersTransRes[0];
  auto upConnPOps = producerTransRes.first;
  auto allPOps = producerTransRes.second;

  vector<shared_ptr<PhysicalOp>> selfPOps;
  vector<string> projectColumnNames{filterPrePOp->getProjectColumnNames().begin(),
                                    filterPrePOp->getProjectColumnNames().end()};
  std::cout<<"PrePToPTransformer::transformFilter "<<upConnPOps.size()<<std::endl;
// #if (PREDICATE_EN>0) || (RUNTIME_SAMPLE_EN>0)
//   auto pred = make_shared<predicate::PredicatePOp>(
//           fmt::format("Predicate[{}]-{}-pre[{}]", -1, "Filter", prePOpId),
//           projectColumnNames,
//           -1,
//           POpType::FILTER);
//   allPOps.emplace_back(pred);
//   for (auto op : upConnPOps) {
//     op->predict(pred);
//     pred->consume(op);
//   }
// #endif
  int nextFactor = 0;
  for (auto op : upConnPOps) {
    if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
      continue;
    }
    nextFactor+=op->getNextFactor();
    break;
  }
  for (size_t i = 0; i < upConnPOps.size(); i += (numNodes_ + 1)) {
    vector<shared_ptr<PhysicalOp>> curFilterPOps,curUpConnPOps;
    for (int j = i ; j < i+numNodes_+1; j++) {
        curUpConnPOps.emplace_back(upConnPOps[j]);
    }
    for (int j = -1; j < numNodes_; j++) {
      auto filterPOp = make_shared<filter::FilterPOp>(fmt::format("Filter[{}]-{}-pre[{}]", j, i, prePOpId),
                                                    projectColumnNames,
                                                    j,
                                                    filterPrePOp->getPredicate(),
                                                    nullptr);
      curFilterPOps.emplace_back(filterPOp);
      //Set factor_
      filterPOp->setFactor(nextFactor);
      filterPOp->setNextFactor(nextFactor);
    }
    connectManyToMany(curUpConnPOps, curFilterPOps);
    selfPOps.insert(selfPOps.end(), curFilterPOps.begin(), curFilterPOps.end());
  }

  // collect all physical ops
  allPOps.insert(allPOps.end(), selfPOps.begin(), selfPOps.end());

  return make_pair(selfPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformHashJoin(const shared_ptr<HashJoinPrePOp> &hashJoinPrePOp) {
  // id
  auto prePOpId = hashJoinPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(hashJoinPrePOp);
  if (producersTransRes.size() != 2) {
    throw runtime_error(fmt::format("Unsupported number of producers for hashJoin, should be {}, but get {}",
                                    2, producersTransRes.size()));
  }

  std::vector<std::pair<bool, std::string>> shuffleRemoteSpawnInfos;
  for (const auto &producer: hashJoinPrePOp->getProducers()) {
    if(producer->getType() == PrePOpType::FILTERABLE_SCAN) {
      shuffleRemoteSpawnInfos.emplace_back(true, "_onRemote");
    } else {
      shuffleRemoteSpawnInfos.emplace_back(false, "");
    }
  }

  auto leftTransRes = producersTransRes[0];
  auto rightTransRes = producersTransRes[1];
  auto allPOps = leftTransRes.second;
  allPOps.insert(allPOps.end(), rightTransRes.second.begin(), rightTransRes.second.end());

  // transform self
  vector<string> projectColumnNames{hashJoinPrePOp->getProjectColumnNames().begin(),
                                    hashJoinPrePOp->getProjectColumnNames().end()};
  auto joinType = hashJoinPrePOp->getJoinType();
  const auto &leftColumnNames = hashJoinPrePOp->getLeftColumnNames();
  const auto &rightColumnNames = hashJoinPrePOp->getRightColumnNames();
  join::HashJoinPredicate hashJoinPredicate(leftColumnNames, rightColumnNames);
  const auto &hashJoinPredicateStr = hashJoinPredicate.toString();

#if (PREDICATE_EN>0) || (RUNTIME_SAMPLE_EN>0)
  auto buildPred = make_shared<predicate::PredicatePOp>(
          fmt::format("Predicate[{}]-{}-{}-pre[{}]", -1, "BUILD", hashJoinPredicateStr, prePOpId),
          projectColumnNames,
          -1,
          POpType::HASH_JOIN_BUILD);
  allPOps.emplace_back(buildPred);
  for (auto op : leftTransRes.first) {
    op->predict(buildPred);
    buildPred->consume(op);
  }
#endif
 
  vector<shared_ptr<PhysicalOp>> hashJoinBuildPOps, hashJoinProbePOps;
  // int leftNextFactor = leftTransRes.first[0]->getNextFactor();
  // int rightNextFactor = rightTransRes.first[0]->getNextFactor();
  std::cout<<"build begin"<<std::endl;
  int leftNextFactor = 0;
  for (auto op : leftTransRes.first) {
    if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
      continue;
    }
    leftNextFactor+=op->getNextFactor();
    std::cout<<op->name()<<std::endl;
  }
  std::cout<<"build end,probe begin"<<std::endl;
  int rightNextFactor = 0;
  for (auto op : rightTransRes.first) {
    if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
      continue;
    }
    rightNextFactor+=op->getNextFactor();
    std::cout<<op->name()<<std::endl;
  }
  std::cout<<"probe end"<<std::endl;
  /*
  * 假定parallelDegree_ = 4, numNodes_ = 1, 则节点编号分别为:
  * -1,0    0,0    -1,1    0,1    -1,2    0,2    -1,3    0,3
  */
  for (size_t j = 0; j < parallelDegree_; j++) {
    vector<shared_ptr<PhysicalOp>> curBuildPOps, curProbePOps;
    for (int i = -1; i < numNodes_; ++i) {
      auto buildPOp = make_shared<join::HashJoinBuildPOp>(
              fmt::format("HashJoinBuild[{}]-{}-{}-pre[{}]", i, hashJoinPredicateStr, j, prePOpId),
              projectColumnNames,
              i,
              leftColumnNames);
      curBuildPOps.emplace_back(buildPOp);
      auto probePOp = make_shared<join::HashJoinProbePOp>(
              fmt::format("HashJoinProbe[{}]-{}-{}-pre[{}]", i, hashJoinPredicateStr, j, prePOpId),
              projectColumnNames,
              i,
              hashJoinPredicate,
              joinType);
      curProbePOps.emplace_back(probePOp);
#if PRINT_PHY_PLAN > 0
      auto prettyName = fmt::format("HashJoinBuild-{}-pre[{}]", hashJoinPredicateStr, prePOpId);
      op2name[buildPOp->name()] = prettyName;
      producer2consumer[op2name[leftTransRes.first[0]->name()]] = prettyName;

      prettyName = fmt::format("HashJoinProbe-{}-pre[{}]", hashJoinPredicateStr, prePOpId);
      op2name[probePOp->name()] = prettyName;
      producer2consumer[op2name[rightTransRes.first[0]->name()]] = prettyName;
      producer2consumer[op2name[buildPOp->name()]] = prettyName;
#endif
#if RUNTIME_SAMPLE_EN>0
      buildPOp->sample(buildPred);
#endif
    }
    connectManyToMany(curBuildPOps, curProbePOps);
    hashJoinBuildPOps.insert(hashJoinBuildPOps.end(), curBuildPOps.begin(), curBuildPOps.end());
    hashJoinProbePOps.insert(hashJoinProbePOps.end(), curProbePOps.begin(), curProbePOps.end());
  }
  allPOps.insert(allPOps.end(), hashJoinBuildPOps.begin(), hashJoinBuildPOps.end());
  allPOps.insert(allPOps.end(), hashJoinProbePOps.begin(), hashJoinProbePOps.end());
  // connectManyToMany(hashJoinBuildPOps, hashJoinProbePOps);

#if (PREDICATE_EN>0) || (RUNTIME_SAMPLE_EN>0)
  auto probePred = make_shared<predicate::PredicatePOp>(
          fmt::format("Predicate[{}]-{}-{}-pre[{}]", -1, "PROBE", hashJoinPredicateStr, prePOpId),
          projectColumnNames,
          -1,
          POpType::HASH_JOIN_PROBE);
  allPOps.emplace_back(probePred);
  for (auto op : hashJoinBuildPOps) {
    op->predict(probePred);
    probePred->consume(op);
  }
  for (auto op : rightTransRes.first) {
    op->predict(probePred);
    probePred->consume(op);
  }
#endif
#if RUNTIME_SAMPLE_EN>0
  for (auto op:hashJoinProbePOps){
    op->sample(probePred);
  }
#endif
  if (parallelDegree_ == 1) {
    connectManyToMany(leftTransRes.first, hashJoinBuildPOps);
    connectManyToMany(rightTransRes.first, hashJoinProbePOps);
    //Set factor_
    for (auto buildPOp : hashJoinBuildPOps) {
      buildPOp->setFactor(leftNextFactor); //收到nextFactor次
      buildPOp->setNextFactor(1);
    }
    for (auto probePOp : hashJoinProbePOps) {
      probePOp->setFactor(rightNextFactor); //收到nextFactor次
      probePOp->setNextFactor(rightNextFactor); //发送nextFactor次
    }
  } else {
    auto shuffleLeftPOps = transformShuffle(leftTransRes.first, leftColumnNames, prePOpId);
    auto shuffleRightPOps = transformShuffle(rightTransRes.first, rightColumnNames, prePOpId);
    connectManyToMany(shuffleLeftPOps, hashJoinBuildPOps);
    connectManyToMany(shuffleRightPOps, hashJoinProbePOps);
    allPOps.insert(allPOps.end(), shuffleLeftPOps.begin(), shuffleLeftPOps.end());
    allPOps.insert(allPOps.end(), shuffleRightPOps.begin(), shuffleRightPOps.end());
    //Set factor_
    // leftNextFactor = shuffleLeftPOps[0]->getNextFactor();
    // rightNextFactor = shuffleRightPOps[0]->getNextFactor();
    leftNextFactor = 0;
    for (auto op : shuffleLeftPOps) {
      if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
        continue;
      }
      leftNextFactor+=op->getNextFactor();
    }
    rightNextFactor = 0;
    for (auto op : shuffleRightPOps) {
      if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
        continue;
      }
      rightNextFactor+=op->getNextFactor();
    }
    for (auto buildPOp : hashJoinBuildPOps) {
      buildPOp->setFactor(leftNextFactor); //收到nextFactor次
      buildPOp->setNextFactor(1);
    }
    for (auto probePOp : hashJoinProbePOps) {
      probePOp->setFactor(rightNextFactor); //收到nextFactor次
      probePOp->setNextFactor(rightNextFactor); //发送nextFactor次
    }
  }
  
  return make_pair(hashJoinProbePOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformNestedLoopJoin(const shared_ptr<NestedLoopJoinPrePOp> &nestedLoopJoinPrePOp) {
  // id
  auto prePOpId = nestedLoopJoinPrePOp->getId();

  // transform producers
  const auto &producersTransRes = transformProducers(nestedLoopJoinPrePOp);
  if (producersTransRes.size() != 2) {
    throw runtime_error(fmt::format("Unsupported number of producers for nestedLoopJoin, should be {}, but get {}",
                                    2, producersTransRes.size()));
  }

  auto leftTransRes = producersTransRes[0];
  auto rightTransRes = producersTransRes[1];
  auto allPOps = leftTransRes.second;
  allPOps.insert(allPOps.end(), rightTransRes.second.begin(), rightTransRes.second.end());

  // transform self
  vector<string> projectColumnNames{nestedLoopJoinPrePOp->getProjectColumnNames().begin(),
                                    nestedLoopJoinPrePOp->getProjectColumnNames().end()};
  auto joinType = nestedLoopJoinPrePOp->getJoinType();
  optional<shared_ptr<fpdb::expression::gandiva::Expression>> predicate;
  if (nestedLoopJoinPrePOp->getPredicate()) {
    predicate = nestedLoopJoinPrePOp->getPredicate();
  }

  vector<shared_ptr<PhysicalOp>> nestedLoopJoinPOps;
  int nextFactor = 0;
  for (auto op : rightTransRes.first) {
    if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
      continue;
    }
    nextFactor+=op->getNextFactor();
  }
  int left_nextFactor = 0;
  for (auto op : leftTransRes.first) {
    if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
      continue;
    }
    left_nextFactor+=op->getNextFactor();
  }
  for (int j = 0; j < parallelDegree_; j++) {
    for (int i = -1; i < numNodes_; i++) {
      shared_ptr<join::NestedLoopJoinPOp> nestedLoopJoinPOp =
            make_shared<join::NestedLoopJoinPOp>(fmt::format("NestedLoopJoin[{}]-{}-pre[{}]", i, j, prePOpId),
                                                 projectColumnNames,
                                                 i,
                                                 predicate,
                                                 joinType);
      for (const auto &upLeftConnPOp: leftTransRes.first) {
        upLeftConnPOp->produce(nestedLoopJoinPOp);
        nestedLoopJoinPOp->addLeftProducer(upLeftConnPOp);
      }
      nestedLoopJoinPOps.emplace_back(nestedLoopJoinPOp);
    }
  }
  allPOps.insert(allPOps.end(), nestedLoopJoinPOps.begin(), nestedLoopJoinPOps.end());

  // connect to right inputs, if num > 1, then we need split operators for right producers
  if (parallelDegree_ == 1) {
    for (const auto &upRightConnPOp: rightTransRes.first) {
      for (auto &loopJoinPOp : nestedLoopJoinPOps) {
        upRightConnPOp->produce(loopJoinPOp);
        static_pointer_cast<join::NestedLoopJoinPOp>(loopJoinPOp)->addRightProducer(upRightConnPOp);
        loopJoinPOp->setFactor(nextFactor);
        loopJoinPOp->setLoopJoinFactor(left_nextFactor);
        loopJoinPOp->setNextFactor(nextFactor);
      }
    }
  } else {
    auto splitPOps = transformSplit(rightTransRes.first, prePOpId);
    nextFactor = 0;
    for (auto op : splitPOps) {
      if(op->name().substr(op->name().find('[') + 1, op->name().find(']') - op->name().find('[') - 1)=="-1"||op->name().find("Predicate")!=std::string::npos){
        continue;
      }
      nextFactor+=op->getNextFactor();
    }
    for (auto &splitPOp : splitPOps) {
      for (auto &loopJoinPOp : nestedLoopJoinPOps) {
        splitPOp->produce(loopJoinPOp);
        static_pointer_cast<join::NestedLoopJoinPOp>(loopJoinPOp)->addRightProducer(splitPOp);
        loopJoinPOp->setFactor(nextFactor);
        loopJoinPOp->setLoopJoinFactor(left_nextFactor);
        loopJoinPOp->setNextFactor(nextFactor);
      }
    }
    allPOps.insert(allPOps.end(), splitPOps.begin(), splitPOps.end());
  }

  return make_pair(nestedLoopJoinPOps, allPOps);
}

pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
PrePToPTransformer::transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp) {
  const auto localFsPTransformer = make_shared<PrePToLocalFSPTransformer>(filterableScanPrePOp->getId(),
                                                                numNodes_);
  auto res = localFsPTransformer->transformFilterableScan(filterableScanPrePOp);
#if PRINT_PHY_PLAN > 0
  for (const auto &it : localFsPTransformer->getOp2Name()) {
    op2name.emplace(it.first, it.second);
  }
  for (const auto &it : localFsPTransformer->getProducer2Consumer()) {
    producer2consumer.emplace(it.first, it.second);
  }
#endif
  return res;
}

vector<shared_ptr<aggregate::AggregateFunction>>
PrePToPTransformer::transformAggFunction(const string &outputColumnName,
                                         const shared_ptr<AggregatePrePFunction> &prePFunction,
                                         bool hasReduceOp) {
  switch (prePFunction->getType()) {
    case plan::prephysical::SUM: {
      return {make_shared<aggregate::Sum>(outputColumnName, prePFunction->getExpression())};
    }
    case plan::prephysical::COUNT: {
      return {make_shared<aggregate::Count>(outputColumnName, prePFunction->getExpression())};
    }
    case plan::prephysical::MIN: {
      return {make_shared<aggregate::MinMax>(true, outputColumnName, prePFunction->getExpression())};
    }
    case plan::prephysical::MAX: {
      return {make_shared<aggregate::MinMax>(false, outputColumnName, prePFunction->getExpression())};
    }
    case plan::prephysical::AVG: {
      if (hasReduceOp) {
        auto sumFunc = make_shared<aggregate::Sum>(
                AggregatePrePFunction::AVG_PARALLEL_SUM_COLUMN_PREFIX + outputColumnName,
                prePFunction->getExpression());
        auto countFunc = make_shared<aggregate::Count>(
                AggregatePrePFunction::AVG_PARALLEL_COUNT_COLUMN_PREFIX + outputColumnName,
                prePFunction->getExpression());
        return {sumFunc, countFunc};
      } else {
        return {make_shared<aggregate::Avg>(outputColumnName, prePFunction->getExpression())};
      }
    }
    default: {
      throw runtime_error(fmt::format("Unsupported aggregate function type: {}", prePFunction->getTypeString()));
    }
  }
}

shared_ptr<aggregate::AggregateFunction>
PrePToPTransformer::transformAggReduceFunction(const string &outputColumnName,
                                               const shared_ptr<AggregatePrePFunction> &prePFunction) {
  switch (prePFunction->getType()) {
    case plan::prephysical::SUM:
    case plan::prephysical::COUNT: {
      return make_shared<aggregate::Sum>(outputColumnName,
                                         fpdb::expression::gandiva::col(outputColumnName));
    }
    case plan::prephysical::MIN: {
      return make_shared<aggregate::MinMax>(true,
                                            outputColumnName,
                                            fpdb::expression::gandiva::col(outputColumnName));
    }
    case plan::prephysical::MAX: {
      return make_shared<aggregate::MinMax>(false,
                                            outputColumnName,
                                            fpdb::expression::gandiva::col(outputColumnName));
    }
    case plan::prephysical::AVG: {
      return make_shared<aggregate::AvgReduce>(outputColumnName,nullptr);
    }
    default: {
      throw runtime_error(fmt::format("Unsupported aggregate function type for parallel execution: {}", prePFunction->getTypeString()));
    }
  }
}

void PrePToPTransformer::connectOneToOne(vector<shared_ptr<PhysicalOp>> &producers,
                                         vector<shared_ptr<PhysicalOp>> &consumers) {
  if (producers.size() != consumers.size()) {
    throw runtime_error(fmt::format("Bad one-to-one operator connection input, producers has {}, but consumers has {}",
                                    producers.size(), consumers.size()));
  }
  for (size_t i = 0; i < producers.size(); ++i) {
    producers[i]->produce(consumers[i]);
    consumers[i]->consume(producers[i]);
  }
}

void PrePToPTransformer::connectManyToMany(vector<shared_ptr<PhysicalOp>> &producers,
                                           vector<shared_ptr<PhysicalOp>> &consumers) {
  for (const auto &producer: producers) {
    for (const auto &consumer: consumers) {
      producer->produce(consumer);
      consumer->consume(producer);
    }
  }
}

void PrePToPTransformer::connectManyToOne(vector<shared_ptr<PhysicalOp>> &producers,
                                          shared_ptr<PhysicalOp> &consumer) {
  for (const auto &producer: producers) {
    producer->produce(consumer);
    consumer->consume(producer);
  }
}

void PrePToPTransformer::connectOneToMany(shared_ptr<PhysicalOp> &producer,
                                          vector<shared_ptr<PhysicalOp>> &consumers) {
  for (const auto &consumer: consumers) {
    producer->produce(consumer);
    consumer->consume(producer);
  }
}
}
