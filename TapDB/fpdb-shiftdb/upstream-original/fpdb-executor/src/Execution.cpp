//
// Created by Yifei Yang on 11/23/21.
//

#include <fpdb/executor/Execution.h>
#include <fpdb/executor/ModeCtrl.h>
#include <fpdb/executor/physical/POpContext.h>
#include <fpdb/executor/physical/POpDirectoryEntry.h>
#include <fpdb/executor/physical/POpConnection.h>
#include <fpdb/executor/physical/POpRelationshipType.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/serialization/POpSerializer.h>
#include <fpdb/executor/rdma_common.h>
#include <fpdb/util/Util.h>
#include <caf/io/all.hpp>
#include <graphviz/gvc.h>
#include <unordered_map>
#include<algorithm>
#include <chrono>

using namespace fpdb::executor::physical;

namespace fpdb::executor {
#if FILESCANPRIORITY >0
  std::map<std::string,int> probevec_;
#endif

void msgSender(int nodeID) {
  struct rdma_event_channel *cm_event_channel = NULL;
  struct rdma_cm_id *cm_server_id = NULL, *cm_client_id = NULL;
  struct ibv_pd *pd = NULL;
  struct ibv_comp_channel *io_completion_channel = NULL;
  struct ibv_cq *cq = NULL;
  struct ibv_qp_init_attr qp_init_attr;
  struct ibv_qp *client_qp = NULL;
  /* RDMA memory resources */
  struct ibv_mr *client_metadata_mr = NULL, *server_buffer_mr = NULL, *server_metadata_mr = NULL;
  struct rdma_buffer_attr client_metadata_attr, server_metadata_attr;
  struct ibv_recv_wr client_recv_wr, *bad_client_recv_wr = NULL;
  struct ibv_send_wr server_send_wr, *bad_server_send_wr = NULL;
  struct ibv_sge client_recv_sge, server_send_sge;



  int ret, option;
  struct sockaddr_in server_sockaddr;
  bzero(&server_sockaddr, sizeof server_sockaddr);
  server_sockaddr.sin_family = AF_INET; /* standard IP NET address */
  std::unordered_map<std::string, std::string> LimitConfig = readConfig("port.conf");
  auto ip = LimitConfig["ip"];
  uint16_t rdmaport=std::stoi(LimitConfig["rdmaport"]);
  server_sockaddr.sin_addr.s_addr = inet_addr(ip.c_str()); /* passed address */
  server_sockaddr.sin_port = htons(rdmaport + nodeID);




  //start_rdma_server
  struct rdma_cm_event *cm_event = NULL;
  /*  Open a channel used to report asynchronous communication event */
  cm_event_channel = rdma_create_event_channel();
  if (!cm_event_channel) {
#ifdef rdma_error
    rdma_error("Creating cm event channel failed with errno : (%d)", -errno);
#endif
    return;
  }
  ret = rdma_create_id(cm_event_channel, &cm_server_id, NULL, RDMA_PS_TCP);
  if (ret) {
#ifdef rdma_error
    rdma_error("Creating server cm id failed with errno: %d ", -errno);
#endif
    return;
  }
  /* Explicit binding of rdma cm id to the socket credentials */
  ret = rdma_bind_addr(cm_server_id, (struct sockaddr*)(&server_sockaddr));
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to bind server address, errno: %d \n", -errno);
#endif
    return;
  }
  ret = rdma_listen(cm_server_id, 8); /* backlog = 8 clients, same as TCP, see man listen*/
  if (ret) {
#ifdef rdma_error
    rdma_error("rdma_listen failed to listen on server address, errno: %d ",
                -errno);
#endif
    return;
  }
  printf("Server is listening successfully at: %s, port: %d. Node: %d \n",
                  inet_ntoa(server_sockaddr.sin_addr),
                  ntohs(server_sockaddr.sin_port),
                  nodeID);
  ret = process_rdma_cm_event(cm_event_channel, 
                  RDMA_CM_EVENT_CONNECT_REQUEST,
                  &cm_event);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to get cm event, ret = %d \n" , ret);
#endif
    return;
  }
  cm_client_id = cm_event->id;
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to acknowledge the cm event errno: %d \n", -errno);
#endif
    return;
  }




  //setup_client_resources
  if(!cm_client_id){
#ifdef rdma_error
    rdma_error("Client id is still NULL \n");
#endif
    return;
  }
  pd = ibv_alloc_pd(cm_client_id->verbs);
  if (!pd) {
#ifdef rdma_error
    rdma_error("Failed to allocate a protection domain errno: %d\n",
                -errno);
#endif
    return;
  }
  io_completion_channel = ibv_create_comp_channel(cm_client_id->verbs);
  if (!io_completion_channel) {
#ifdef rdma_error
    rdma_error("Failed to create an I/O completion event channel, %d\n",
                    -errno);
#endif
    return;
  }
  cq = ibv_create_cq(cm_client_id->verbs /* which device*/, 
                  CQ_CAPACITY /* maximum capacity*/, 
                  NULL /* user context, not used here */,
                  io_completion_channel /* which IO completion channel */, 
                  0 /* signaling vector, not used here*/);
  if (!cq) {
#ifdef rdma_error
    rdma_error("Failed to create a completion queue (cq), errno: %d\n",
                    -errno);
#endif
    return;
  }
  /* Ask for the event for all activities in the completion queue*/
  ret = ibv_req_notify_cq(cq /* on which CQ */, 
                  0 /* 0 = all event type, no filter*/);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to request notifications on CQ errno: %d \n",
                    -errno);
#endif
    return;
  }
  bzero(&qp_init_attr, sizeof qp_init_attr);
  qp_init_attr.cap.max_recv_sge = MAX_SGE; /* Maximum SGE per receive posting */
  qp_init_attr.cap.max_recv_wr = MAX_WR; /* Maximum receive posting capacity */
  qp_init_attr.cap.max_send_sge = MAX_SGE; /* Maximum SGE per send posting */
  qp_init_attr.cap.max_send_wr = MAX_WR; /* Maximum send posting capacity */
  qp_init_attr.qp_type = IBV_QPT_RC; /* QP type, RC = Reliable connection */
  /* We use same completion queue, but one can use different queues */
  qp_init_attr.recv_cq = cq; /* Where should I notify for receive completion operations */
  qp_init_attr.send_cq = cq; /* Where should I notify for send completion operations */
  /*Lets create a QP */
  ret = rdma_create_qp(cm_client_id /* which connection id */,
                  pd /* which protection domain*/,
                  &qp_init_attr /* Initial attributes */);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to create QP due to errno: %d\n", -errno);
#endif
    return;
  }
  /* Save the reference for handy typing but is not required */
  client_qp = cm_client_id->qp;




  //accept_client_connection
  struct rdma_conn_param conn_param;
  cm_event = NULL;
  struct sockaddr_in remote_sockaddr; 
  if(!cm_client_id || !client_qp) {
#ifdef rdma_error
    rdma_error("Client resources are not properly setup\n");
#endif
    return;
  }
  /* we prepare the receive buffer in which we will receive the client metadata*/
  client_metadata_mr = rdma_buffer_register(pd /* which protection domain */, 
                  &client_metadata_attr /* what memory */,
                  sizeof(client_metadata_attr) /* what length */, 
                  (ibv_access_flags)(IBV_ACCESS_LOCAL_WRITE) /* access permissions */);
  if(!client_metadata_mr){
#ifdef rdma_error
    rdma_error("Failed to register client attr buffer\n");
#endif
    return;
  }
  /* We pre-post this receive buffer on the QP. SGE credentials is where we 
    * receive the metadata from the client */
  client_recv_sge.addr = (uint64_t) client_metadata_mr->addr; // same as &client_buffer_attr
  client_recv_sge.length = client_metadata_mr->length;
  client_recv_sge.lkey = client_metadata_mr->lkey;
  /* Now we link this SGE to the work request (WR) */
  bzero(&client_recv_wr, sizeof(client_recv_wr));
  client_recv_wr.sg_list = &client_recv_sge;
  client_recv_wr.num_sge = 1; // only one SGE
  ret = ibv_post_recv(client_qp /* which QP */,
                &client_recv_wr /* receive work request*/,
                &bad_client_recv_wr /* error WRs */);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
#endif
    return;
  }
  memset(&conn_param, 0, sizeof(conn_param));
  /* this tell how many outstanding requests can we handle */
  conn_param.initiator_depth = 3; /* For this exercise, we put a small number here */
  /* This tell how many outstanding requests we expect other side to handle */
  conn_param.responder_resources = 3; /* For this exercise, we put a small number */
  ret = rdma_accept(cm_client_id, &conn_param);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to accept the connection, errno: %d \n", -errno);
#endif
    return;
  }
  ret = process_rdma_cm_event(cm_event_channel, 
                  RDMA_CM_EVENT_ESTABLISHED,
                  &cm_event);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to get the cm event, errnp: %d \n", -errno);
#endif
    return;
  }
  /* We acknowledge the event */
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to acknowledge the cm event %d\n", -errno);
#endif
    return;
  }
  /* Just FYI: How to extract connection information */
  memcpy(&remote_sockaddr /* where to save */, 
                  rdma_get_peer_addr(cm_client_id) /* gives you remote sockaddr */, 
                  sizeof(struct sockaddr_in) /* max size */);
  printf("A new connection is accepted from %s. Node: %d\n", 
                  inet_ntoa(remote_sockaddr.sin_addr), nodeID);




  //send_server_metadata_to_client
  struct ibv_wc wc;
  ret = process_work_completion_events(io_completion_channel, &wc, 1);
  if (ret != 1) {
#ifdef rdma_error
    rdma_error("Failed to receive , ret = %d \n", ret);
#endif
    return;
  }
  /* if all good, then we should have client's buffer information, lets see */
  printf("Client side buffer information is received... Node: %d\n", nodeID);
  show_rdma_buffer_attr(&client_metadata_attr);
  printf("The client has requested buffer length of : %u bytes. Node: %d\n", 
                  client_metadata_attr.length, nodeID);
  /* We need to setup requested memory buffer. This is where the client will 
  * do RDMA READs and WRITEs. */
  server_buffer_mr = rdma_buffer_alloc(pd /* which protection domain */, 
                  client_metadata_attr.length /* what size to allocate */, 
                  (ibv_access_flags)(IBV_ACCESS_LOCAL_WRITE|
                  IBV_ACCESS_REMOTE_READ|
                  IBV_ACCESS_REMOTE_WRITE) /* access permissions */);
  if(!server_buffer_mr){
#ifdef rdma_error
    rdma_error("Server failed to create a buffer \n");
#endif
    /* we assume that it is due to out of memory error */
    return;
  }
  server_metadata_attr.address = (uint64_t) server_buffer_mr->addr;
  server_metadata_attr.length = (uint32_t) server_buffer_mr->length;
  server_metadata_attr.stag.local_stag = (uint32_t) server_buffer_mr->lkey;
  server_metadata_mr = rdma_buffer_register(pd /* which protection domain*/, 
                  &server_metadata_attr /* which memory to register */, 
                  sizeof(server_metadata_attr) /* what is the size of memory */,
                  (ibv_access_flags)IBV_ACCESS_LOCAL_WRITE /* what access permission */);
  if(!server_metadata_mr){
#ifdef rdma_error
    rdma_error("Server failed to create to hold server metadata \n");
#endif
    /* we assume that this is due to out of memory error */
    return;
  }
  server_send_sge.addr = (uint64_t) &server_metadata_attr;
  server_send_sge.length = sizeof(server_metadata_attr);
  server_send_sge.lkey = server_metadata_mr->lkey;
  /* now we link this sge to the send request */
  bzero(&server_send_wr, sizeof(server_send_wr));
  server_send_wr.sg_list = &server_send_sge;
  server_send_wr.num_sge = 1; // only 1 SGE element in the array 
  server_send_wr.opcode = IBV_WR_SEND; // This is a send request 
  server_send_wr.send_flags = IBV_SEND_SIGNALED; // We want to get notification 
  /* This is a fast data path operation. Posting an I/O request */
  ret = ibv_post_send(client_qp /* which QP */, 
                  &server_send_wr /* Send request that we prepared before */, 
                  &bad_server_send_wr /* In case of error, this will contain failed requests */);
  if (ret) {
#ifdef rdma_error
    rdma_error("Posting of server metdata failed, errno: %d \n",
                    -errno);
#endif
    return;
  }
  /* We check for completion notification */
  ret = process_work_completion_events(io_completion_channel, &wc, 1);
  if (ret != 1) {
#ifdef rdma_error
    rdma_error("Failed to send server metadata, ret = %d \n", ret);
#endif
    return;
  }
  printf("Local buffer metadata has been sent to the client. Node: %d \n", nodeID);



  //Read remote buffer.
  while (!execFinish.load()) {
    char* ch = (char*)server_buffer_mr->addr;
    bool nonFull = (ch[0] == '1');
    bool full = (ch[0] == '0');
    if (full) {
      std::cout << "NIC full." << std::endl;
    } else if (nonFull) {
      std::cout << "NIC non-full." << std::endl;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }




  //disconnect_and_cleanup
  cm_event = NULL;
  /* Now we wait for the client to send us disconnect event */
  // debug("Waiting for cm event: RDMA_CM_EVENT_DISCONNECTED\n");
  ret = process_rdma_cm_event(cm_event_channel, 
                  RDMA_CM_EVENT_DISCONNECTED, 
                  &cm_event);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to get disconnect event, ret = %d \n", ret);
#endif
    return;
  }
  /* We acknowledge the event */
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to acknowledge the cm event %d\n", -errno);
#endif
    return;
  }
  printf("A disconnect event is received from the client... Node: %d\n", nodeID);
  /* We free all the resources */
  /* Destroy QP */
  rdma_destroy_qp(cm_client_id);
  /* Destroy client cm id */
  ret = rdma_destroy_id(cm_client_id);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
#endif
  }
  /* Destroy CQ */
  ret = ibv_destroy_cq(cq);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to destroy completion queue cleanly, %d \n", -errno);
#endif
  }
  /* Destroy completion channel */
  ret = ibv_destroy_comp_channel(io_completion_channel);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to destroy completion channel cleanly, %d \n", -errno);
#endif
  }
  /* Destroy memory buffers */
  rdma_buffer_free(server_buffer_mr);
  rdma_buffer_deregister(server_metadata_mr);
  rdma_buffer_deregister(client_metadata_mr);
  /* Destroy protection domain */
  ret = ibv_dealloc_pd(pd);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);
#endif
    // we continue anyways;
  }
  /* Destroy rdma server id */
  ret = rdma_destroy_id(cm_server_id);
  if (ret) {
#ifdef rdma_error
    rdma_error("Failed to destroy server id cleanly, %d \n", -errno);
#endif
    // we continue anyways;
  }
  rdma_destroy_event_channel(cm_event_channel);
  printf("Server shut-down is complete. Node: %d\n", nodeID);

}

Execution::Execution(long queryId, 
                     const shared_ptr<::caf::actor_system> &actorSystem,
                     const vector<::caf::node_id> &nodes,
                     const ::caf::actor &segmentCacheActor,
                     const shared_ptr<PhysicalPlan> &physicalPlan,
                     bool isDistributed) :
  queryId_(queryId),
  actorSystem_(actorSystem),
  nodes_(nodes),
  segmentCacheActor_(segmentCacheActor),
  physicalPlan_(physicalPlan),
  isDistributed_(isDistributed) {
  rootActor_ = make_shared<::caf::scoped_actor>(*actorSystem_);
}

Execution::~Execution() {
  close();
}

shared_ptr<TupleSet> Execution::execute(std::string queryName) {
  queryName_ = queryName;
  boot();
  start();
  join();
  return legacyCollateOperator_->tuples();
}

void Execution::boot() {
  startTime_ = chrono::steady_clock::now();
#if FILESCANPRIORITY >0
  for (int i = 0; i < nodes_.size(); i++) {
    std::vector<std::pair<std::string, ::caf::actor> > vec;
    fileHandleSet_[i] = vec;
  }
#else
  for (int i = 0; i < nodes_.size(); i++) {
    std::unordered_set<::caf::actor> set;
    fileHandleSet_[i] = set;
  }
#endif

  // Tell segment cache actor that new query comes
  if (segmentCacheActor_) {
    (*rootActor_)->anon_send(segmentCacheActor_, NewQueryAtom_v);
  }

  // Set query id, and add physical operators to operator directory
  for (const auto &op: physicalPlan_->getPhysicalOps()) {
    assert(op);
    op->setQueryId(queryId_);
    auto result = opDirectory_.insert(POpDirectoryEntry(op, nullptr, false));
    if (!result.has_value()) {
      throw runtime_error(result.error());
    }
  }
  std::map<std::string,std::shared_ptr<PhysicalOp> > name2Def;
  name2Def.clear();
  for (const auto &element: opDirectory_){
    auto entry = element.second;
    name2Def[entry.getDef()->name()]=entry.getDef();
  }

  // std::cout<<"log physical plan Factor begin"<<std::endl;
  // std::string file_path = "/home/huazhang/Work/FPDB-build-4242/OpExecTimes.txt";
  // std::ofstream output_file(file_path, std::ios::app);
  // output_file<<"operator,factor,sum_producer_nextfactor"<<"\n";
  // for (const auto &element: opDirectory_){
  //   auto entry = element.second;
  //   auto op=entry.getDef();
  //   if(op->name().find("Predicate")!=std::string::npos){
  //     continue;
  //   }
  //   int sum_producer_nextfactor=0;
  //   for(auto proname:op->producers()){
  //     if(proname.substr(proname.find('[') + 1, proname.find(']') - proname.find('[') - 1)!="-1")
  //       continue;
  //     sum_producer_nextfactor+=name2Def[proname]->getNextFactor();
  //   }
  //   output_file<<op->name()<<","<<op->getFactor()<<","<<sum_producer_nextfactor<<"\n";
  // }
  // std::cout<<"log physical plan Factor end"<<std::endl;
  // return ;

  for (const auto &element: opDirectory_){
    auto entry = element.second;
    auto op=entry.getDef();
    if(op->name().find("Predicate")!=std::string::npos){
      continue;
    }
    for(auto proname:op->producers()){
      name2Def[proname]->setConsumerFactor(op->getFactor());
    }
  }
  // Spawn actors locally/remotely according to nodeId assigned
  for (auto &element: opDirectory_) {
    auto op = element.second.getDef();
    auto ctx = make_shared<POpContext>(*rootActor_, segmentCacheActor_);
    op->create(ctx);

    // Spawn collate at the coordinator
    if (op->getType() == POpType::COLLATE) {
      legacyCollateOperator_ = static_pointer_cast<physical::collate::CollatePOp>(op);
      op->setSpawnOnRemote(false);
      element.second.setActorHandle(localSpawn(op));
    } else {
      if (op->getNodeId() != -1) {
        //spawn on remote
        op->setSpawnOnRemote(true);
        element.second.setActorHandle(remoteSpawn(op, op->getNodeId()));
      } else {
        //spawn on local
        op->setSpawnOnRemote(false);
        element.second.setActorHandle(localSpawn(op));
      }
    }

    if (op->getType() == POpType::FILE_SCAN) {
      name2Loc_[op->name()] = op->getNodeId();
#if FILESCANPRIORITY >0
      fileHandleSet_[op->getNodeId()].push_back(std::make_pair(op->name(),element.second.getActorHandle()));
#else
      fileHandleSet_[op->getNodeId()].insert(element.second.getActorHandle());
#endif
    }
  }


  auto curTime = chrono::steady_clock::now();
  auto spawnTimeUsed = chrono::duration_cast<chrono::nanoseconds>(curTime - startTime_).count();
  std::cout << "Spawn used time: " + std::to_string((double)spawnTimeUsed / 1000000000.0) + " secs" << std::endl;
}

::caf::actor Execution::localSpawn(const shared_ptr<PhysicalOp> &op) {
  // TODO: After POpActor2 is generally finished we may use this
//  if(op->getType() == "FileScan"){
//    auto fileScanOp = static_pointer_cast<FileScan>(op);
//    auto actorHandle = operatorManager_.lock()->getActorSystem()->spawn(FileScanFunctor,
//                                                                        fileScanOp->name(),
//                                                                        fileScanOp->getKernel()->getPath(),
//                                                                        fileScanOp->getKernel()->getFileType().value(),
//                                                                        fileScanOp->getColumnNames(),
//                                                                        fileScanOp->getKernel()->getStartPos(),
//                                                                        fileScanOp->getKernel()->getFinishPos(),
//                                                                        fileScanOp->getQueryId(),
//                                                                        *rootActor_,
//                                                                        operatorManager_.lock()->getSegmentCacheActor(),
//                                                                        fileScanOp->isScanOnStart()
//    );
//    if (!actorHandle)
//      throw runtime_error(fmt::format("Failed to spawn operator actor '{}'", op->name()));
//    element.second.setActorHandle(::caf::actor_cast<::caf::actor>(actorHandle));
//  }
//  else if(op->getType() == "Collate"){
//    legacyCollateOperator_ = static_pointer_cast<Collate>(op);
//    collateActorHandle_  = operatorManager_.lock()->getActorSystem()->spawn(CollateFunctor,
//                                        legacyCollateOperator_->name(),
//                                        legacyCollateOperator_->getQueryId(),
//                                      *rootActor_,
//                                      operatorManager_.lock()->getSegmentCacheActor()
//    );
//    if (!collateActorHandle_)
//    throw runtime_error(fmt::format("Failed to spawn operator actor '{}'", op->name()));
//    element.second.setActorHandle(::caf::actor_cast<::caf::actor>(collateActorHandle_));
//  }

  // Need s3 operators to be "detached" to not block others while loading data.
  // Don't run more S3Get requests in parallel than # cores, earlier testing showed this did not help as S3Get
  // already utilizes the full network bandwidth with #cores requests whereas S3Select does not when
  // selectivity is low.
  if (op->getType() == POpType::S3_SELECT || op->getType() == POpType::S3_GET) {
    auto actorHandle = actorSystem_->spawn<POpActor, detached>(op);
    if (!actorHandle)
      throw runtime_error(fmt::format("Failed to spawn operator actor '{}'", op->name()));
    return ::caf::actor_cast<::caf::actor>(actorHandle);
  }

  else {
    auto actorHandle = actorSystem_->spawn<POpActor>(op);
    if (!actorHandle)
      throw runtime_error(fmt::format("Failed to spawn operator actor '{}'", op->name()));
    return ::caf::actor_cast<::caf::actor>(actorHandle);
  }
}

::caf::actor Execution::remoteSpawn(const shared_ptr<PhysicalOp> &op, int nodeId) {
  std::cout<<"Remote Spawn Op: "<<op->name()<<std::endl;
  auto remoteSpawnTout = std::chrono::seconds(10);
  auto args = make_message(op);

  // Need s3 operators to be "detached" to not block others while loading data.
  // Don't run more S3Get requests in parallel than # cores, earlier testing showed this did not help as S3Get
  // already utilizes the full network bandwidth with #cores requests whereas S3Select does not when
  // selectivity is low.
  auto actorTypeName = (op->getType() == POpType::S3_SELECT || op->getType() == POpType::S3_GET) ?
          "POpActor-detached" : "POpActor";

  auto expectedActorHandle = actorSystem_->middleman().remote_spawn<::caf::actor>(nodes_[nodeId],
                                                                                  actorTypeName,
                                                                                  args,
                                                                                  remoteSpawnTout);
  if (!expectedActorHandle) {
    throw std::runtime_error(fmt::format("Failed to remote-spawn operator actor '{}': {}",
                                         op->name(),
                                         to_string(expectedActorHandle.error())));
  }
  return *expectedActorHandle;
}

void Execution::start() {
  ModeCtrl ctrl(true);//controlled by external file
  // Mark all the operators as incomplete
  opDirectory_.setIncomplete();

  std::unordered_map<std::string, int> spawnAddr;
  for (auto &element: opDirectory_) {
    auto op = element.second.getDef();
    //if spawn on local: -1. otherwise: 0 ~ numNodes_ - 1
    spawnAddr.emplace(op->name(), op->getNodeId()); 
  }

  std::unordered_map<std::string, int> activateAddr;
#if (PREDICATE_EN==0) || (RUNTIME_SAMPLE_EN==0)
#if FINE_GRAIN == 0
  activateAddr.emplace("Aggregate", ctrl.activeAddr(POpType::AGGREGATE));
  activateAddr.emplace("AggregateReduce", ctrl.activeAddr(POpType::AGGREGATE));
  activateAddr.emplace("Collate", ctrl.activeAddr(POpType::COLLATE));
  activateAddr.emplace("FileScan", ctrl.activeAddr(POpType::FILE_SCAN));
  activateAddr.emplace("Filter", ctrl.activeAddr(POpType::FILTER));
  activateAddr.emplace("Group", ctrl.activeAddr(POpType::GROUP));
  activateAddr.emplace("Split", ctrl.activeAddr(POpType::SPLIT));
  activateAddr.emplace("Project", ctrl.activeAddr(POpType::PROJECT));
  activateAddr.emplace("LimitSort", ctrl.activeAddr(POpType::LIMIT_SORT));
  activateAddr.emplace("NestedLoopJoin", ctrl.activeAddr(POpType::NESTED_LOOP_JOIN));
  activateAddr.emplace("HashJoinBuild", ctrl.activeAddr(POpType::HASH_JOIN_BUILD));
  activateAddr.emplace("HashJoinProbe", ctrl.activeAddr(POpType::HASH_JOIN_PROBE));
  activateAddr.emplace("Shuffle", ctrl.activeAddr(POpType::SHUFFLE));
  activateAddr.emplace("Sort", ctrl.activeAddr(POpType::SORT));
  activateAddr.emplace("Predicate", ctrl.activeAddr(POpType::PREDICATE));
#else
  std::unordered_map<std::string, std::string> opAddr = readConfig(queryName_ + ".txt");
  for (auto &it : opAddr) {
    activateAddr.emplace(it.first, std::stoi(it.second));
  }
#endif
#endif

  // Connect the actors
  for (const auto &element: opDirectory_) {
    auto entry = element.second;

    vector<POpConnection> opConnections;
    for (const auto &producer: element.second.getDef()->producers()) {
      auto producerHandle = opDirectory_.get(producer).value().getActorHandle();
      opConnections.emplace_back(producer, producerHandle, POpRelationshipType::Producer);
    }
    for (const auto &consumer: element.second.getDef()->consumers()) {
      auto consumerHandle = opDirectory_.get(consumer).value().getActorHandle();
      opConnections.emplace_back(consumer, consumerHandle, POpRelationshipType::Consumer);
    }
    for (const auto &predictor : element.second.getDef()->predictors()) {
      auto predictorHandle = opDirectory_.get(predictor).value().getActorHandle();
      opConnections.emplace_back(predictor, predictorHandle, POpRelationshipType::Prediction);
    }
    for (const auto &sampler : element.second.getDef()->samplers()) {
      auto samplerHandle = opDirectory_.get(sampler).value().getActorHandle();
      opConnections.emplace_back(sampler, samplerHandle, POpRelationshipType::Sampler);
    }

    auto cm = make_shared<message::ConnectMessage>(opConnections, ExecutionRootActorName, spawnAddr, activateAddr,
                                                  (unsigned long long) startTime_.time_since_epoch().count());
    (*rootActor_)->anon_send(element.second.getActorHandle(), Envelope(cm));
  }

  // Start the actors
  for (const auto &element: opDirectory_) {
    auto entry = element.second;
    // std::cout<<entry.getDef()->name()<<" factor is "<<entry.getDef()->getFactor()<<std::endl;
    // std::cout<<entry.getDef()->name()<<" consumerfactor is "<<entry.getDef()->getConsumerFactor()<<std::endl;
    if (entry.getDef()->getType() != POpType::FILE_SCAN) {
      auto sm = make_shared<message::StartMessage>(ExecutionRootActorName);
      (*rootActor_)->anon_send(element.second.getActorHandle(), Envelope(sm));
    }
  }
#if FILESCANPRIORITY >0
  // probe order
  probevec_.clear();
  std::map<std::string,std::shared_ptr<PhysicalOp> > name2Def;
  name2Def.clear();
  for (const auto &element: opDirectory_){
    auto entry = element.second;
    name2Def[entry.getDef()->name()]=entry.getDef();
  }
  for (const auto &element: opDirectory_) {
    auto entry = element.second;
    if (entry.getDef()->getType() == POpType::FILE_SCAN) {
          bool buildOnPath=false;
          int buildDepth=0;
          std::shared_ptr<PhysicalOp> opDef=entry.getDef();
          while(1){
            if(opDef->consumers().size()==0){
                break;
            }
            std::string consumerName=*(opDef->consumers().begin());
            if(buildOnPath)
              buildDepth++;
            if(!buildOnPath && consumerName.find("HashJoinBuild")!=string::npos){
                buildOnPath=true;
                buildDepth++;
            }
            opDef=name2Def[consumerName];
          }
          probevec_[entry.getDef()->name()]=buildDepth;
    }
  }

   std::cout<<"probevec_"<<std::endl;
   for (auto it = probevec_.begin(); it != probevec_.end(); ++it) {
        std::cout << "Key: " << it->first << ", Value: " << it->second << std::endl;
    }
#endif
#if FILESCANPRIORITY >0
  for (int i = 0; i < nodes_.size(); i++) {
    if(fileHandleSet_[i].empty()) continue;
    std::cout<<"before sort"<<fileHandleSet_[i].size()<<std::endl;
    std::sort(fileHandleSet_[i].begin(),fileHandleSet_[i].end(),compActorName);
    std::cout<<"after sort"<<std::endl;
    for(auto k = fileHandleSet_[i].begin();k!=fileHandleSet_[i].end();k++){
      std::cout<<"@@@@"<<k->first<<std::endl;
    }
  }
#endif
}

void Execution::join() {
  for (int i = 0; i < nodes_.size(); i++) {
    std::thread sender(msgSender, i);
    sender.detach();
  }

  std::unordered_map<std::string, std::string> FilescanConfig = readConfig("filescan.conf");
  std::string p = FilescanConfig["PARALLEL"];
  int parallel = std::stoi(p);
  auto sm = make_shared<message::StartMessage>(ExecutionRootActorName);

  SPDLOG_DEBUG("Waiting for all operators to complete");

  auto handle_err = [&](const ::caf::error &err) {
    throw runtime_error(to_string(err));
  };

  for (int i = 0; i < nodes_.size(); i++) {
    for (int j = 0; j < parallel && !fileHandleSet_[i].empty(); j++) {
      auto head = fileHandleSet_[i].begin();
#if FILESCANPRIORITY >0
      std::cout<<"####"<<head->first<<std::endl;
      (*rootActor_)->anon_send((head->second), Envelope(sm));
#else
      (*rootActor_)->anon_send(*head, Envelope(sm));
#endif
      fileHandleSet_[i].erase(head);
    }
  }

  bool allComplete = false;
  (*rootActor_)->receive_while([&] { return !allComplete; })(
          [&](const Envelope &e) {
            const auto &msg = e.message();
            SPDLOG_DEBUG("Query root actor received message  |  query: '{}', messageKind: '{}', from: '{}'",
                         queryId_, msg.getTypeString(), msg.sender());

            auto errAct = [&](const std::string &errMsg) {
              allComplete = true;
              close();
              throw runtime_error(errMsg);
            };

            switch (msg.type()) {
              case MessageType::COMPLETE: {
                this->opDirectory_.setComplete(msg.sender())
                        .map_error(errAct);
                allComplete = this->opDirectory_.allComplete();
                
                break;
              }
              case MessageType::ERROR: {
                errAct(fmt::format("ERROR: {}, from {}", ((ErrorMessage &) msg).getContent(), msg.sender()));
              }
              case MessageType::DEBUG: {
                auto snd = msg.sender();
                if (snd.substr(0, 8).compare("FileScan") == 0) {
                  int nodeID = name2Loc_[snd];
                  if (!fileHandleSet_[nodeID].empty()) {
                    auto head = fileHandleSet_[nodeID].begin();
#if FILESCANPRIORITY >0
                    (*rootActor_)->anon_send((head->second), Envelope(sm));
#else
                    (*rootActor_)->anon_send(*head, Envelope(sm));
#endif
                    fileHandleSet_[nodeID].erase(head);
                  }
                  
                }
                break;
              }
              default: {
                errAct(fmt::format("Invalid message type sent to the root actor: {}, from {}", msg.getTypeString(), msg.sender()));
              }
            }

          },
          handle_err);

  // TODO: After POpActor2 is generally finished we may use this
//  (*rootActor_)->request(collateActorHandle_, ::caf::infinite, GetTupleSetAtom::value).receive(
//	  [&](const tl::expected<shared_ptr<TupleSet>, string> &expectedTupleSet) {
//		legacyCollateOperator_->setTuples(expectedTupleSet.value());
//	  },
//	  [&](const ::caf::error&  error){
//		throw runtime_error(to_string(error));
//	  });
  execFinish = true;
  stopTime_ = chrono::steady_clock::now();
}

void Execution::close() {
  if(rootActor_ != nullptr){
    for (const auto &element: opDirectory_) {
      (*rootActor_)->send_exit(element.second.getActorHandle(), ::caf::exit_reason::user_shutdown);
    }
    rootActor_.reset();
  }
  legacyCollateOperator_.reset();
}

void Execution::write_graph(const string &file) {
  auto gvc = gvContext();

  auto graph = agopen(const_cast<char *>(string("Execution Plan").c_str()), Agstrictdirected, nullptr);

  // Init attributes
  agattr(graph, AGNODE, const_cast<char *>("fixedsize"), const_cast<char *>("false"));
  agattr(graph, AGNODE, const_cast<char *>("shape"), const_cast<char *>("ellipse"));
  agattr(graph, AGNODE, const_cast<char *>("label"), const_cast<char *>("<not set>"));
  agattr(graph, AGNODE, const_cast<char *>("fontname"), const_cast<char *>("Arial"));
  agattr(graph, AGNODE, const_cast<char *>("fontsize"), const_cast<char *>("8"));

  // Add all the nodes
  for (const auto &op: this->opDirectory_) {
    string nodeName = op.second.getDef()->name();
    auto node = agnode(graph, const_cast<char *>(nodeName.c_str()), true);

    agset(node, const_cast<char *>("shape"), const_cast<char *>("plaintext"));

    string nodeLabel = "<table border='1' cellborder='0' cellpadding='5'>"
                            "<tr><td align='left'><b>" + op.second.getDef()->getTypeString() + "</b></td></tr>"
                                                                                         "<tr><td align='left'>"
                            + op.second.getDef()->name() + "</td></tr>"
                                                           "</table>";
    char *htmlNodeLabel = agstrdup_html(graph, const_cast<char *>(nodeLabel.c_str()));
    agset(node, const_cast<char *>("label"), htmlNodeLabel);
    agstrfree(graph, htmlNodeLabel);
  }

  // Add all the edges
  for (const auto &op: this->opDirectory_) {
    auto opNode = agfindnode(graph, (char *)(op.second.getDef()->name().c_str()));
    for (const auto &c: op.second.getDef()->consumers()) {
      auto consumerOpNode = agfindnode(graph, (char *)(c.c_str()));
      agedge(graph, opNode, consumerOpNode, const_cast<char *>(string("Edge").c_str()), true);
    }
  }

  const filesystem::path &path = filesystem::path(file);
  if (!filesystem::exists(path.parent_path())) {
    throw runtime_error("Could not open file '" + file + "' for writing. Parent directory does not exist");
  } else {
    FILE *outFile = fopen(file.c_str(), "w");
    if (outFile == nullptr) {
      throw runtime_error("Could not open file '" + file + "' for writing. Errno: " + to_string(errno));
    }

    gvLayout(gvc, graph, "dot");
    gvRender(gvc, graph, "svg", outFile);

    fclose(outFile);

    gvFreeLayout(gvc, graph);
    agclose(graph);
    gvFreeContext(gvc);
  }
}

long Execution::getQueryId() const {
  return queryId_;
}

long Execution::getElapsedTime() {
  if (startTime_.time_since_epoch().count() == 0)
    throw runtime_error("Execution time unavailable, query has not been started");
  if (stopTime_.time_since_epoch().count() == 0)
    throw runtime_error("Execution time unavailable, query has not been stopped");

  return chrono::duration_cast<chrono::nanoseconds>(stopTime_ - startTime_).count();
}

shared_ptr<PhysicalOp> Execution::getPhysicalOp(const string &name) {
  const auto expectedEntry = opDirectory_.get(name);
  if (!expectedEntry.has_value()) {
    throw runtime_error(expectedEntry.error());
  }
  return expectedEntry.value().getDef();
}

physical::s3::S3SelectScanStats Execution::getAggregateS3SelectScanStats() {
  physical::s3::S3SelectScanStats aggregateS3SelectScanStats = {0, 0, 0, 0 , 0, 0 ,0, 0}; // initialize all fields to 0
  for (const auto &entry: opDirectory_) {
    const auto op = entry.second.getDef();
    if (op->getType() == POpType::S3_GET || op->getType() == POpType::S3_SELECT) {
      auto s3SelectScanOp = static_pointer_cast<physical::s3::S3SelectScanAbstractPOp>(op);
      physical::s3::S3SelectScanStats currentS3SelectScanStats = s3SelectScanOp->getS3SelectScanStats();

      // Add these s3SelectScanStats to our aggregate stats
      aggregateS3SelectScanStats.processedBytes += currentS3SelectScanStats.processedBytes;
      aggregateS3SelectScanStats.returnedBytes += currentS3SelectScanStats.returnedBytes;
      aggregateS3SelectScanStats.outputBytes += currentS3SelectScanStats.outputBytes;
      aggregateS3SelectScanStats.numRequests += currentS3SelectScanStats.numRequests;
      aggregateS3SelectScanStats.getTransferTimeNS += currentS3SelectScanStats.getTransferTimeNS;
      aggregateS3SelectScanStats.getConvertTimeNS += currentS3SelectScanStats.getConvertTimeNS;
      aggregateS3SelectScanStats.selectTransferTimeNS += currentS3SelectScanStats.selectTransferTimeNS;
      aggregateS3SelectScanStats.selectConvertTimeNS += currentS3SelectScanStats.selectConvertTimeNS;
    }
  }
  return aggregateS3SelectScanStats;
}

std::tuple<size_t, size_t, size_t> Execution::getFilterTimeNSInputOutputBytes() {
  size_t timeNS = 0;
  size_t inputBytes = 0;
  size_t outputBytes = 0;
  for (const auto &entry: opDirectory_) {
    const auto op = entry.second.getDef();
    if (op->getType() == POpType::FILTER) {
      auto filterOp = static_pointer_cast<filter::FilterPOp>(entry.second.getDef());

      timeNS += filterOp->getFilterTimeNS();
      inputBytes += filterOp->getFilterInputBytes();
      outputBytes += filterOp->getFilterOutputBytes();
    }
  }
  return {timeNS, inputBytes, outputBytes};
}

string Execution::showMetrics(bool showOpTimes, bool showScanMetrics) {
  stringstream ss;
  if (!showOpTimes && !showScanMetrics) {
    return ss.str();
  }

  ss << endl;
  ss << "Metrics |" << endl << endl;
  long totalProcessingTime = 0;
  for (auto &entry : opDirectory_) {
    (*rootActor_)->request(entry.second.getActorHandle(), ::caf::infinite, GetProcessingTimeAtom_v).receive(
            [&](long processingTime) {
              totalProcessingTime += processingTime;
            },
            [&](const ::caf::error&  error){
              throw runtime_error(to_string(error));
            });
  }

  if (showOpTimes) {
    auto totalExecutionTime = getElapsedTime();
    stringstream formattedExecutionTime;
    formattedExecutionTime << totalExecutionTime << " \u33B1" << " (" << ((double) totalExecutionTime / 1000000000.0)
                           << " secs)";
    ss << left << setw(60) << "Total Execution Time ";
    ss << left << setw(60) << formattedExecutionTime.str();
    ss << endl;
    ss << endl;

    ss << left << setw(120) << "Operator Execution Times" << endl;
    ss << setfill(' ');

    ss << left << setw(120) << setfill('-') << "" << endl;
    ss << setfill(' ');

    ss << left << setw(60) << "Operator";
    ss << left << setw(40) << "Execution Time";
    ss << left << setw(20) << "% Total Time";
    ss << endl;

    ss << left << setw(120) << setfill('-') << "" << endl;
    ss << setfill(' ');

    map<string, size_t> opTypeToRuntime;

    for (auto &entry : opDirectory_) {
      auto operatorName = entry.first;

      long processingTime;
      (*rootActor_)->request(entry.second.getActorHandle(), ::caf::infinite, GetProcessingTimeAtom_v).receive(
              [&](long time) {
                processingTime = time;
              },
              [&](const ::caf::error &error) {
                throw runtime_error(to_string(error));
              });

      auto op = entry.second.getDef();
      string typeString = op->getTypeString();
      if (opTypeToRuntime.find(typeString) == opTypeToRuntime.end()) {
        opTypeToRuntime.emplace(typeString, processingTime);
      } else {
        opTypeToRuntime[typeString] = opTypeToRuntime[typeString] + processingTime;
      }

      auto processingFraction = (double) processingTime / (double) totalProcessingTime;
      stringstream formattedProcessingTime;
      formattedProcessingTime << processingTime << " \u33B1" << " (" << ((double) processingTime / 1000000000.0)
                              << " secs)";
      stringstream formattedProcessingPercentage;
      formattedProcessingPercentage << (processingFraction * 100.0);
      ss << left << setw(60) << operatorName;
      ss << left << setw(40) << formattedProcessingTime.str();
      ss << left << setw(20) << formattedProcessingPercentage.str();
      ss << endl;
    }

    ss << left << setw(120) << setfill('-') << "" << endl;
    ss << setfill(' ');
    ss << endl;

    for (auto const &opTime : opTypeToRuntime) {
      stringstream formattedOpTime;
      formattedOpTime << ((double) opTime.second / 1000000000.0) << " secs";
      ss << left << setw(60) << opTime.first;
      ss << left << setw(40) << formattedOpTime.str();
      ss << left << setw(20) << ((double) opTime.second / (double) totalProcessingTime) * 100.0;
      ss << endl;
    }

    ss << left << setw(120) << setfill('-') << "" << endl;
    ss << setfill(' ');

    stringstream formattedProcessingTime;
    formattedProcessingTime << totalProcessingTime << " \u33B1" << " (" << ((double) totalProcessingTime / 1000000000.0)
                            << " secs)";
    ss << left << setw(60) << "Total ";
    ss << left << setw(40) << formattedProcessingTime.str();
    ss << left << setw(20) << "100.0";
    ss << endl;
    ss << endl;
  }

  if (showScanMetrics) {
    physical::s3::S3SelectScanStats s3SelectScanStats = getAggregateS3SelectScanStats();

    stringstream formattedProcessedBytes;
    formattedProcessedBytes << s3SelectScanStats.processedBytes << " B" << " ("
                            << ((double) s3SelectScanStats.processedBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";
    stringstream formattedReturnedBytes;
    formattedReturnedBytes << s3SelectScanStats.returnedBytes << " B" << " ("
                           << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";
    stringstream formattedArrowConvertedBytes;
    formattedArrowConvertedBytes << s3SelectScanStats.outputBytes << " B" << " ("
                                 << ((double) s3SelectScanStats.outputBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";

    stringstream formattedConversionOutputRate;
    if (s3SelectScanStats.getConvertTimeNS + s3SelectScanStats.selectConvertTimeNS > 0) {
      formattedConversionOutputRate << ((double) s3SelectScanStats.outputBytes / 1024.0 / 1024.0) /
                                       ((double) (s3SelectScanStats.getConvertTimeNS +
                                                  s3SelectScanStats.selectConvertTimeNS) / 1.0e9) << " MB/s/req";
    } else {
      formattedConversionOutputRate << "NA";
    }

    stringstream formattedStorageFormatToArrowSizeX;
    if (s3SelectScanStats.outputBytes > 0) {
      formattedStorageFormatToArrowSizeX
              << (double) s3SelectScanStats.returnedBytes / (double) s3SelectScanStats.outputBytes << "x";
    } else {
      formattedStorageFormatToArrowSizeX << "NA";
    }
    ss << left << setw(60) << "Processed Bytes";
    ss << left << setw(60) << formattedProcessedBytes.str();
    ss << endl;
    ss << left << setw(60) << "Returned Bytes";
    ss << left << setw(60) << formattedReturnedBytes.str();
    ss << endl;
    ss << left << setw(60) << "Arrow Converted Bytes";
    ss << left << setw(60) << formattedArrowConvertedBytes.str();
    ss << endl;
    ss << left << setw(60) << "Conversion Output Rate";
    ss << left << setw(60) << formattedConversionOutputRate.str();
    ss << endl;
    ss << left << setw(60) << "Storage/Compute Data Ratio";
    ss << left << setw(60) << formattedStorageFormatToArrowSizeX.str();
    ss << endl;

    stringstream formattedGetTransferRate;
    stringstream formattedGetConvertRate;
    stringstream formattedGetTransferConvertRate;
    if (s3SelectScanStats.getTransferTimeNS > 0 && s3SelectScanStats.getConvertTimeNS > 0) {
      formattedGetTransferRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                  ((double) s3SelectScanStats.getTransferTimeNS / 1.0e9) << " MB/s/req";
      formattedGetConvertRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                 ((double) s3SelectScanStats.getConvertTimeNS / 1.0e9) << " MB/s/req";
      formattedGetTransferConvertRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                         (((double) s3SelectScanStats.getTransferTimeNS +
                                         (double) s3SelectScanStats.getConvertTimeNS) / 1.0e9) << " MB/s/req";
    } else {
      formattedGetTransferRate << "NA";
      formattedGetConvertRate << "NA";
      formattedGetTransferConvertRate << "NA";
    }
    // Caf actor framework seems to converge #workers -> # cores for non detached workers, and each worker runs to
    // completion. This should approximate to per core rates rather than just per request, as one request maps to a core
    ss << left << setw(60) << "S3 GET Data Transfer Rate";
    ss << left << setw(60) << formattedGetTransferRate.str();
    ss << endl;
    ss << left << setw(60) << "S3 GET Data Convert rate";
    ss << left << setw(60) << formattedGetConvertRate.str();
    ss << endl;
    ss << left << setw(60) << "S3 GET Data Transfer + Convert rate";
    ss << left << setw(60) << formattedGetTransferConvertRate.str();
    ss << endl;

    stringstream formattedSelectTransferRate;
    stringstream formattedSelectConvertRate;
    stringstream formattedSelectTransferConvertRate;
    if (s3SelectScanStats.selectTransferTimeNS > 0 && s3SelectScanStats.selectConvertTimeNS > 0) {
      formattedSelectTransferRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                     ((double) (s3SelectScanStats.selectTransferTimeNS) / 1.0e9) << " MB/s/req";
      formattedSelectConvertRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                    ((double) s3SelectScanStats.selectConvertTimeNS / 1.0e9) << " MB/s/req";
      formattedSelectTransferConvertRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                            ((double) (s3SelectScanStats.selectTransferTimeNS +
                                                       s3SelectScanStats.selectConvertTimeNS) / 1.0e9) << " MB/s/req";
    } else {
      formattedSelectTransferRate << "NA";
      formattedSelectConvertRate << "NA";
      formattedSelectTransferConvertRate << "NA";
    }

    // FIXME: This only works if the query is entirely pushdown (it only uses S3 Select and never GET), as the bytes
    //        transferred is grouped together for select and get requests, so they are not differentiated
    //        If we decide to eventually allow GET and Select requests to be in the same query for a mode we will need
    //        to have a way to differentiate this (this isn't the case for now though so it is fine)
    //        Also note that some storage backends such as Airmettle don't tell use processedBytes, in which case
    //        we estimate this value with the select range.
    stringstream formattedS3SelectSelectivityPercent;
    if (s3SelectScanStats.returnedBytes > 0 && s3SelectScanStats.processedBytes &&
        s3SelectScanStats.selectTransferTimeNS > 0 && s3SelectScanStats.getConvertTimeNS == 0) {
      formattedS3SelectSelectivityPercent << (double) s3SelectScanStats.returnedBytes /
                                             (double) s3SelectScanStats.processedBytes * 100 << "%";
    } else {
      formattedS3SelectSelectivityPercent << "NA";
    }
    ss << left << setw(60) << "Appx S3 Select Data Transfer Rate";
    ss << left << setw(60) << formattedSelectTransferRate.str();
    ss << endl;
    ss << left << setw(60) << "S3 Select Data Convert rate";
    ss << left << setw(60) << formattedSelectConvertRate.str();
    ss << endl;
    ss << left << setw(60) << "S3 Select Data Transfer + Convert rate";
    ss << left << setw(60) << formattedSelectTransferConvertRate.str();
    ss << endl;
    ss << left << setw(60) << "S3 Selectivity";
    ss << left << setw(60) << formattedS3SelectSelectivityPercent.str();
    ss << endl;

    auto filterTimeNSInputOutputBytes = getFilterTimeNSInputOutputBytes();
    size_t filterTimeNS = get<0>(filterTimeNSInputOutputBytes);
    size_t filterInputBytes = get<1>(filterTimeNSInputOutputBytes);
    size_t filterOutputBytes = get<2>(filterTimeNSInputOutputBytes);
    stringstream formattedLocalFilterRateGBs;
    stringstream formattedLocalFilterGB;
    stringstream formattedLocalFilterSelectivity;
    if (filterTimeNS > 0) {
      double filterGB = ((double) filterInputBytes / 1024.0 / 1024.0 / 1024.0);
      formattedLocalFilterRateGBs << filterGB / ((double) filterTimeNS / 1.0e9) << " GB/s/req";
      formattedLocalFilterGB << filterGB << " GB";
      formattedLocalFilterSelectivity << ((double) filterOutputBytes / (double) filterInputBytes) * 100 << "%";
    } else {
      formattedLocalFilterRateGBs << "NA";
      formattedLocalFilterGB << "NA";
      formattedLocalFilterSelectivity << "NA";
    }
    // Caf actor framework seems to converge #workers -> # cores, and each worker runs to completion
    // so this should approximate to per core rates rather than just per request, as one request maps to a core
    ss << left << setw(60) << "Local filter rate";
    ss << left << setw(60) << formattedLocalFilterRateGBs.str();
    ss << endl;
    ss << left << setw(60) << "Local filter bytes";
    ss << left << setw(60) << formattedLocalFilterGB.str();
    ss << endl;
    ss << left << setw(60) << "Local filter selectivity (bytes)";
    ss << left << setw(60) << formattedLocalFilterSelectivity.str();
    ss << endl;
    ss << endl;
  }

  return ss.str();
}

}
