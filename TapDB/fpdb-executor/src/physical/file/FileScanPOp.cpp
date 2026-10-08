#include <fpdb/executor/physical/file/FileScanPOp.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/cache/CacheHelper.h>
#include <fpdb/executor/message/Message.h>
#include <fpdb/catalogue/format/CSVFormat.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/catalogue/local-fs/LocalFSPartition.h>
#include <fpdb/tuple/TupleSet.h>

#include <arrow/csv/options.h> 
#include <arrow/csv/reader.h>
#include <arrow/io/buffered.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/type_fwd.h>            // for default_memory_pool
#include <arrow/csv/api.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/wait.h>
#include <memory>                      // for make_unique, unique_ptr, __sha...
#include <utility>
#include <filesystem>
#include <algorithm> //transform
#include <iostream>
#include <unistd.h>

#include <fpdb/util/Util.h>
#include <fpdb/executor/rdma_common.h>
#include <fpdb/expression/gandiva/Filter.h>

#include <thread>
#include <chrono>
#include <random>
#include <arrow/compute/api.h>
using namespace fpdb::expression::gandiva;
using namespace fpdb::executor::physical::cache;
using namespace fpdb::executor::message;
using namespace fpdb::tuple;
using namespace fpdb::catalogue::format;
using namespace std;

namespace arrow { class MemoryPool; }
string mytoupper(string s){
    string res = "";
    int len=s.size();
    for(int i=0;i<len;i++){
        if(s[i]>='a'&&s[i]<='z'){
            s[i]-=32;
     res += s[i];
      } else {
      res += s[i];
     }
    }
  return res;
}

namespace fpdb::executor::physical::file {

std::shared_ptr<TupleSet> FileScanPOp::readCSVFile(std::shared_ptr<arrow::io::ReadableFile> &arrowInputStream) {
    auto ioContext = arrow::io::IOContext();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto read_options = arrow::csv::ReadOptions::Defaults();
    read_options.use_threads = false;
    auto convert_options = arrow::csv::ConvertOptions::Defaults();
    std::unordered_map<std::string, std::shared_ptr<::arrow::DataType>> columnTypes;
    std::vector<std::string> temp = getProjectColumnNames();
    for (int i=0; i < temp.size(); i++) {
        temp[i] = mytoupper(temp[i]);
    }
    convert_options.include_columns = temp;
    parse_options.delimiter = '|';
    for(const auto &columnName: this->projectColumnNames_) {
        columnTypes.emplace(mytoupper(columnName), table_->getSchema()->GetFieldByName(columnName)->type());
    }
    convert_options.column_types = columnTypes;
    // Instantiate TableReader from input stream and options
    auto makeReaderResult = arrow::csv::TableReader::Make(ioContext,
                                                            arrowInputStream,
                                                            read_options,
                                                            parse_options,
                                                            convert_options);
    if (!makeReaderResult.ok()) {
        ctx()->notifyError(fmt::format(
                "Cannot parse S3 payload  |  Could not create a table reader, error: '{}'",
                makeReaderResult.status().message()));
    }
    std::shared_ptr<arrow::csv::TableReader> tableReader = *makeReaderResult;
    // Parse the payload and create the tupleset
    auto expTupleSet = TupleSet::make(tableReader);
    if (!expTupleSet.has_value()) {
        ctx()->notifyError(expTupleSet.error());
    }
    //std::cout << expTupleSet->toString() << std::endl;
    return *expTupleSet;
}
std::shared_ptr<TupleSet> FileScanPOp::readCSVFile_randomAcross(){
	auto ioContext = arrow::io::IOContext();
    std::shared_ptr<arrow::Table> table;
    std::shared_ptr<arrow::Schema> schema;
    bool firstFile = true;
	std::unordered_map<std::string, std::string> pathConfig = readConfig("filescan.conf");
  	uint16_t LENGTH=std::stoi(pathConfig["LENGTH"]);
    for (const auto& entry : linesToRead_) {
		std::cout<<"FileScanPOp::readCSVFile_randomAcross() start one file "<<name()<<std::endl;
		std::vector<int> offsets = entry.second;
		std::sort(offsets.begin(),offsets.end());
		std::string str_lines=readWithOffsets(entry.first,offsets,LENGTH);
		// std::cout<<str_lines<<std::endl;

		std::vector<std::string> lines;
		std::istringstream stream(str_lines);
		std::string line;
		while (std::getline(stream, line)) {
			lines.push_back(line);
		}

		std::cout<<"FileScanPOp::readCSVFile_randomAcross() get string "<<name()<<std::endl;
		std::shared_ptr<arrow::Table> temp_table=strings_to_table(lines);
        if (firstFile) {
            schema = temp_table->schema();
            firstFile = false;
        }

        if (table == nullptr) {
            table = temp_table;
        } else {
            // 将新表附加到现有表
            auto combined_table = arrow::ConcatenateTables({ table, temp_table });
            if (!combined_table.ok()) {
                std::cerr << "合并表时出错: " << combined_table.status().ToString() << std::endl;
                continue;
            }
            table = combined_table.ValueOrDie();
        }
	std::cout<<"FileScanPOp::readCSVFile_randomAcross() get one file "<<name()<<std::endl;
    }
	std::cout<<"FileScanPOp::readCSVFile_randomAcross() get table "<<name()<<std::endl;
	auto expTupleSet = TupleSet::make(table);
    return expTupleSet;

}

/* These are basic RDMA resources */
/* These are RDMA connection related resources */
static struct rdma_event_channel *cm_event_channel = NULL;
static struct rdma_cm_id *cm_client_id = NULL;
static struct ibv_pd *pd = NULL;
static struct ibv_comp_channel *io_completion_channel = NULL;
static struct ibv_cq *client_cq = NULL;
static struct ibv_qp_init_attr qp_init_attr;
static struct ibv_qp *client_qp;
/* These are memory buffers related resources */
static struct ibv_mr *client_metadata_mr = NULL, 
		     *client_src_mr = NULL, 
		     *client_dst_mr = NULL, 
		     *server_metadata_mr = NULL;
static struct rdma_buffer_attr client_metadata_attr, server_metadata_attr;
static struct ibv_send_wr client_send_wr, *bad_client_send_wr = NULL;
static struct ibv_recv_wr server_recv_wr, *bad_server_recv_wr = NULL;
static struct ibv_sge client_send_sge, server_recv_sge;
/* Source and Destination buffers, where RDMA operations source and sink */
static char *src = NULL, *dst = NULL; 

/* This is our testing function */
static int check_src_dst() 
{
	return memcmp((void*) src, (void*) dst, strlen(src));
}

/* This function prepares client side connection resources for an RDMA connection */
static int client_prepare_connection(struct sockaddr_in *s_addr)
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	/*  Open a channel used to report asynchronous communication event */
	cm_event_channel = rdma_create_event_channel();
	if (!cm_event_channel) {
		#ifdef rdma_error
		rdma_error("Creating cm event channel failed, errno: %d \n", -errno);
		#endif
		return -errno;
	}
	// debug("RDMA CM event channel is created at : %p \n", cm_event_channel);
	/* rdma_cm_id is the connection identifier (like socket) which is used 
	 * to define an RDMA connection. 
	 */
	ret = rdma_create_id(cm_event_channel, &cm_client_id, 
			NULL,
			RDMA_PS_TCP);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Creating cm id failed with errno: %d \n", -errno); 
		#endif
		return -errno;
	}
	/* Resolve destination and optional source addresses from IP addresses  to
	 * an RDMA address.  If successful, the specified rdma_cm_id will be bound
	 * to a local device. */
	ret = rdma_resolve_addr(cm_client_id, NULL, (struct sockaddr*) s_addr, 2000);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to resolve address, errno: %d \n", -errno);
		#endif
		return -errno;
	}
	// debug("waiting for cm event: RDMA_CM_EVENT_ADDR_RESOLVED\n");
	ret  = process_rdma_cm_event(cm_event_channel, 
			RDMA_CM_EVENT_ADDR_RESOLVED,
			&cm_event);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to receive a valid event, ret = %d \n", ret);
		#endif
		return ret;
	}
	/* we ack the event */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to acknowledge the CM event, errno: %d\n", -errno);
		#endif
		return -errno;
	}
	// debug("RDMA address is resolved \n");

	 /* Resolves an RDMA route to the destination address in order to 
	  * establish a connection */
	ret = rdma_resolve_route(cm_client_id, 2000);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to resolve route, erno: %d \n", -errno);
		#endif
	       return -errno;
	}
	// debug("waiting for cm event: RDMA_CM_EVENT_ROUTE_RESOLVED\n");
	ret = process_rdma_cm_event(cm_event_channel, 
			RDMA_CM_EVENT_ROUTE_RESOLVED,
			&cm_event);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to receive a valid event, ret = %d \n", ret);
		#endif
		return ret;
	}
	/* we ack the event */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to acknowledge the CM event, errno: %d \n", -errno);
		#endif
		return -errno;
	}
	printf("Trying to connect to server at : %s port: %d \n", 
			inet_ntoa(s_addr->sin_addr),
			ntohs(s_addr->sin_port));
	/* Protection Domain (PD) is similar to a "process abstraction" 
	 * in the operating system. All resources are tied to a particular PD. 
	 * And accessing recourses across PD will result in a protection fault.
	 */
	pd = ibv_alloc_pd(cm_client_id->verbs);
	if (!pd) {
		#ifdef rdma_error
		rdma_error("Failed to alloc pd, errno: %d \n", -errno);
		#endif
		return -errno;
	}
	// debug("pd allocated at %p \n", pd);
	/* Now we need a completion channel, were the I/O completion 
	 * notifications are sent. Remember, this is different from connection 
	 * management (CM) event notifications. 
	 * A completion channel is also tied to an RDMA device, hence we will 
	 * use cm_client_id->verbs. 
	 */
	io_completion_channel = ibv_create_comp_channel(cm_client_id->verbs);
	if (!io_completion_channel) {
		#ifdef rdma_error
		rdma_error("Failed to create IO completion event channel, errno: %d\n",
			       -errno);
		#endif
	return -errno;
	}
	// debug("completion event channel created at : %p \n", io_completion_channel);
	/* Now we create a completion queue (CQ) where actual I/O 
	 * completion metadata is placed. The metadata is packed into a structure 
	 * called struct ibv_wc (wc = work completion). ibv_wc has detailed 
	 * information about the work completion. An I/O request in RDMA world 
	 * is called "work" ;) 
	 */
	client_cq = ibv_create_cq(cm_client_id->verbs /* which device*/, 
			CQ_CAPACITY /* maximum capacity*/, 
			NULL /* user context, not used here */,
			io_completion_channel /* which IO completion channel */, 
			0 /* signaling vector, not used here*/);
	if (!client_cq) {
		#ifdef rdma_error
		rdma_error("Failed to create CQ, errno: %d \n", -errno);
		#endif
		return -errno;
	}
	// debug("CQ created at %p with %d elements \n", client_cq, client_cq->cqe);
	ret = ibv_req_notify_cq(client_cq, 0);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to request notifications, errno: %d\n", -errno);
		#endif
		return -errno;
	}
       /* Now the last step, set up the queue pair (send, recv) queues and their capacity.
         * The capacity here is define statically but this can be probed from the 
	 * device. We just use a small number as defined in rdma_common.h */
       bzero(&qp_init_attr, sizeof qp_init_attr);
       qp_init_attr.cap.max_recv_sge = MAX_SGE; /* Maximum SGE per receive posting */
       qp_init_attr.cap.max_recv_wr = MAX_WR; /* Maximum receive posting capacity */
       qp_init_attr.cap.max_send_sge = MAX_SGE; /* Maximum SGE per send posting */
       qp_init_attr.cap.max_send_wr = MAX_WR; /* Maximum send posting capacity */
       qp_init_attr.qp_type = IBV_QPT_RC; /* QP type, RC = Reliable connection */
       /* We use same completion queue, but one can use different queues */
       qp_init_attr.recv_cq = client_cq; /* Where should I notify for receive completion operations */
       qp_init_attr.send_cq = client_cq; /* Where should I notify for send completion operations */
       /*Lets create a QP */
       ret = rdma_create_qp(cm_client_id /* which connection id */,
		       pd /* which protection domain*/,
		       &qp_init_attr /* Initial attributes */);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to create QP, errno: %d \n", -errno);
		#endif
	       return -errno;
	}
	client_qp = cm_client_id->qp;
	// debug("QP created at %p \n", client_qp);
	return 0;
}

/* Pre-posts a receive buffer before calling rdma_connect () */
static int client_pre_post_recv_buffer()
{
	int ret = -1;
	server_metadata_mr = rdma_buffer_register(pd,
			&server_metadata_attr,
			sizeof(server_metadata_attr),
			(ibv_access_flags)(IBV_ACCESS_LOCAL_WRITE));
	if(!server_metadata_mr){
		#ifdef rdma_error
		rdma_error("Failed to setup the server metadata mr , -ENOMEM\n");
		#endif
		return -ENOMEM;
	}
	server_recv_sge.addr = (uint64_t) server_metadata_mr->addr;
	server_recv_sge.length = (uint32_t) server_metadata_mr->length;
	server_recv_sge.lkey = (uint32_t) server_metadata_mr->lkey;
	/* now we link it to the request */
	bzero(&server_recv_wr, sizeof(server_recv_wr));
	server_recv_wr.sg_list = &server_recv_sge;
	server_recv_wr.num_sge = 1;
	ret = ibv_post_recv(client_qp /* which QP */,
		      &server_recv_wr /* receive work request*/,
		      &bad_server_recv_wr /* error WRs */);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		#endif
		return ret;
	}
	// debug("Receive buffer pre-posting is successful \n");
	return 0;
}

/* Connects to the RDMA server */
static int client_connect_to_server() 
{
	struct rdma_conn_param conn_param;
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	bzero(&conn_param, sizeof(conn_param));
	conn_param.initiator_depth = 3;
	conn_param.responder_resources = 3;
	conn_param.retry_count = 3; // if fail, then how many times to retry
	ret = rdma_connect(cm_client_id, &conn_param);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to connect to remote host , errno: %d\n", -errno);
		#endif
		return -errno;
	}
	// debug("waiting for cm event: RDMA_CM_EVENT_ESTABLISHED\n");
	ret = process_rdma_cm_event(cm_event_channel, 
			RDMA_CM_EVENT_ESTABLISHED,
			&cm_event);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to get cm event, ret = %d \n", ret);
		#endif
	       return ret;
	}
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to acknowledge cm event, errno: %d\n", 
			       -errno);
		#endif
		return -errno;
	}
	printf("The client is connected successfully \n");
	return 0;
}

/* Exchange buffer metadata with the server. The client sends its, and then receives
 * from the server. The client-side metadata on the server is _not_ used because
 * this program is client driven. But it shown here how to do it for the illustration
 * purposes
 */
static int client_xchange_metadata_with_server()
{
	struct ibv_wc wc[2];
	int ret = -1;
	client_src_mr = rdma_buffer_register(pd,
			src,
			strlen(src),
			(ibv_access_flags)(IBV_ACCESS_LOCAL_WRITE|
			 IBV_ACCESS_REMOTE_READ|
			 IBV_ACCESS_REMOTE_WRITE));
	if(!client_src_mr){
		#ifdef rdma_error
		rdma_error("Failed to register the first buffer, ret = %d \n", ret);
		#endif
		return ret;
	}
	/* we prepare metadata for the first buffer */
	client_metadata_attr.address = (uint64_t) client_src_mr->addr; 
	client_metadata_attr.length = client_src_mr->length; 
	client_metadata_attr.stag.local_stag = client_src_mr->lkey;
	/* now we register the metadata memory */
	client_metadata_mr = rdma_buffer_register(pd,
			&client_metadata_attr,
			sizeof(client_metadata_attr),
			(ibv_access_flags)IBV_ACCESS_LOCAL_WRITE);
	if(!client_metadata_mr) {
		#ifdef rdma_error
		rdma_error("Failed to register the client metadata buffer, ret = %d \n", ret);
		#endif
		return ret;
	}
	/* now we fill up SGE */
	client_send_sge.addr = (uint64_t) client_metadata_mr->addr;
	client_send_sge.length = (uint32_t) client_metadata_mr->length;
	client_send_sge.lkey = client_metadata_mr->lkey;
	/* now we link to the send work request */
	bzero(&client_send_wr, sizeof(client_send_wr));
	client_send_wr.sg_list = &client_send_sge;
	client_send_wr.num_sge = 1;
	client_send_wr.opcode = IBV_WR_SEND;
	client_send_wr.send_flags = IBV_SEND_SIGNALED;
	/* Now we post it */
	ret = ibv_post_send(client_qp, 
		       &client_send_wr,
	       &bad_client_send_wr);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to send client metadata, errno: %d \n", 
				-errno);
		#endif
		return -errno;
	}
	/* at this point we are expecting 2 work completion. One for our 
	 * send and one for recv that we will get from the server for 
	 * its buffer information */
	ret = process_work_completion_events(io_completion_channel, 
			wc, 2);
	if(ret != 2) {
		#ifdef rdma_error
		rdma_error("We failed to get 2 work completions , ret = %d \n",
				ret);
		#endif
		return ret;
	}
	// debug("Server sent us its buffer location and credentials, showing \n");
	show_rdma_buffer_attr(&client_metadata_attr);
	return 0;
}

/* This function does :
 * 1) Prepare memory buffers for RDMA operations 
 * 1) RDMA write from src -> remote buffer 
 * 2) RDMA read from remote bufer -> dst
 */ 
static int client_remote_memory_ops() 
{
	struct ibv_wc wc;
	int ret = -1;
	client_dst_mr = rdma_buffer_register(pd,
			dst,
			strlen(src),
			(ibv_access_flags)(IBV_ACCESS_LOCAL_WRITE | 
			 IBV_ACCESS_REMOTE_WRITE | 
			 IBV_ACCESS_REMOTE_READ));
	if (!client_dst_mr) {
		#ifdef rdma_error
		rdma_error("We failed to create the destination buffer, -ENOMEM\n");
		#endif
		return -ENOMEM;
	}
	/* Step 1: is to copy the local buffer into the remote buffer. We will 
	 * reuse the previous variables. */
	/* now we fill up SGE */
	client_send_sge.addr = (uint64_t) client_src_mr->addr;
	client_send_sge.length = (uint32_t) client_src_mr->length;
	client_send_sge.lkey = client_src_mr->lkey;
	/* now we link to the send work request */
	bzero(&client_send_wr, sizeof(client_send_wr));
	client_send_wr.sg_list = &client_send_sge;
	client_send_wr.num_sge = 1;
	client_send_wr.opcode = IBV_WR_RDMA_WRITE;
	client_send_wr.send_flags = IBV_SEND_SIGNALED;
	/* we have to tell server side info for RDMA */
	client_send_wr.wr.rdma.rkey = server_metadata_attr.stag.remote_stag;
	client_send_wr.wr.rdma.remote_addr = server_metadata_attr.address;
	/* Now we post it */
	ret = ibv_post_send(client_qp, 
		       &client_send_wr,
	       &bad_client_send_wr);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to write client src buffer, errno: %d \n", 
				-errno);
		#endif
		return -errno;
	}
	/* at this point we are expecting 1 work completion for the write */
	ret = process_work_completion_events(io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
		#ifdef rdma_error
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		#endif
		return ret;
	}
	printf("Client side WRITE is complete \n");
	/* Now we prepare a READ using same variables but for destination */
	client_send_sge.addr = (uint64_t) client_dst_mr->addr;
	client_send_sge.length = (uint32_t) client_dst_mr->length;
	client_send_sge.lkey = client_dst_mr->lkey;
	/* now we link to the send work request */
	bzero(&client_send_wr, sizeof(client_send_wr));
	client_send_wr.sg_list = &client_send_sge;
	client_send_wr.num_sge = 1;
	client_send_wr.opcode = IBV_WR_RDMA_READ;
	client_send_wr.send_flags = IBV_SEND_SIGNALED;
	/* we have to tell server side info for RDMA */
	client_send_wr.wr.rdma.rkey = server_metadata_attr.stag.remote_stag;
	client_send_wr.wr.rdma.remote_addr = server_metadata_attr.address;
	/* Now we post it */
	ret = ibv_post_send(client_qp, 
		       &client_send_wr,
	       &bad_client_send_wr);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to read client dst buffer from the master, errno: %d \n", 
				-errno);
		#endif
		return -errno;
	}
	/* at this point we are expecting 1 work completion for the write */
	ret = process_work_completion_events(io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
		#ifdef rdma_error
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		#endif
		return ret;
	}
	// debug("Client side READ is complete \n");
	printf("dst is %s\n", dst);
	return 0;
}

/* This function disconnects the RDMA connection from the server and cleans up 
 * all the resources.
 */
static int client_disconnect_and_clean()
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	/* active disconnect from the client side */
	ret = rdma_disconnect(cm_client_id);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to disconnect, errno: %d \n", -errno);
		#endif
		//continuing anyways
	}
	ret = process_rdma_cm_event(cm_event_channel, 
			RDMA_CM_EVENT_DISCONNECTED,
			&cm_event);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to get RDMA_CM_EVENT_DISCONNECTED event, ret = %d\n",
				ret);
		#endif
		//continuing anyways 
	}
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to acknowledge cm event, errno: %d\n", 
			       -errno);
		#endif
		//continuing anyways
	}
	/* Destroy QP */
	rdma_destroy_qp(cm_client_id);
	/* Destroy client cm id */
	ret = rdma_destroy_id(cm_client_id);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
		#endif
		// we continue anyways;
	}
	/* Destroy CQ */
	ret = ibv_destroy_cq(client_cq);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to destroy completion queue cleanly, %d \n", -errno);
		#endif
		// we continue anyways;
	}
	/* Destroy completion channel */
	ret = ibv_destroy_comp_channel(io_completion_channel);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to destroy completion channel cleanly, %d \n", -errno);
		#endif
		// we continue anyways;
	}
	/* Destroy memory buffers */
	rdma_buffer_deregister(server_metadata_mr);
	rdma_buffer_deregister(client_metadata_mr);	
	rdma_buffer_deregister(client_src_mr);	
	rdma_buffer_deregister(client_dst_mr);	
	/* We free the buffers */
	free(src);
	free(dst);
	/* Destroy protection domain */
	ret = ibv_dealloc_pd(pd);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);
		#endif
		// we continue anyways;
	}
	rdma_destroy_event_channel(cm_event_channel);
	printf("Client resource clean up is complete \n");
	return 0;
}

static int client_remote_memory_write(char* content) 
{
	strncpy(src, content, strlen(content));
	struct ibv_wc wc;
	int ret = -1;
	client_dst_mr = rdma_buffer_register(pd,
			dst,
			strlen(src),
			(ibv_access_flags)(IBV_ACCESS_LOCAL_WRITE | 
			 IBV_ACCESS_REMOTE_WRITE | 
			 IBV_ACCESS_REMOTE_READ));
	if (!client_dst_mr) {
		#ifdef rdma_error
		rdma_error("We failed to create the destination buffer, -ENOMEM\n");
		#endif
		return -ENOMEM;
	}
	client_send_sge.addr = (uint64_t) client_src_mr->addr;
	client_send_sge.length = (uint32_t) client_src_mr->length;
	client_send_sge.lkey = client_src_mr->lkey;
	bzero(&client_send_wr, sizeof(client_send_wr));
	client_send_wr.sg_list = &client_send_sge;
	client_send_wr.num_sge = 1;
	client_send_wr.opcode = IBV_WR_RDMA_WRITE;
	client_send_wr.send_flags = IBV_SEND_SIGNALED;
	client_send_wr.wr.rdma.rkey = server_metadata_attr.stag.remote_stag;
	client_send_wr.wr.rdma.remote_addr = server_metadata_attr.address;
	ret = ibv_post_send(client_qp, 
		       &client_send_wr,
	       &bad_client_send_wr);
	if (ret) {
		#ifdef rdma_error
		rdma_error("Failed to write client src buffer, errno: %d \n", 
				-errno);
		#endif
		return -errno;
	}
	ret = process_work_completion_events(io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
		#ifdef rdma_error
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		#endif
		return ret;
	}
	printf("Client side WRITE is complete \n");
	return 0;
}

void rdmaClient(int nodeID) {
	struct sockaddr_in server_sockaddr;
	int ret, option;
	bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET;
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	/* buffers are NULL */
	src = dst = NULL; 
	src = (char*)calloc(10, 1);
	dst = (char*)calloc(10, 1);
	src[0] = '0';
	std::unordered_map<std::string, std::string> LimitConfig = readConfig("port.conf");
	uint16_t rdmaport=std::stoi(LimitConfig["rdmaport"]);
	server_sockaddr.sin_port = htons(rdmaport + nodeID);
	
	auto ip = LimitConfig["ip"];
	server_sockaddr.sin_addr.s_addr = inet_addr(ip.c_str());
	ret = client_prepare_connection(&server_sockaddr);
		if (ret) { 
			#ifdef rdma_error
			rdma_error("Failed to setup client connection , ret = %d \n", ret);
			#endif
			return;
		}
		ret = client_pre_post_recv_buffer(); 
		if (ret) { 
			#ifdef rdma_error
			rdma_error("Failed to setup client connection , ret = %d \n", ret);
			#endif
			return;
		}
		ret = client_connect_to_server();
		if (ret) { 
			#ifdef rdma_error
			rdma_error("Failed to setup client connection , ret = %d \n", ret);
			#endif
			return;
		}
		ret = client_xchange_metadata_with_server();
		if (ret) {
			#ifdef rdma_error
			rdma_error("Failed to setup client connection , ret = %d \n", ret);
			#endif
			return;
		}
	while (1) {
		// unsigned long long curTime = std::chrono::steady_clock::now().time_since_epoch().count();
		auto used = std::chrono::system_clock::now().time_since_epoch().count() / 1000000000.0;
		if (rowLimit.load() <= 0) {
			client_remote_memory_write("0");
			// std::cout << "Remote write full. Current time: " << used << std::endl;
		} else {
			// std::cout << "Remote write non-full. Current time: " << used << std::endl;
			client_remote_memory_write("1");
		} 
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
}

FileScanPOp::FileScanPOp(std::string name,
           std::vector<std::string> projectColumnNames,
           int nodeId,
       const std::string& filePath,
           std::shared_ptr<Table> table,
		   std::unordered_map<std::string, std::vector<int>> linesToRead,
       bool scanOnStart) :
 PhysicalOp(std::move(name), FILE_SCAN, std::move(projectColumnNames), nodeId),
 scanOnStart_(scanOnStart),
  filePath_(filePath),
  table_(std::move(table)),
  linesToRead_(linesToRead){}

std::string FileScanPOp::getTypeString() const {
  return "FileScanPOp";
}

void FileScanPOp::onReceive(const Envelope &message) {
  std::cout<<name_<<" PhysicalOp thread id is "<<std::this_thread::get_id()<<std::endl;
  if (message.message().type() == MessageType::START) {
   this->onStart();
  } else if (message.message().type() == MessageType::SCAN) {
    auto scanMessage = dynamic_cast<const ScanMessage &>(message.message());
    this->onCacheLoadResponse(scanMessage);
  } else if (message.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else if (message.message().type() == MessageType::DEBUG) {
	this->onDebug();
  } else if (message.message().type() == MessageType::PREDICATE) {
	auto predicateMessage = dynamic_cast<const PredicateMessage &>(message.message());
	this->onPredicate(predicateMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + message.message().getTypeString());
  }
}

void FileScanPOp::onPredicate(const PredicateMessage &message) {
	recvFromPred = true;
	std::cout << name_ + " receive Predicate Msg from " + message.sender() + ", location: " + std::to_string(message.getLocation()) << std::endl;
	nextOpLocation_ = message.getLocation();
	sendSavedTuple();
	if (recvAllCompleteMsg() && readAllFile() && readAllSavedTuple()) {
		ctx()->notifyDebug();
#if OP_INFO>0
		if(op_stop_time_==0){
			op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
		}
#endif
		ctx()->notifyComplete();appendOpTime();
	}
}

void FileScanPOp::onComplete(const CompleteMessage &) {
	std::cout<<"FileScan total drop bytes= "<<drop_bytes<<std::endl;
  if (recvAllCompleteMsg() && readAllFile() && readAllSavedTuple()) {
		ctx()->notifyDebug();
#if OP_INFO>0
		if(op_stop_time_==0){
			op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
		}
#endif
      ctx()->notifyComplete();appendOpTime();
  }
}

void FileScanPOp::onStart() {
#if OP_INFO>0
	if(op_start_time_==0){
		op_start_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
	}
#endif
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
  if(scanOnStart_){
    if (rowLimitInit == false) {
      init_mtx.lock();
      if(rowLimitInit == false) {
        std::unordered_map<std::string, std::string> LimitConfig = readConfig("rowLimit.conf");
        std::string p = LimitConfig["ROWLIMIT"];
        long long block_size = std::stoll(p);
        std::atomic_init(&rowLimit,block_size);
        std::thread th(rdmaClient, nodeId_);
        th.detach();
        rowLimitInit = true;
        // std::cout << "in FileScanPOp::onStart() init rowLimit,check this only excute once,rowLimit= " << rowLimit << std::endl;
      }
      init_mtx.unlock();
    }
    if (!shouldSpawnOnRemote_ || rowLimit.load() > 0) {
    //   std::cout << name() << " in FileScanPOp::onStart() rowLimit = " << rowLimit << ",execute filescan" << std::endl;
      readAndSendTuples(getProjectColumnNames());
	//   if (recvAllCompleteMsg() ) {
    //     std::cout << name() << " recvAllCompleteMsg() " << std::endl;
	//   }
	//   if (readAllFile()) {
    //     std::cout << name() << "readAllFile() " << std::endl;
	//   }
	//   if (readAllSavedTuple()) {
    //     std::cout << name() << " readAllSavedTuple() " << std::endl;
	//   }
	  if (recvAllCompleteMsg() && readAllFile() && readAllSavedTuple()) {
		ctx()->notifyDebug();
#if OP_INFO>0
		if(op_stop_time_==0){
			op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
		}
#endif
        ctx()->notifyComplete();appendOpTime();
	  }
    } else {
    //   std::cout << name() << " in FileScanPOp::onStart() rowLimit = " << rowLimit << ",sleep" << std::endl;
      rowLimit_mtx.lock();
      vec.push_back(this->ctx()->operatorActor());
      rowLimit_mtx.unlock();
    }
    // std::cout<<"FileScanPOp  "<<this->name()<<" execute finish,release"<<std::endl;
    // ParallelFileScanOps-=1;
    // std::cout<<"ParallelFileScanOps now: "<<ParallelFileScanOps<<std::endl;
  }
}

void FileScanPOp::onDebug() {
	if (rowLimit.load() > 0) {
		readAndSendTuples(getProjectColumnNames());
		if (recvAllCompleteMsg() && readAllFile() && readAllSavedTuple()) {
			ctx()->notifyDebug();
#if OP_INFO>0
			if(op_stop_time_==0){
			op_stop_time_=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
		}
#endif
          ctx()->notifyComplete();appendOpTime();
		}
	} else {
		rowLimit_mtx.lock();
		vec.push_back(this->ctx()->operatorActor());
		rowLimit_mtx.unlock();
	}
}

int FileScanPOp::fsend2model(std::string opName, uint64_t inputSize, int location, uint64_t inputRow, 
							uint64_t sampleTime, double percent) {
  std::cout<<"FileScanPOp::send2model"<<std::endl;
  auto start = std::chrono::steady_clock::now();
  std::unordered_map<std::string, std::string> LimitConfig = readConfig("port.conf");
  auto ip = LimitConfig["ip"];
  std::shared_ptr<HttpGetPostMethod> http_request = std::make_shared<HttpGetPostMethod>();
  std::string content = "op=" + opName + "&inputSize=" + std::to_string(inputSize) + "&location=" + 
                        std::to_string(location) + "&inputRow=" + std::to_string(inputRow) + "&sampletime=" +
						std::to_string(sampleTime) + "&samplePercent=" + std::to_string(percent);
  std::cout<<"FileScanPOp::send2model content: "<<content<<std::endl;
  int ret = http_request->HttpGet(ip, "/OpDecision/get/", content);
  if(ret == -1) {
      std::cout << name_ + " Http Socket error!" << std::endl;
      return -1;
  }
  if(http_request->get_return_status_code() != 200) {
      std::cout << name_ + " Http get status code was: " << http_request->get_return_status_code() << std::endl;
      return -1;
  }
  std::string request_return = http_request->get_request_return();
  std::string main_text = http_request->get_main_text();

  auto end = chrono::steady_clock::now();
  auto used = chrono::duration_cast<chrono::nanoseconds>(end - start).count() / 1000000000.0;
  std::cout << name() + " receive model location: " + main_text + ", time used: " + std::to_string(used) + " secs" << std::endl;

  return std::stoi(main_text);
}

void FileScanPOp::readAndSendTuples(const std::vector<std::string> &columnNames){
  // Read the columns not present in the cache
  /*
   * FIXME: Should support reading the file in pieces
   */

  std::shared_ptr<TupleSet> readTupleSet;
  if (columnNames.empty()) {
    readTupleSet = TupleSet::makeWithEmptyTable();
  } else {
    readTupleSet = readTuples();
    SPDLOG_DEBUG("{} -> {} rows", name(), readTupleSet->numRows());
  }
#if RUNTIME_SAMPLE_EN > 0
uint64_t numRows = readTupleSet->numRows();
  uint64_t size = readTupleSet->size();
  std::string target = "";
  for (auto consumer : consumers_) {
	if (consumer.find("[0]") != std::string::npos) {
	target = consumer;
	break;
	}
  }
  if (link2Pred&&!recvFromPred) {
	std::cout<<"link 2 Pred"<<std::endl;
	if (!recvFromPred && !sendToPred && numRows > 0&&iflock()) {
		auto msg = std::make_shared<PredicateMessage>(name(), target, nodeId_, numRows*consumerFactor_, size*consumerFactor_,consumerFactor_);
		ctx()->producer2predictor(msg);
		sendToPred = true;
	}
	if(!sendSampleTuple){
		sendSampleTuple=true;
		std::unordered_map<std::string, std::string> pathConfig = readConfig("filescan.conf");
		uint16_t SAMPLE=std::stoi(pathConfig["SAMPLE"]);
		uint16_t PROPORTION=std::stoi(pathConfig["PROPORTION"]);
		double sample_ratio = (1.0*PROPORTION)/100;
		uint64_t splitRows = (uint64_t)(sample_ratio * numRows);
		SampleDataResult_=readTupleSet->slice(0, splitRows);
		readTupleSet=readTupleSet->slice(splitRows,numRows-splitRows);
		// send sample tuple to -1 & 0
		auto nodeSet=getNodeSet();

		for (const int& element : nodeSet) {
			auto tmpTuple=SampleDataResult_->slice(0, SampleDataResult_->numRows());
			std::cout<<name()<<" copy a new tuple"<<std::endl;
			std::shared_ptr<Message> sampleTupleMessage = std::make_shared<TupleMessage>(tmpTuple, this->name());
			ctx()->tell(sampleTupleMessage, element);
			// std::this_thread::sleep_for(std::chrono::milliseconds(2000));
		}
	}

  } 
  if (link2Pred && !recvFromPred) {
	saveTuple(readTupleSet);
  } else {
	std::shared_ptr<Message> tupleMessage = std::make_shared<TupleMessage>(readTupleSet, this->name());
	ctx()->tell(tupleMessage, nextOpLocation_);
  }
#elif PREDICATE_EN > 0
  uint64_t numRows = readTupleSet->numRows();
  uint64_t size = readTupleSet->size();
  std::string target = "";
  for (auto consumer : consumers_) {
	if (consumer.find("[0]") != std::string::npos) {
	target = consumer;
	break;
	}
  }
  if (link2Pred) {
	std::cout<<"link 2 Pred"<<std::endl;
	if (!recvFromPred && !sendToPred && numRows > 0) {
		auto msg = std::make_shared<PredicateMessage>(name(), target, nodeId_, numRows*consumerFactor_, size*consumerFactor_,consumerFactor_);
		ctx()->producer2predictor(msg);
		sendToPred = true;
	}
  } else {
	std::cout<<"Link to filter"<<std::endl;
	// //Link to filter
	// std::unordered_map<std::string, std::string> config = readConfig("percent.conf");
	// std::string p = config["PERCENT"];
	// int percent = std::stoi(p);
	// uint64_t splitRows = (uint64_t)(percent * 1.0 / 100 * numRows);
	// uint64_t sampleTime = sampleFilter(readTupleSet->slice(0, splitRows));
	int percent=10;
	uint64_t sampleTime=1;
	std::cout<<"send2model begin"<<std::endl;
	nextOpLocation_ = fsend2model(target, size, nodeId_, numRows, sampleTime, percent / 100);
	std::cout<<"send2model finish"<<std::endl;
  }
  if (link2Pred && !recvFromPred) {
	saveTuple(readTupleSet);
  } else {
	std::shared_ptr<Message> tupleMessage = std::make_shared<TupleMessage>(readTupleSet, this->name());
	ctx()->tell(tupleMessage, nextOpLocation_);
  }
#else
	std::shared_ptr<Message> tupleMessage = std::make_shared<TupleMessage>(readTupleSet, this->name());
	ctx()->tell(tupleMessage, nextOpLocation_);
#endif
  
  
}

std::shared_ptr<TupleSet> FileScanPOp::readTuples() {
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
	uint64_t stTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif
	auto inFile = std::filesystem::absolute(this->filePath_);//if it's a relative path, please replace absolute with relative
	auto expectedInFile = arrow::io::ReadableFile::Open(inFile, arrow::default_memory_pool());
	auto inputStream = *expectedInFile;
#if RANDOM_FILESCAN>0
	auto tupleSet = readCSVFile_randomAcross();
#else
	auto tupleSet = readCSVFile(inputStream);
#endif
#if OP_INFO>0
	std::cout << tupleSet->numRows() << std::endl;
	input_row+=tupleSet->numRows();
	if(input_row_first==0){
		input_row_first=tupleSet->numRows();
	}
	input_size+=tupleSet->size();
#endif
	inputStream->Close();
	tupleSet->renameColumns(this->projectColumnNames_);
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
	uint64_t enTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	exectime_+=enTime-stTime;
#endif
	return tupleSet;
}

void FileScanPOp::onCacheLoadResponse(const ScanMessage &Message) {
  readAndSendTuples(Message.getColumnNames());
}

bool FileScanPOp::isScanOnStart() const {
  return scanOnStart_;
}

void FileScanPOp::clear() {
  // Noop
}

void FileScanPOp::setPredicate(std::shared_ptr<fpdb::expression::gandiva::Expression> predicate) {
	predicate_ = predicate;
}

uint64_t FileScanPOp::sampleFilter(std::shared_ptr<TupleSet> sampleSet) {
	auto filter = Filter::make(predicate_);
	filter->compile(Schema::make(sampleSet->schema()));

	std::chrono::steady_clock::time_point startTime = std::chrono::steady_clock::now();
	filter->evaluate(*sampleSet);
	std::chrono::steady_clock::time_point stopTime = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();
}
std::shared_ptr<arrow::Table> FileScanPOp::strings_to_table(const std::vector<std::string>& strings) {
  if (strings.empty()) {
    return nullptr;
  }

  // Combine the strings into a single CSV string
  std::stringstream ss;
  for (const auto& line : strings) {
    ss << line << "\n";
  }

  // Create a Buffer from the combined string
  std::string combined = ss.str();
//   std::string::size_type pos = 0;
//     while ((pos = combined.find('\n', pos)) != std::string::npos) {
//         combined.replace(pos, 1, "@");
//         pos += 1; // 移动到下一个位置
//     }
//   std::cout<<combined<<std::endl;
  auto buffer = std::make_shared<arrow::Buffer>(combined);

  // Create a BufferReader from the Buffer
  auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);

  // Define CSV read options
  auto read_options = arrow::csv::ReadOptions::Defaults();
  read_options.use_threads = false; // Disable threading for simplicity
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();
  std::unordered_map<std::string, std::shared_ptr<::arrow::DataType>> columnTypes;
  std::vector<std::string> temp = getProjectColumnNames();
  for (int i=0; i < temp.size(); i++) {
      temp[i] = mytoupper(temp[i]);
  }
  convert_options.include_columns = temp;
  parse_options.delimiter = '|';
  for(const auto &columnName: this->projectColumnNames_) {
      columnTypes.emplace(mytoupper(columnName), table_->getSchema()->GetFieldByName(columnName)->type());
  }
  convert_options.column_types = columnTypes;


  // Create a TableReader from the BufferReader
  auto maybe_reader = arrow::csv::TableReader::Make(arrow::io::default_io_context(), buffer_reader, read_options, parse_options, convert_options);
  if (!maybe_reader.ok()) {
    throw std::runtime_error("Error creating TableReader: " + maybe_reader.status().message());
  }

  // Read the table
  std::shared_ptr<arrow::Table> table;
  auto status = maybe_reader.ValueOrDie()->Read();
  if (!status.ok()) {
    throw std::runtime_error("Error reading table: " + status.status().message());
  } 
  table=status.ValueOrDie();

  return table;
}
std::string FileScanPOp::readWithOffsets(const std::string& filePath, const std::vector<int>& offsets, int len) {
	std::ifstream file(filePath);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file");
    }
	std::string header;
    std::getline(file, header);
	uint64_t header_size=header.size();

    std::string result;
	file.seekg(0, std::ios::end);
    std::streampos fileSize = file.tellg();
    for (size_t i = 0; i < offsets.size(); ++i) {
        // std::cout<<" it "<<i<<std::endl;
        uint64_t A = offsets[i]+header_size;
        file.seekg(A);
        if (file.fail()) {
            std::cout<<"A, fileSize "<<A<<" "<<fileSize<<std::endl;
            throw std::runtime_error("Error seeking to offset A");
        }
        bool isLineStart = true;
        if (A > 0) {
            file.seekg(A - 1);
            if (file.get() != '\n') {
                file.seekg(A);
                std::string temp;
                std::getline(file, temp);
                drop_bytes+=temp.size();
                A = file.tellg(); 
                isLineStart = false;
            } else {
                file.seekg(A);
            }
        }
        if (!isLineStart) {
            A = file.tellg();
        }

        // 从当前偏移量读取 len 个字符
        std::streampos numCharsToRead = len;
        if (!isLineStart) {
            numCharsToRead -= (A - (offsets[i]+header_size)); // 调整读取长度
        }
        if(numCharsToRead>0){
            numCharsToRead = static_cast<std::streampos>(std::min(static_cast<uint64_t>(numCharsToRead), static_cast<uint64_t>(fileSize)-A));
            std::string temp(numCharsToRead, ' ');
            file.read(&temp[0], numCharsToRead);
            if (temp[temp.size()-1]!='\n'&&file.peek() != '\n' && file.peek() != EOF) {
                // 读取 b 所在行的剩余部分
                std::string line;
                std::getline(file, line);
                temp += line;
            }
            if(temp[temp.size()-1]=='\n'){
                result += temp;
            }
            else{
                result += temp + "\n";
            }   
            
        }
        
    }
    std::string re=header+"\n"+result;
    if (!re.empty() && re.back() == '\n') {
        re.pop_back();
    }
    return re;
}

}