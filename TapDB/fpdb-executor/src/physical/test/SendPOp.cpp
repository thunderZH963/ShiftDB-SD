//
// Created by Czk on 08/03/22.
//

#include <fpdb/executor/physical/test/SendPOp.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/message/DebugMessage.h>
#include <fpdb/expression/gandiva/BinaryExpression.h>
#include <fpdb/tuple/Globals.h>
#include <utility>

using namespace fpdb::executor::physical::test;
using namespace fpdb::cache;
using namespace fpdb::expression::gandiva;

SendPOp::SendPOp(std::string name,
               std::vector<std::string> projectColumnNames,
               int nodeId) :
	PhysicalOp(std::move(name), SEND, std::move(projectColumnNames), nodeId) {}

std::string SendPOp::getTypeString() const {
  return "SendPOp";
}

void SendPOp::onReceive(const Envelope &Envelope) {
  const auto& message = Envelope.message();

  if (message.type() == MessageType::START) {
	  this->onStart();
  } else {
    ctx()->notifyError("Unrecognized message type " + message.getTypeString());
  }
}

void SendPOp::onStart() {
    SPDLOG_DEBUG("Starting '{}'", name());
    rdma_socket server(RDMA_PS_TCP);

    server.bind("192.168.1.5", 8000);
    server.listen();
    rdma_socket rclient = server.accept();
    char* data = new char[4096];
    sprintf(data, "%d", rowLimit.load());
    rclient.post_send(data, 4096);
    struct ibv_wc wc = rclient.send_comp();
}

void SendPOp::clear() {
    //Noop
}
