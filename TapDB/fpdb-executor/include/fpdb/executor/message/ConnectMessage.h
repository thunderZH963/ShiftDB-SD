//
// Created by matt on 30/9/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CONNECTMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CONNECTMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/physical/POpConnection.h>
#include <vector>
#include <unordered_map>
#include <chrono>

using namespace fpdb::executor::physical;

namespace fpdb::executor::message {

/**
 * Message sent to operators to tell them who they are connected to
 */
class ConnectMessage : public Message {

public:
  explicit ConnectMessage(std::vector<POpConnection> operatorConnections, std::string from, 
                          std::unordered_map<std::string, int> spawnAddr, 
                          std::unordered_map<std::string, int> activateAddr,
                          unsigned long long startTime);
  ConnectMessage() = default;
  ConnectMessage(const ConnectMessage&) = default;
  ConnectMessage& operator=(const ConnectMessage&) = default;

  std::string getTypeString() const override;

  const std::vector<POpConnection> &connections() const;
  const std::unordered_map<std::string, int> &spawnAddr() const;
  const std::unordered_map<std::string, int> &activeAddr() const;
  const unsigned long long startTime() const;

private:
  std::vector<POpConnection> connections_;
  std::unordered_map<std::string, int> spawnAddr_;
  std::unordered_map<std::string, int> activeAddr_;
  unsigned long long startTime_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ConnectMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("operatorConnections", msg.connections_),
                                f.field("operatorSpawnAddress", msg.spawnAddr_),
                                f.field("operatorActiveAddress", msg.activeAddr_),
                                f.field("startTime", msg.startTime_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CONNECTMESSAGE_H
