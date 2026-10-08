//
// Created by matt on 30/9/20.
//

#include <fpdb/executor/message/ConnectMessage.h>

namespace fpdb::executor::message {

ConnectMessage::ConnectMessage(std::vector<POpConnection> connections, std::string from,
								std::unordered_map<std::string, int> spawnAddr,
								std::unordered_map<std::string, int> activateAddr,
								unsigned long long startTime) :
	Message(CONNECT, std::move(from)),
	connections_(std::move(connections)) {
	for (auto &it : spawnAddr) {
		spawnAddr_.emplace(it.first, it.second);
	}
	for (auto &it : activateAddr) {
		activeAddr_.emplace(it.first, it.second);
	}
	startTime_ = startTime;
}

std::string ConnectMessage::getTypeString() const {
  return "ConnectMessage";
}

const std::vector<POpConnection> &ConnectMessage::connections() const {
  return connections_;
}

const std::unordered_map<std::string, int> &ConnectMessage::spawnAddr() const {
	return spawnAddr_;
}

const std::unordered_map<std::string, int> &ConnectMessage::activeAddr() const {
	return activeAddr_;
}

const unsigned long long ConnectMessage::startTime() const {
	return startTime_;
}

}