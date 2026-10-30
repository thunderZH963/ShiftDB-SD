//
// Created by Matt Youill on 31/12/19.
//

#include <fpdb/executor/physical/POpActor.h>
#ifdef FPDB_ENABLE_SHIFTDB
#include <shiftdb/tapdb_actor_migration.hpp>
#endif
#include <fpdb/executor/message/Envelope.h>
#include <fpdb/executor/message/ConnectMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/DebugMessage.h>
#include <fpdb/executor/message/TupleMessage.h>
#include <fpdb/executor/message/TupleSetIndexMessage.h>
#include <spdlog/spdlog.h>
#include <utility>

#include <fpdb/util/Util.h>

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
#include <arpa/inet.h> 
#include <set>
#include <thread>
using namespace std;

namespace fpdb::executor::physical {

POpActor::POpActor(::caf::actor_config &cfg, std::shared_ptr<PhysicalOp> opBehaviour) :
	::caf::event_based_actor(cfg),
	opBehaviour_(std::move(opBehaviour)) {

  name_ = opBehaviour_->name();
#ifdef FPDB_ENABLE_SHIFTDB
  migration_=std::make_shared<shiftdb::native::TapDBActorMigration>(*this);
#endif
}

::caf::behavior behaviour(POpActor *self) {

  auto ctx = self->operator_()->ctx();
  ctx->operatorActor(self);

  return {
#ifdef FPDB_ENABLE_SHIFTDB
    [=](const std::string& command,const std::string& ticket,const std::string& fingerprint) -> ::caf::result<::caf::message> {
      try {
        if(command!="shiftdb.freeze.v1")return ::caf::make_error(::caf::sec::invalid_argument,"unknown migration command");
        return self->migration_->freeze(ticket,fingerprint);
      }catch(const std::exception& e){return ::caf::make_error(::caf::sec::runtime_error,e.what());}
    },
    [=](const std::string& command,const std::string& ticket,const std::string& fingerprint,
        const std::vector<uint8_t>& image,const POpContext& context,
        const std::map<std::string,int>& received,int64_t count) -> ::caf::result<bool> {
      try {
        if(command!="shiftdb.install.v1")return ::caf::make_error(::caf::sec::invalid_argument,"unknown migration command");
        return self->migration_->install(ticket,fingerprint,image,std::make_shared<POpContext>(context),received,count);
      }catch(const std::exception& e){return ::caf::make_error(::caf::sec::runtime_error,e.what());}
    },
    [=](const std::string& command,const std::string& ticket,const ::caf::actor& target) -> ::caf::result<bool> {
      try {
        if(command!="shiftdb.commit.v1")return ::caf::make_error(::caf::sec::invalid_argument,"unknown migration command");
        return self->migration_->commit(ticket,target);
      }catch(const std::exception& e){return ::caf::make_error(::caf::sec::runtime_error,e.what());}
    },
    [=](const std::string& command,const std::string& ticket) -> ::caf::result<std::string> {
      try {
        if(command=="shiftdb.status.v1")return self->migration_->status(ticket);
        if(command=="shiftdb.activate.v1"){self->migration_->activate(ticket);return std::string("ACTIVE");}
        if(command=="shiftdb.abort.v1"){self->migration_->abort(ticket);return std::string("ABORTED");}
        return ::caf::make_error(::caf::sec::invalid_argument,"unknown migration command");
      }catch(const std::exception& e){return ::caf::make_error(::caf::sec::runtime_error,e.what());}
    },
#endif
	  [=](GetProcessingTimeAtom) {
		auto start = std::chrono::steady_clock::now();
		auto finish = std::chrono::steady_clock::now();
		auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
		self->incrementProcessingTime(elapsedTime);
		return self->getProcessingTime();
	  },
	  [=](int &num) {
		std::cout << self->operator_()->name() << " receive Debug Message from TupleSet." << std::endl;
		auto sm = make_shared<fpdb::executor::message::DebugMessage>("TupleSet");
		fpdb::executor::message::Envelope e(sm);
#ifdef FPDB_ENABLE_SHIFTDB
        if(self->migration_->intercept(e))return;
#endif
		self->operator_()->onReceive(e);
	  },
	  [=](const fpdb::executor::message::Envelope &msg) {
#ifdef FPDB_ENABLE_SHIFTDB
        if(self->migration_->intercept(msg))return;
#endif

		auto start = std::chrono::steady_clock::now();

		if (msg.message().type() == MessageType::TUPLE) {
			cout << "TUPLE: " + self->operator_()->name() << endl;
		}

		string rec_type;

		switch (msg.message().type()) {
			case MessageType::START : {
				rec_type = "START";
				self->operator_()->receivedTupleSet = 0;
				self->operator_()->receivedTupleSetMap.clear();
				break;
			}
			case MessageType::STOP : {
				rec_type = "STOP";
				break;
			}
			case MessageType::CONNECT : {
				rec_type = "CONNECT";
				break;
			}
			case MessageType::COMPLETE : {
				rec_type = "COMPLETE";
				break;
			}
			case MessageType::SCAN : {
				rec_type = "SCAN";
				break;
			}
			case MessageType::ERROR : {
				rec_type = "ERROR";
				break;
			}
			case MessageType::TUPLE : {
				rec_type = "TUPLE";
				self->operator_()->receivedTupleSet++;
				if(self->operator_()->receivedTupleSetMap.find(msg.message().sender())==self->operator_()->receivedTupleSetMap.end()){
					self->operator_()->receivedTupleSetMap[msg.message().sender()]=1;
				}
				else{
					self->operator_()->receivedTupleSetMap[msg.message().sender()]=self->operator_()->receivedTupleSetMap[msg.message().sender()]+1;
				}
				break;
			}
			case MessageType::DEBUG : {
				rec_type = "DEBUG";
				break;
			}
			case MessageType::PREDICATE : {
				rec_type = "PREDICATE";
				self->operator_()->ifunlock(msg.message().sender());
				break;
			}
			case MessageType::TUPLESET_INDEX : {
				rec_type = "Index";
				break;
			}
			default: {
				rec_type = "Other";
			}
		}
		uint64_t curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
		double elaTime = std::chrono::system_clock::now().time_since_epoch().count() / 1000000000.0;
		string str = "recipient: " + self->operator_()->name() + ", sender: " + msg.message().sender() + ", type: " + rec_type + 
					 ", time: " + std::to_string(elaTime) + " secs";
		cout << str << endl;

		auto senderAddr = (self->operator_()->ctx()->spawnAddr())[msg.message().sender()];
		if (msg.message().type() == MessageType::TUPLE && self->operator_()->getNodeId() != senderAddr) {
			auto size = (dynamic_cast<const TupleMessage &>(msg.message())).tuples()->size();
			std::cout << "Bandwidth: " + std::to_string(1.0 * size / (curTime - msg.message().getSentTime())) + " bytes/ms" << std::endl;
		} else if (msg.message().type() == MessageType::TUPLESET_INDEX && self->operator_()->getNodeId() != senderAddr) {
			auto size = (dynamic_cast<const TupleSetIndexMessage &>(msg.message())).getTupleSetIndex()->getTupleSet()->size();
			std::cout << "Bandwidth: " + std::to_string(1.0 * size / (curTime - msg.message().getSentTime())) + " bytes/ms" << std::endl;
		}

    SPDLOG_DEBUG("Message received  |  recipient: '{}', sender: '{}', type: '{}'",
                 self->operator_()->name(),
                 msg.message().sender(),
                 msg.message().type());

		if (msg.message().type() == MessageType::CONNECT) {
			auto connectMessage = dynamic_cast<const message::ConnectMessage &>(msg.message());

			for (const auto &element: connectMessage.connections()) {
				auto localEntry = LocalPOpDirectoryEntry(element.getName(),
										element.getActorHandle(),
										element.getConnectionType(),
										false);

				auto result = self->operator_()->ctx()->operatorMap().insert(localEntry);
				if (!result.has_value()) {
				self->operator_()->ctx()->notifyError(result.error());
				}
			}

			auto consumers = self->operator_()->consumers();

			for (const auto &it : connectMessage.spawnAddr()) {
				if (consumers.find(it.first) != consumers.end()) {
					self->operator_()->ctx()->spawnAddr().emplace(it.first, it.second);
				}
			}

			for (const auto &it : connectMessage.activeAddr()) {
#if FINE_GRAIN == 0
				self->operator_()->ctx()->activeAddr().emplace(it.first, it.second);
#else
				if (consumers.find(it.first) != consumers.end()) {
					self->operator_()->ctx()->activeAddr().emplace(it.first, it.second);
				}
#endif
			}

			self->operator_()->setTime(connectMessage.startTime());
		}
		else if (msg.message().type() == MessageType::START) {
		  auto startMessage = dynamic_cast<const message::StartMessage &>(msg.message());

		  self->running_ = true;

		  self->operator_()->onReceive(msg);

		  while(!self->buffer_.empty()){
			auto item = self->buffer_.front();
			self->overriddenMessageSender_ = item.second;
			self->call_handler(self->current_behavior(), item.first);
			self->buffer_.pop();
		  }

		  self->overriddenMessageSender_ = std::nullopt;

		} else if (msg.message().type() == MessageType::DEBUG) {
			self->operator_()->onReceive(msg);
		} else if (msg.message().type() == MessageType::STOP) {
		  self->running_ = false;

		  self->operator_()->onReceive(msg);
		}
		else{
		  if(!self->running_){
			// cout << self->operator_()->name() + " is not running!" << endl;
			self->buffer_.emplace(self->current_mailbox_element()->content(), self->current_sender());
		  }
		  else {
			if (msg.message().type() == MessageType::COMPLETE) {
				auto completeMessage = dynamic_cast<const message::CompleteMessage &>(msg.message());
				auto result = self->operator_()->ctx()->operatorMap().setComplete(msg.message().sender());
				if (!result.has_value()) {
				return self->operator_()->ctx()->notifyError(result.error());
				}
			}
			self->operator_()->onReceive(msg);
		  }
		}

		auto finish = std::chrono::steady_clock::now();
		auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
		self->incrementProcessingTime(elapsedTime);
	  }
  };
}

::caf::behavior POpActor::make_behavior() {
  return behaviour(this);
}

std::shared_ptr<fpdb::executor::physical::PhysicalOp> POpActor::operator_() const {
  return opBehaviour_;
}

long POpActor::getProcessingTime() const {
  return processingTime_;
}

void POpActor::incrementProcessingTime(long time) {
  processingTime_ += time;
}

void POpActor::on_exit() {
  SPDLOG_DEBUG("Stopping operator  |  name: '{}'", this->opBehaviour_->name());

  /*
   * Need to delete the actor handle in operator otherwise CAF will never release the actor
   */
  this->opBehaviour_->destroyActor();
}

}
