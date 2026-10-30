#pragma once
#include "arrow_state.hpp"
#include "shiftdb/migration_gate.hpp"
#include <caf/all.hpp>
#include <deque>
#include <memory>
namespace fpdb::executor::physical {class POpActor;class POpContext;}
namespace fpdb::executor::message {class Envelope;}
namespace shiftdb::native {
// Stop-copy for pre-output AggregatePOp / GroupPOp. Source is a forwarding
// anchor after commit. Requires a trusted, serialized controller and reliable
// live CAF connections; process-loss recovery is not supplied by this class.
class TapDBActorMigration {
 fpdb::executor::physical::POpActor& actor_;
 MigrationGate gate_;
 caf::actor target_;
 std::string fingerprint_;
 Bytes image_;
 std::shared_ptr<fpdb::executor::physical::POpContext> context_;
 std::map<std::string,int> received_;
 int64_t count_=0;
 std::deque<std::pair<caf::message,caf::strong_actor_ptr>> queued_;
 uint64_t queued_bytes_=0;
 bool replaying_=false;
 void drain(bool forward);
public:
 explicit TapDBActorMigration(fpdb::executor::physical::POpActor& actor):actor_(actor){}
 // Intercept before all normal message handling and counters.
 bool intercept(const fpdb::executor::message::Envelope&);
 caf::message freeze(const std::string& ticket,const std::string& fingerprint);
 bool install(const std::string& ticket,const std::string& fingerprint,const Bytes&,
              const std::shared_ptr<fpdb::executor::physical::POpContext>&,
              const std::map<std::string,int>&,int64_t count);
 bool commit(const std::string& ticket,const caf::actor& target);
 bool activate(const std::string& ticket);
 bool abort(const std::string& ticket);
 std::string status(const std::string& ticket)const;
};
}
