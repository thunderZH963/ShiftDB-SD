#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>
namespace shiftdb {
// Actor-thread serialized state machine. Ticket retries are idempotent; after
// commit the old actor can only forward, never resume execution.
class MigrationGate {
public:
 enum class Phase { Running, Frozen, Prepared, Forwarding, Active, Aborted };
private:
 Phase phase_=Phase::Running;
 std::string ticket_,peer_;
 uint64_t generation_=1;
public:
 Phase phase()const{return phase_;}
 const std::string& ticket()const{return ticket_;}
 uint64_t generation()const{return generation_;}
 bool freeze(const std::string& ticket) {
  if(ticket.empty())throw std::invalid_argument("empty migration ticket");
  if(phase_==Phase::Frozen&&ticket_==ticket)return false;
  if(phase_!=Phase::Running)throw std::logic_error("operator not migratable");
  ticket_=ticket;phase_=Phase::Frozen;return true;
 }
 void undo_failed_freeze(){if(phase_!=Phase::Frozen)throw std::logic_error("not frozen");phase_=Phase::Running;ticket_.clear();}
 bool prepare(const std::string& ticket,uint64_t generation) {
  if(phase_==Phase::Prepared&&ticket_==ticket&&generation_==generation)return false;
  if(phase_!=Phase::Running||ticket.empty()||generation<2)throw std::logic_error("invalid target preparation");
  ticket_=ticket;generation_=generation;phase_=Phase::Prepared;return true;
 }
 bool commit(const std::string& ticket,const std::string& peer) {
  if(phase_==Phase::Forwarding&&ticket_==ticket&&peer_==peer)return false;
  if(phase_!=Phase::Frozen||ticket_!=ticket||peer.empty())throw std::logic_error("invalid ownership commit");
  peer_=peer;phase_=Phase::Forwarding;return true;
 }
 bool activate(const std::string& ticket) {
  if(phase_==Phase::Active&&ticket_==ticket)return false;
  if(phase_!=Phase::Prepared||ticket_!=ticket)throw std::logic_error("target not prepared");
  phase_=Phase::Active;return true;
 }
 void abort_source(const std::string& ticket) {
  if(ticket_!=ticket||phase_!=Phase::Frozen)throw std::logic_error("cannot roll back committed source");
  phase_=Phase::Running;ticket_.clear();
 }
 void abort_target(const std::string& ticket) {
  if(ticket_!=ticket||phase_!=Phase::Prepared)throw std::logic_error("cannot roll back active target");
  phase_=Phase::Aborted;
 }
};
}
