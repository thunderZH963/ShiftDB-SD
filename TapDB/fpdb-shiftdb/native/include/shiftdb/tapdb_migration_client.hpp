#pragma once
#include <caf/all.hpp>
#include <chrono>
#include <string>
namespace shiftdb::native {
struct MigrationResult {
 bool active=false;
 // On failure the source may already have committed. Keep both actors alive
 // and retry the SAME ticket/target; never infer abort from an RPC timeout.
 std::string stage, error;
};
// Blocking control-plane helper. Target must be a separately constructed,
// inactive POpActor on the desired CAF node, with the same canonical plan.
// Caller owns resource reservation and serializes all migration attempts.
MigrationResult migrate_active(caf::scoped_actor& controller,
 const caf::actor& source,const caf::actor& target,const std::string& ticket,
 const std::string& canonical_plan_fingerprint,
 std::chrono::milliseconds timeout=std::chrono::seconds(30));
}
