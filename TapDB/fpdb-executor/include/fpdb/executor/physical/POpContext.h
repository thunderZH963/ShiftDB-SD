//
// Created by matt on 5/12/19.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPCONTEXT_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPCONTEXT_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/POpActor.h>
#include <fpdb/executor/physical/LocalPOpDirectory.h>
#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/physical/Forward.h>
#include <fpdb/caf/CAFUtil.h>
#include <memory>
#include <string>
#include <mutex>
#include <queue>
#include <thread>
#include <atomic>
#include <unordered_map>

namespace fpdb::executor::physical {

extern std::mutex msgLock;
extern std::queue<fpdb::executor::message::Envelope> msgQueue;
extern std::queue<::caf::actor> actorQueue;
extern std::atomic_bool nicFull;
extern std::atomic_bool execFinish;

/**
 * The API physical operators use to interact with their environment, e.g. sending messages
 */
class POpContext {

public:
  POpContext(::caf::actor rootActor, ::caf::actor segmentCacheActor);
  POpContext() = default;
  POpContext(const POpContext&) = default;
  POpContext& operator=(const POpContext&) = default;

  POpActor* operatorActor();
  void operatorActor(POpActor *operatorActor);

  LocalPOpDirectory &operatorMap();
  std::unordered_map<std::string, int> &spawnAddr();
  std::unordered_map<std::string, int> &activeAddr();

  void tell(std::shared_ptr<message::Message> &msg, int location);
  void send(const std::shared_ptr<message::Message> &msg, const std::string &recipientId);
  void notifyComplete();
  void notifyError(const std::string &content);
  void notifyDebug();
  void producer2predictor(const std::shared_ptr<message::Message> &msg);
  void predictor2producer(const std::shared_ptr<message::Message> &msg);
  void consumer2predictor(const std::shared_ptr<message::Message> &msg);
  int opActiveAddr(std::string name);

  void destroyActorHandles();
  [[nodiscard]] bool isComplete() const;

  ::caf::actor getRootActor();

private:
  POpActor* operatorActor_;
  LocalPOpDirectory operatorMap_;
  ::caf::actor rootActor_;
  ::caf::actor segmentCacheActor_;
  bool complete_ = false;
  std::unordered_map<std::string, int> spawnAddr_;
  std::unordered_map<std::string, int> activeAddr_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, POpContext& ctx) {
    return f.object(ctx).fields(f.field("operatorMap", ctx.operatorMap_),
                                f.field("rootActor", ctx.rootActor_),
                                f.field("segmentCacheActor", ctx.segmentCacheActor_),
                                f.field("complete", ctx.complete_)
#ifdef FPDB_ENABLE_SHIFTDB
                                ,
                                f.field("spawnAddr", ctx.spawnAddr_),
                                f.field("activeAddr", ctx.activeAddr_)
#endif
                                );
  }
};

}

using POpContextPtr = std::shared_ptr<fpdb::executor::physical::POpContext>;

CAF_BEGIN_TYPE_ID_BLOCK(POpContext, fpdb::caf::CAFUtil::POpContext_first_custom_type_id)
CAF_ADD_TYPE_ID(POpContext, (fpdb::executor::physical::POpContext))
CAF_END_TYPE_ID_BLOCK(POpContext)

namespace caf {
template <>
struct inspector_access<POpContextPtr> : variant_inspector_access<POpContextPtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPCONTEXT_H
