#pragma once
#include "arrow_state.hpp"
namespace fpdb::executor::physical::aggregate {
class AggregateResult;
class AggregatePOp;
} // namespace fpdb::executor::physical::aggregate
namespace fpdb::executor::physical::group {
class GroupKernel;
}
namespace fpdb::executor::physical::join {
class HashJoinBuildKernel;
}
namespace fpdb::executor::physical::sort {
class SortPOp;
}
namespace shiftdb::native {
// Call only on a quiesced operator's actor thread, between handlers. Restore
// into a fresh, non-publishing target constructed from the same canonical plan.
// The controller supplies and independently validates generation and frontiers.
// These methods do not serialize CAF actor identity, node resources or routes.
class TapDBStateCodec {
public:
  static Bytes snapshot(fpdb::executor::physical::aggregate::AggregatePOp &,
                        StateImage binding);
  static StateImage restore(fpdb::executor::physical::aggregate::AggregatePOp &,
                            const Bytes &, const StateImage &expected);
  static Bytes snapshot(fpdb::executor::physical::group::GroupKernel &,
                        StateImage binding);
  static StateImage restore(fpdb::executor::physical::group::GroupKernel &,
                            const Bytes &, const StateImage &expected);
  static Bytes snapshot(fpdb::executor::physical::join::HashJoinBuildKernel &,
                        StateImage binding);
  static StateImage
  restore(fpdb::executor::physical::join::HashJoinBuildKernel &, const Bytes &,
          const StateImage &expected);
  static Bytes snapshot(fpdb::executor::physical::sort::SortPOp &,
                        StateImage binding);
  static StateImage restore(fpdb::executor::physical::sort::SortPOp &,
                            const Bytes &, const StateImage &expected);

private:
  static Bytes
  result(const fpdb::executor::physical::aggregate::AggregateResult &);
};
} // namespace shiftdb::native
