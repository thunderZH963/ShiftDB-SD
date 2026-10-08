// Full TapDB dependency environment required; no mock operators in this test.
#include "shiftdb/tapdb_operator_state.hpp"
#include <fpdb/executor/physical/aggregate/function/Sum.h>
#include <fpdb/executor/physical/group/GroupKernel.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinBuildKernel.h>
#include <fpdb/expression/gandiva/Column.h>
#include <iostream>
#include <map>
#include <stdexcept>
using namespace fpdb::executor::physical;
using namespace fpdb::tuple;
using namespace shiftdb::native;
#define CHECK(x)                                                               \
  do {                                                                         \
    if (!(x))                                                                  \
      throw std::runtime_error(#x);                                            \
  } while (false)
auto tuples() {
  arrow::Int64Builder keys, values;
  CHECK(keys.AppendValues({1, 1, 2}).ok());
  CHECK(values.AppendValues({10, 20, 30}).ok());
  return TupleSet::make(arrow::Table::Make(
      arrow::schema({arrow::field("k", arrow::int64()),
                     arrow::field("v", arrow::int64())}),
      {keys.Finish().ValueOrDie(), values.Finish().ValueOrDie()}));
}
auto group_kernel() {
  return group::GroupKernel({"k"},
                            {std::make_shared<aggregate::Sum>(
                                "sum", fpdb::expression::gandiva::col("v"))});
}
auto totals(group::GroupKernel &kernel) {
  auto result = kernel.finalise();
  CHECK(result);
  std::map<int64_t, int64_t> out;
  auto table = result.value()->table();
  for (int64_t row = 0; row < table->num_rows(); ++row) {
    auto k = std::static_pointer_cast<arrow::Int64Scalar>(
                 table->column(0)->GetScalar(row).ValueOrDie())
                 ->value;
    auto v = std::static_pointer_cast<arrow::Int64Scalar>(
                 table->column(1)->GetScalar(row).ValueOrDie())
                 ->value;
    out[k] = v;
  }
  return out;
}
int main() {
  try {
    StateImage binding;
    binding.query_operator = "q/group";
    binding.plan_fingerprint = "test-plan";
    binding.generation = 1;
    auto input = tuples();
    auto source = group_kernel(), target = group_kernel();
    CHECK(source.group(*input));
    auto snapshot = TapDBStateCodec::snapshot(source, binding);
    TapDBStateCodec::restore(target, snapshot, binding);
    // Capture did not flush or consume source input. Both can continue
    // independently.
    CHECK(source.group(*input));
    CHECK(target.group(*input));
    auto expected = std::map<int64_t, int64_t>{{1, 60}, {2, 60}};
    CHECK(totals(source) == expected);
    CHECK(totals(target) == expected);
    join::HashJoinBuildKernel build({"k"}), restored({"k"});
    CHECK(build.put(input));
    auto bytes = TapDBStateCodec::snapshot(build, binding);
    TapDBStateCodec::restore(restored, bytes, binding);
    CHECK(restored.getTupleSetIndex().value()->size() == 3);
    CHECK(restored.put(input));
    CHECK(restored.getTupleSetIndex().value()->size() == 6);
    CHECK(build.getTupleSetIndex().value()->size() == 3);
    CHECK(restored.getTupleSetIndex().value()->validate());
    std::cout << "Native group continuation and duplicate-preserving build "
                 "restore passed\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
