#include "shiftdb/arrow_state.hpp"
#include <arrow/compute/api.h>
#include <iostream>
#include <stdexcept>
using namespace shiftdb::native;
static int checks = 0;
#define CHECK(x)                                                               \
  do {                                                                         \
    ++checks;                                                                  \
    if (!(x))                                                                  \
      throw std::runtime_error(#x);                                            \
  } while (false)
template <class F> void throws(F f) {
  bool fail = false;
  try {
    f();
  } catch (const std::exception &) {
    fail = true;
  }
  CHECK(fail);
}
int main() {
  try {
    std::map<std::string, std::shared_ptr<arrow::Scalar>> scalars{
        {"sum", std::make_shared<arrow::DoubleScalar>(123.75)},
        {"count", std::make_shared<arrow::Int64Scalar>(17)},
        {"null", arrow::MakeNullScalar(arrow::float64())},
        {"text", std::make_shared<arrow::StringScalar>("ab\"c")},
        {"decimal", std::make_shared<arrow::Decimal128Scalar>(
                        arrow::Decimal128(12345), arrow::decimal128(12, 2))}};
    auto scalar_copy = decode_scalars(encode_scalars(scalars));
    for (auto &[k, v] : scalars)
      CHECK(scalar_copy.at(k)->Equals(*v));
    auto sum =
        std::static_pointer_cast<arrow::DoubleScalar>(scalar_copy.at("sum"))
            ->value;
    auto count =
        std::static_pointer_cast<arrow::Int64Scalar>(scalar_copy.at("count"))
            ->value;
    CHECK(sum / count == 123.75 / 17);
    arrow::Int64Builder key;
    arrow::StringBuilder payload;
    CHECK(key.AppendValues({1, 1, 2}).ok());
    CHECK(payload.Append("x").ok());
    CHECK(payload.AppendNull().ok());
    CHECK(payload.Append("z").ok());
    auto keys = key.Finish().ValueOrDie();
    auto values = payload.Finish().ValueOrDie();
    auto table =
        arrow::Table::Make(arrow::schema({arrow::field("k", arrow::int64()),
                                          arrow::field("v", arrow::utf8())}),
                           {keys, values});
    auto decoded = decode_table(encode_table(table));
    CHECK(decoded->Equals(*table));
    // Duplicate-key build rows and NULL payloads survive in original row order.
    CHECK(decoded->num_rows() == 3 && decoded->column(1)->null_count() == 1);
    StateImage s{"group",
                 "q17/group0",
                 "plan-hash",
                 3,
                 5,
                 {{"scan-a", 47}, {"scan-b", 19}},
                 {{"partial", encode_scalars(scalars)},
                  {"pending", encode_table(table)}}};
    auto packed = pack(s);
    auto copy = unpack(packed);
    validate(copy, "group", "q17/group0", "plan-hash", 3);
    CHECK(copy.input_frontiers == s.input_frontiers &&
          copy.output_frontier == 5);
    CHECK(decode_table(copy.fields.at("pending"))->Equals(*table));
    throws([&] { validate(copy, "sort", "q17/group0", "plan-hash", 3); });
    throws([&] { validate(copy, "group", "q17/group0", "other-plan", 3); });
    throws([&] { validate(copy, "group", "q17/group0", "plan-hash", 4); });
    throws([&] { pack(s, 32); });
    auto corrupt = packed;
    corrupt[30] ^= 1;
    throws([&] { unpack(corrupt); });
    packed.pop_back();
    throws([&] { unpack(packed); });
    CHECK(decode_strings(encode_strings({"SUM(a*b)", "AVG(v)", "a\nb"})) ==
          std::vector<std::string>({"SUM(a*b)", "AVG(v)", "a\nb"}));
    CHECK(decode_scalars(encode_scalars({})).empty());
    std::cout << checks << " Arrow state checks passed\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
