#pragma once
#include <arrow/api.h>
#include <cstdint>
#include <map>
#include <string>
#include <vector>
namespace shiftdb::native {
using Bytes = std::vector<uint8_t>;
// Caller binds the canonical physical-plan/schema fingerprint to a
// query/operator. The envelope never grants publication authority or replaces
// an ownership record.
struct StateImage {
  std::string kind, query_operator, plan_fingerprint;
  uint64_t generation = 0, output_frontier = 0;
  std::map<std::string, uint64_t> input_frontiers;
  std::map<std::string, Bytes> fields;
};
Bytes pack(const StateImage &, uint64_t limit = 256ULL * 1024 * 1024);
StateImage unpack(const Bytes &, uint64_t limit = 256ULL * 1024 * 1024);
void validate(const StateImage &, const std::string &kind,
              const std::string &op, const std::string &fingerprint,
              uint64_t source_generation);
Bytes encode_table(const std::shared_ptr<arrow::Table> &);
std::shared_ptr<arrow::Table> decode_table(const Bytes &);
Bytes encode_scalars(
    const std::map<std::string, std::shared_ptr<arrow::Scalar>> &);
std::map<std::string, std::shared_ptr<arrow::Scalar>>
decode_scalars(const Bytes &);
Bytes encode_strings(const std::vector<std::string> &);
std::vector<std::string> decode_strings(const Bytes &);
} // namespace shiftdb::native
