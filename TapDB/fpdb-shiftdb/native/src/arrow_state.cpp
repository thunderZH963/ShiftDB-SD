#include "shiftdb/arrow_state.hpp"
#include "shiftdb/state.hpp"
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <limits>
#include <stdexcept>
namespace shiftdb::native {
namespace {
void ok(const arrow::Status &s) {
  if (!s.ok())
    throw std::runtime_error(s.ToString());
}
template <class T> T get(arrow::Result<T> result) {
  if (!result.ok())
    throw std::runtime_error(result.status().ToString());
  return std::move(result).ValueOrDie();
}
struct Writer {
  Bytes data;
  void number(uint64_t x) {
    for (int i = 0; i < 8; ++i)
      data.push_back(uint8_t(x >> (8 * i)));
  }
  void bytes(const Bytes &v) {
    number(v.size());
    data.insert(data.end(), v.begin(), v.end());
  }
  void text(const std::string &s) { bytes(Bytes(s.begin(), s.end())); }
};
struct Reader {
  const Bytes &data;
  size_t pos = 0;
  uint64_t number() {
    if (data.size() - pos < 8)
      throw std::runtime_error("truncated image");
    uint64_t x = 0;
    for (int i = 0; i < 8; ++i)
      x |= uint64_t(data[pos++]) << (8 * i);
    return x;
  }
  uint64_t count() {
    auto n = number();
    if (n > data.size() - pos)
      throw std::runtime_error("invalid count");
    return n;
  }
  Bytes bytes() {
    auto n = count();
    Bytes v(data.begin() + pos, data.begin() + pos + n);
    pos += n;
    return v;
  }
  std::string text() {
    auto b = bytes();
    return {b.begin(), b.end()};
  }
  void finish() {
    if (pos != data.size())
      throw std::runtime_error("trailing image bytes");
  }
};
} // namespace
Bytes pack(const StateImage &s, uint64_t limit) {
  if (s.kind.empty() || s.query_operator.empty() ||
      s.plan_fingerprint.empty() || !s.generation)
    throw std::invalid_argument("unbound snapshot identity");
  Writer w;
  w.number(0x5344424e41544956ULL);
  w.number(1);
  w.text(s.kind);
  w.text(s.query_operator);
  w.text(s.plan_fingerprint);
  w.number(s.generation);
  w.number(s.output_frontier);
  w.number(s.input_frontiers.size());
  for (auto &[k, v] : s.input_frontiers) {
    w.text(k);
    w.number(v);
  }
  w.number(s.fields.size());
  for (auto &[k, v] : s.fields) {
    w.text(k);
    w.bytes(v);
    if (w.data.size() > limit)
      throw std::length_error("snapshot budget exceeded");
  }
  w.number(checksum(w.data));
  if (w.data.size() > limit)
    throw std::length_error("snapshot budget exceeded");
  return std::move(w.data);
}
StateImage unpack(const Bytes &bytes, uint64_t limit) {
  if (bytes.size() > limit || bytes.size() < 24)
    throw std::length_error("invalid snapshot length");
  Bytes body(bytes.begin(), bytes.end() - 8);
  Reader tail{bytes};
  tail.pos = bytes.size() - 8;
  if (tail.number() != checksum(body))
    throw std::runtime_error("snapshot checksum mismatch");
  Reader r{body};
  if (r.number() != 0x5344424e41544956ULL || r.number() != 1)
    throw std::runtime_error("snapshot version mismatch");
  StateImage s;
  s.kind = r.text();
  s.query_operator = r.text();
  s.plan_fingerprint = r.text();
  s.generation = r.number();
  s.output_frontier = r.number();
  auto n = r.count();
  for (uint64_t i = 0; i < n; ++i) {
    auto k = r.text();
    auto v = r.number();
    if (!s.input_frontiers.emplace(k, v).second)
      throw std::runtime_error("duplicate frontier");
  }
  n = r.count();
  for (uint64_t i = 0; i < n; ++i) {
    auto k = r.text();
    auto v = r.bytes();
    if (!s.fields.emplace(k, std::move(v)).second)
      throw std::runtime_error("duplicate state field");
  }
  r.finish();
  if (s.kind.empty() || s.query_operator.empty() ||
      s.plan_fingerprint.empty() || !s.generation)
    throw std::runtime_error("invalid snapshot identity");
  return s;
}
void validate(const StateImage &s, const std::string &kind,
              const std::string &op, const std::string &fingerprint,
              uint64_t source_generation) {
  if (s.kind != kind || s.query_operator != op ||
      s.plan_fingerprint != fingerprint || s.generation != source_generation)
    throw std::runtime_error("incompatible operator snapshot");
}
Bytes encode_table(const std::shared_ptr<arrow::Table> &table) {
  if (!table)
    throw std::invalid_argument("null state table");
  ok(table->ValidateFull());
  auto stream = get(arrow::io::BufferOutputStream::Create());
  auto writer =
      get(arrow::ipc::MakeStreamWriter(stream.get(), table->schema()));
  ok(writer->WriteTable(*table));
  ok(writer->Close());
  auto buffer = get(stream->Finish());
  return {buffer->data(), buffer->data() + buffer->size()};
}
std::shared_ptr<arrow::Table> decode_table(const Bytes &bytes) {
  if (bytes.empty())
    throw std::runtime_error("empty table payload");
  // Own the bytes after this function returns; Arrow arrays may retain slices.
  auto copy = get(arrow::Buffer::Wrap(bytes)->CopySlice(
      0, bytes.size(), arrow::default_memory_pool()));
  auto input = std::make_shared<arrow::io::BufferReader>(copy);
  auto reader = get(arrow::ipc::RecordBatchStreamReader::Open(input));
  auto table = get(arrow::Table::FromRecordBatchReader(reader.get()));
  ok(table->ValidateFull());
  return table;
}
Bytes encode_scalars(
    const std::map<std::string, std::shared_ptr<arrow::Scalar>> &values) {
  // A separate count avoids ambiguous zero-column one-row tables.
  Writer w;
  w.number(values.size());
  for (auto &[name, value] : values) {
    if (!value)
      throw std::invalid_argument("null scalar pointer");
    auto array = get(arrow::MakeArrayFromScalar(*value, 1));
    w.text(name);
    w.bytes(encode_table(arrow::Table::Make(
        arrow::schema({arrow::field("value", value->type)}), {array})));
  }
  return std::move(w.data);
}
std::map<std::string, std::shared_ptr<arrow::Scalar>>
decode_scalars(const Bytes &bytes) {
  Reader r{bytes};
  auto n = r.count();
  std::map<std::string, std::shared_ptr<arrow::Scalar>> out;
  for (uint64_t i = 0; i < n; ++i) {
    auto name = r.text();
    auto table = decode_table(r.bytes());
    if (table->num_columns() != 1 || table->num_rows() != 1)
      throw std::runtime_error("invalid scalar table");
    auto value = get(table->column(0)->GetScalar(0));
    if (!out.emplace(name, value).second)
      throw std::runtime_error("duplicate scalar key");
  }
  r.finish();
  return out;
}
Bytes encode_strings(const std::vector<std::string> &items) {
  Writer w;
  w.number(items.size());
  for (auto &v : items)
    w.text(v);
  return std::move(w.data);
}
std::vector<std::string> decode_strings(const Bytes &data) {
  Reader r{data};
  auto n = r.count();
  std::vector<std::string> out;
  for (uint64_t i = 0; i < n; ++i)
    out.push_back(r.text());
  r.finish();
  return out;
}
} // namespace shiftdb::native
