#include "shiftdb/tapdb_operator_state.hpp"
#include <fpdb/executor/physical/aggregate/AggregatePOp.h>
#include <fpdb/executor/physical/group/GroupKernel.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinBuildKernel.h>
#include <fpdb/executor/physical/sort/SortPOp.h>
#include <fpdb/tuple/ArrayAppenderWrapper.h>
#include <functional>
#include <numeric>
#include <stdexcept>
using namespace fpdb::executor::physical;
using namespace fpdb::tuple;
namespace shiftdb::native {
namespace {
using Results =
    std::vector<std::vector<std::shared_ptr<aggregate::AggregateResult>>>;
using Functions = std::vector<std::shared_ptr<aggregate::AggregateFunction>>;
void checked(tl::expected<void, std::string> x) {
  if (!x)
    throw std::runtime_error(x.error());
}
template <class T> auto checked(T x) {
  if (!x)
    throw std::runtime_error(x.error());
  return x.value();
}
// Do not use checked() for Arrow results (whose errors are Status).
template <class T> T arrow_get(arrow::Result<T> x) {
  if (!x.ok())
    throw std::runtime_error(x.status().ToString());
  return std::move(x).ValueOrDie();
}
uint64_t count(const StateImage &s, const std::string &key) {
  auto text = decode_strings(s.fields.at(key));
  if (text.size() != 1 || text[0].empty() ||
      text[0].find_first_not_of("0123456789") != std::string::npos)
    throw std::runtime_error("invalid count: " + key);
  size_t n = 0;
  auto value = std::stoull(text[0], &n);
  if (n != text[0].size() || value > 256ULL * 1024 * 1024)
    throw std::runtime_error("oversized count");
  return value;
}
void put_count(StateImage &s, const std::string &key, uint64_t n) {
  s.fields[key] = encode_strings({std::to_string(n)});
}
std::vector<std::string> signatures(const Functions &f) {
  std::vector<std::string> out;
  for (const auto &x : f) {
    if (!x)
      throw std::runtime_error("null aggregate function");
    out.push_back(std::to_string(x->getType()));
    out.push_back(x->getOutputColumnName());
    out.push_back(x->getExpression() ? x->getExpression()->alias() : "<none>");
  }
  return out;
}
std::shared_ptr<arrow::Table>
empty_table(const std::shared_ptr<arrow::Schema> &schema) {
  arrow::ArrayVector arrays;
  for (auto &f : schema->fields())
    arrays.push_back(arrow_get(arrow::MakeArrayOfNull(f->type(), 0)));
  return arrow::Table::Make(schema, arrays, 0);
}
void put_results(
    StateImage &s, const std::string &prefix, const Results &rows,
    const std::function<Bytes(const aggregate::AggregateResult &)> &encode) {
  put_count(s, prefix + "/functions", rows.size());
  for (size_t f = 0; f < rows.size(); ++f) {
    auto name = prefix + "/" + std::to_string(f);
    put_count(s, name + "/count", rows[f].size());
    for (size_t i = 0; i < rows[f].size(); ++i) {
      if (!rows[f][i])
        throw std::runtime_error("null partial");
      s.fields[name + "/" + std::to_string(i)] = encode(*rows[f][i]);
    }
  }
}
Results get_results(const StateImage &s, const std::string &prefix,
                    size_t functions) {
  if (count(s, prefix + "/functions") != functions)
    throw std::runtime_error("aggregate arity mismatch");
  Results rows(functions);
  for (size_t f = 0; f < functions; ++f) {
    auto name = prefix + "/" + std::to_string(f);
    auto n = count(s, name + "/count");
    if (n > s.fields.size())
      throw std::runtime_error("invalid partial count");
    for (uint64_t i = 0; i < n; ++i) {
      auto r = std::make_shared<aggregate::AggregateResult>();
      for (auto &[key, value] :
           decode_scalars(s.fields.at(name + "/" + std::to_string(i))))
        r->put(key, value);
      rows[f].push_back(r);
    }
  }
  return rows;
}
Bytes key_bytes(const std::shared_ptr<TupleKey> &key) {
  auto scalars = checked(key->getScalars());
  arrow::ArrayVector arrays;
  arrow::FieldVector fields;
  for (size_t i = 0; i < scalars.size(); ++i) {
    if (!scalars[i]->is_valid)
      throw std::runtime_error(
          "nullable group keys unsupported by upstream TupleKey");
    arrays.push_back(arrow_get(arrow::MakeArrayFromScalar(*scalars[i], 1)));
    fields.push_back(arrow::field("k" + std::to_string(i), scalars[i]->type));
  }
  return encode_table(arrow::Table::Make(arrow::schema(fields), arrays, 1));
}
std::shared_ptr<TupleKey> decode_key(const Bytes &b, size_t arity) {
  auto table = decode_table(b);
  if (table->num_rows() != 1 || size_t(table->num_columns()) != arity)
    throw std::runtime_error("group key arity mismatch");
  arrow::ArrayVector arrays;
  for (auto c : table->columns()) {
    if (c->null_count())
      throw std::runtime_error("nullable group key");
    arrays.push_back(c->chunk(0));
  }
  auto batch = arrow::RecordBatch::Make(table->schema(), 1, arrays);
  std::vector<int> indices(arity);
  std::iota(indices.begin(), indices.end(), 0);
  return checked(TupleKeyBuilder::make(0, indices, *batch));
}
StateImage read(const Bytes &b, const StateImage &expected,
                const std::string &kind) {
  auto s = unpack(b);
  validate(s, kind, expected.query_operator, expected.plan_fingerprint,
           expected.generation);
  return s;
}
void validate_join_keys(const std::shared_ptr<arrow::Table> &table,
                        const std::vector<std::string> &keys) {
  for (const auto &key : keys) {
    auto column = table->GetColumnByName(key);
    if (!column || column->null_count() != 0)
      throw std::runtime_error(
          "native build checkpoint requires non-null join keys");
  }
}
void bind(StateImage &s, const std::string &kind) {
  s.kind = kind;
  s.fields.clear();
}
void quiescent_memory(PhysicalOp &op) {
  if (!op.readAllFile() || !op.readAllSavedTuple())
    throw std::runtime_error(
        "spill or saved output requires a file-backed adapter");
}
} // namespace
Bytes TapDBStateCodec::result(const aggregate::AggregateResult &r) {
  std::map<std::string, std::shared_ptr<arrow::Scalar>> values;
  for (const auto &[key, value] : r.resultMap_) {
    if (!value)
      throw std::runtime_error("null AggregateResult pointer");
    values[key] = value->getArrowScalar();
  }
  return encode_scalars(values);
}
Bytes TapDBStateCodec::snapshot(aggregate::AggregatePOp &op, StateImage s) {
  quiescent_memory(op);
  bind(s, "tapdb.aggregate.v1");
  s.fields["functions"] = encode_strings(signatures(op.functions_));
  put_results(s, "partial", op.aggregateResults_, result);
  for (size_t i = 0; i < op.functions_.size(); ++i) {
    auto &schema = op.functions_[i]->inputSchema_;
    if (schema)
      s.fields["schema/" + std::to_string(i)] =
          encode_table(empty_table(*schema));
    if (op.functions_[i]->aggColumnDataType_)
      s.fields["type/" + std::to_string(i)] =
          encode_table(empty_table(arrow::schema(
              {arrow::field("value", op.functions_[i]->aggColumnDataType_)})));
  }
  put_count(s, "received_input", op.receiveTupleSet);
  return pack(s);
}
StateImage TapDBStateCodec::restore(aggregate::AggregatePOp &op, const Bytes &b,
                                    const StateImage &expected) {
  quiescent_memory(op);
  auto s = read(b, expected, "tapdb.aggregate.v1");
  if (decode_strings(s.fields.at("functions")) != signatures(op.functions_))
    throw std::runtime_error("aggregate definition mismatch");
  for (const auto &partial : op.aggregateResults_)
    if (!partial.empty())
      throw std::runtime_error("restore requires a fresh aggregate target");
  auto results = get_results(s, "partial", op.functions_.size());
  for (size_t i = 0; i < op.functions_.size(); ++i) {
    auto it = s.fields.find("schema/" + std::to_string(i));
    if (it != s.fields.end())
      checked(op.functions_[i]->compile(decode_table(it->second)->schema()));
    else {
      auto type = s.fields.find("type/" + std::to_string(i));
      if (type != s.fields.end()) {
        auto schema = decode_table(type->second)->schema();
        if (schema->num_fields() != 1)
          throw std::runtime_error("invalid aggregate value type");
        op.functions_[i]->aggColumnDataType_ = schema->field(0)->type();
      } else if (!results[i].empty() && op.functions_[i]->getExpression())
        throw std::runtime_error("missing aggregate input type");
    }
  }
  auto received = count(s, "received_input");
  op.aggregateResults_.swap(results);
  op.receiveTupleSet = received != 0;
  return s;
}
Bytes TapDBStateCodec::snapshot(group::GroupKernel &kernel, StateImage s) {
  bind(s, "tapdb.group.v1");
  if (!kernel.groupFinalAggregateResultVectorMap_.empty())
    throw std::runtime_error("checkpoint before group finalization only");
  s.fields["columns"] = encode_strings(kernel.groupColumnNames_);
  s.fields["functions"] =
      encode_strings(signatures(kernel.aggregateFunctions_));
  put_count(s, "has_schema", kernel.inputSchema_.has_value());
  if (kernel.inputSchema_)
    s.fields["input_schema"] = encode_table(empty_table(*kernel.inputSchema_));
  put_count(s, "partial_groups", kernel.aggregateResultsPartial_.size());
  size_t i = 0;
  for (const auto &[key, partial] : kernel.aggregateResultsPartial_) {
    auto name = "partial/" + std::to_string(i++);
    s.fields[name + "/key"] = key_bytes(key);
    put_results(s, name, *partial, result);
  }
  put_count(s, "pending_groups", kernel.groupArrayAppenderVectorMap_.size());
  i = 0;
  uint64_t rows = 0;
  for (const auto &[key, appenders] : kernel.groupArrayAppenderVectorMap_) {
    if (!kernel.aggregateInputSchema_)
      throw std::runtime_error("missing buffered group schema");
    auto name = "pending/" + std::to_string(i++);
    s.fields[name + "/key"] = key_bytes(key);
    arrow::ArrayVector arrays;
    for (const auto &appender : appenders)
      arrays.push_back(checked(appender->snapshot()));
    auto table = arrow::Table::Make(*kernel.aggregateInputSchema_, arrays);
    rows += table->num_rows();
    s.fields[name + "/table"] = encode_table(table);
  }
  if (rows != uint64_t(kernel.numRows_))
    throw std::runtime_error("group pending-row count mismatch");
  put_count(s, "pending_rows", rows);
  return pack(s);
}
StateImage TapDBStateCodec::restore(group::GroupKernel &kernel, const Bytes &b,
                                    const StateImage &expected) {
  auto s = read(b, expected, "tapdb.group.v1");
  if (decode_strings(s.fields.at("columns")) != kernel.groupColumnNames_ ||
      decode_strings(s.fields.at("functions")) !=
          signatures(kernel.aggregateFunctions_))
    throw std::runtime_error("group definition mismatch");
  if (kernel.inputSchema_ || !kernel.aggregateResultsPartial_.empty() ||
      !kernel.groupArrayAppenderVectorMap_.empty())
    throw std::runtime_error("restore requires a fresh group target");
  group::GroupKernel next(kernel.groupColumnNames_, kernel.aggregateFunctions_);
  if (count(s, "has_schema")) {
    auto schema = decode_table(s.fields.at("input_schema"))->schema();
    checked(next.cache(*TupleSet::make(empty_table(schema))));
    for (auto &f : next.aggregateFunctions_)
      checked(f->compile(*next.aggregateInputSchema_));
  }
  auto n = count(s, "partial_groups");
  if (n > s.fields.size())
    throw std::runtime_error("invalid group count");
  for (uint64_t i = 0; i < n; ++i) {
    auto name = "partial/" + std::to_string(i);
    auto key =
        decode_key(s.fields.at(name + "/key"), kernel.groupColumnNames_.size());
    auto values = std::make_shared<Results>(
        get_results(s, name, kernel.aggregateFunctions_.size()));
    if (!next.aggregateResultsPartial_.emplace(key, values).second)
      throw std::runtime_error("duplicate partial group");
  }
  uint64_t rows = 0;
  n = count(s, "pending_groups");
  if (n > s.fields.size())
    throw std::runtime_error("invalid pending-group count");
  for (uint64_t i = 0; i < n; ++i) {
    auto name = "pending/" + std::to_string(i);
    auto key =
        decode_key(s.fields.at(name + "/key"), kernel.groupColumnNames_.size());
    auto table = decode_table(s.fields.at(name + "/table"));
    if (!next.aggregateInputSchema_ ||
        !table->schema()->Equals(**next.aggregateInputSchema_))
      throw std::runtime_error("pending input schema mismatch");
    group::ArrayAppenderVector appenders;
    for (auto &column : table->columns()) {
      auto appender = checked(ArrayAppenderBuilder::make(column->type()));
      for (auto &chunk : column->chunks())
        for (int64_t j = 0; j < chunk->length(); ++j)
          checked(appender->appendValue(chunk, j));
      appenders.push_back(appender);
    }
    rows += table->num_rows();
    if (!next.groupArrayAppenderVectorMap_.emplace(key, appenders).second)
      throw std::runtime_error("duplicate pending group");
  }
  if (rows != count(s, "pending_rows"))
    throw std::runtime_error("pending row count mismatch");
  next.numRows_ = rows;
  kernel = std::move(next);
  return s;
}
Bytes TapDBStateCodec::snapshot(join::HashJoinBuildKernel &kernel,
                                StateImage s) {
  bind(s, "tapdb.hash-build.v1");
  s.fields["keys"] = encode_strings(kernel.columnNames_);
  put_count(s, "present", kernel.tupleSetIndex_.has_value());
  if (kernel.tupleSetIndex_) {
    auto table = (*kernel.tupleSetIndex_)->getTupleSet()->table();
    validate_join_keys(table, kernel.columnNames_);
    s.fields["tuples"] = encode_table(table);
  }
  return pack(s);
}
StateImage TapDBStateCodec::restore(join::HashJoinBuildKernel &kernel,
                                    const Bytes &b,
                                    const StateImage &expected) {
  auto s = read(b, expected, "tapdb.hash-build.v1");
  if (decode_strings(s.fields.at("keys")) != kernel.columnNames_)
    throw std::runtime_error("join key definition mismatch");
  if (kernel.tupleSetIndex_)
    throw std::runtime_error("restore requires a fresh build target");
  std::optional<std::shared_ptr<TupleSetIndex>> index;
  if (count(s, "present")) {
    auto table = decode_table(s.fields.at("tuples"));
    validate_join_keys(table, kernel.columnNames_);
    index = checked(
        TupleSetIndex::make(kernel.columnNames_, TupleSet::make(table)));
    checked((*index)->validate());
  }
  kernel.tupleSetIndex_ = std::move(index);
  return s;
}
Bytes TapDBStateCodec::snapshot(sort::SortPOp &op, StateImage s) {
  quiescent_memory(op);
  bind(s, "tapdb.sort-buffer.v1");
  if (s.output_frontier != 0 || op.allRows != 0)
    throw std::runtime_error(
        "sort merge/output checkpoint requires run-state adapter");
  std::vector<std::string> keys;
  for (auto &k : op.sortKeys_) {
    keys.push_back(k.getName());
    keys.push_back(std::to_string(k.getOrder()));
  }
  s.fields["keys"] = encode_strings(keys);
  put_count(s, "present", op.buffer_.has_value());
  if (op.buffer_)
    s.fields["tuples"] = encode_table((*op.buffer_)->table());
  put_count(s, "received_input", op.receiveTupleSet);
  put_count(s, "line_size", op.LINESIZE);
  put_count(s, "arrived", op.arrivedBuild_);
  put_count(s, "producers", op.totBuild_);
  return pack(s);
}
StateImage TapDBStateCodec::restore(sort::SortPOp &op, const Bytes &b,
                                    const StateImage &expected) {
  quiescent_memory(op);
  auto s = read(b, expected, "tapdb.sort-buffer.v1");
  if (op.buffer_ || op.allRows != 0 || s.output_frontier != 0)
    throw std::runtime_error("restore requires pre-output sort target");
  std::vector<std::string> keys;
  for (auto &k : op.sortKeys_) {
    keys.push_back(k.getName());
    keys.push_back(std::to_string(k.getOrder()));
  }
  if (decode_strings(s.fields.at("keys")) != keys)
    throw std::runtime_error("sort comparator mismatch");
  std::optional<std::shared_ptr<TupleSet>> buffer;
  if (count(s, "present"))
    buffer = TupleSet::make(decode_table(s.fields.at("tuples")));
  auto received = count(s, "received_input"), line_size = count(s, "line_size");
  auto arrived = count(s, "arrived"), producers = count(s, "producers");
  op.receiveTupleSet = received != 0;
  op.LINESIZE = line_size;
  op.buffer_ = std::move(buffer);
  op.arrivedBuild_ = arrived;
  op.totBuild_ = producers;
  op.arrowSortOptions_.reset();
  return s;
}
} // namespace shiftdb::native
