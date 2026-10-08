#pragma once
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>
namespace shiftdb {
enum class Kind : uint64_t { Aggregate=1, InnerJoin=2, Sort=3 };
struct Row { int64_t id=0,key=0; std::optional<int64_t> value; };
struct Aggregate { int64_t sum=0,min=0,max=0; uint64_t count=0; };
struct Image { std::vector<uint8_t> bytes; uint64_t checksum=0; };
struct Delta { std::string ticket; uint64_t sequence=0; Image post_image; };
struct Progress { uint64_t log_sequence=0; std::map<std::string,uint64_t> input; };
enum class Consume { Applied, Duplicate, Gap, Wait, Fenced };
// Executable integer-key reference adapters. Not a serialization of Arrow pointers.
// One instance represents one state partition; independently migratable partitions
// each have a separate instance and ownership record.
class StatePartition {
 struct Data {
  Kind kind=Kind::Aggregate;
  std::map<std::string,uint64_t> input;
  std::map<int64_t,Aggregate> groups;
  std::vector<Row> rows; // join build tuples or sorted rows
  std::vector<std::pair<int64_t,int64_t>> matches; // probe ID, build ID
  uint64_t sequence=0,output_cursor=0;
  bool finalized=false,probe_started=false;
 } data_;
 mutable std::mutex mutex_;
 std::string ticket_;
 std::map<uint64_t,Delta> log_,pending_;
 uint64_t generation_=0,max_encoded_bytes_,max_pending_bytes_;
 bool frozen_=true,logging_=false;
 static Image encode(const Data&);
 static Data decode(const Image&);
public:
 explicit StatePartition(Kind kind,uint64_t encoded_limit=64*1024*1024,
                         uint64_t reorder_limit=16*1024*1024);
 void activate(uint64_t generation);
 void pause();
 void retire();
 Consume consume(const std::string& edge,uint64_t sequence,uint64_t generation,
                 const std::vector<Row>& rows);
 void finalize(uint64_t generation);
 Image snapshot(const std::string& ticket);
 void restore(const Image&,const std::string& ticket);
 uint64_t apply(const Delta&); // contiguous ACK, bounded reorder buffer
 std::vector<Delta> updates()const;
 void acknowledge(uint64_t sequence);
 uint64_t dirty_bytes()const;
 Progress freeze();
 Progress progress()const;
 void stop_logging();
 std::map<int64_t,Aggregate> groups()const;
 std::vector<std::pair<int64_t,int64_t>> matches()const;
 std::vector<Row> sorted_rows()const;
 // Monotonic logical output cursor is transferred in images. Caller must also
 // check OwnershipStore::can_publish at the externally visible send boundary.
 std::optional<std::pair<uint64_t,Row>> next_sorted(uint64_t generation);
};
uint64_t checksum(const std::vector<uint8_t>&);
}
