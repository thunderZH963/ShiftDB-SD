#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PREDICATEMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PREDICATEMESSAGE_H

#include <fpdb/executor/message/Message.h>

namespace fpdb::executor::message {
/**
 * Message fired when an operator completes its work
 */
class PredicateMessage : public Message {

public:
  explicit PredicateMessage(std::string sender, int location);
  explicit PredicateMessage(std::string sender, std::string consumer, int location, uint64_t rows, uint64_t size,int factor);
  explicit PredicateMessage(std::string sender, bool sample,uint64_t sampleTime,int sampleLocation);
  PredicateMessage() = default;
  PredicateMessage(const PredicateMessage&) = default;
  PredicateMessage& operator=(const PredicateMessage&) = default;
  int getLocation() const;
  unsigned long long getSize() const;
  int getRows() const;
  std::string getConsumer() const;
  int getFactor() const;
  std::string getTypeString() const override;
  bool getSample() const;
  uint64_t getSampleTime() const;
  int getSampleLocation() const;
private:
    int location_;
    uint64_t rows_;
    uint64_t size_;
    std::string consumer_;
    int factor_;
    bool sample_=false;
    uint64_t sampleTime_;
    int sampleLocation_;
// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, PredicateMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("location", msg.location_),
                                f.field("rows", msg.rows_),
                                f.field("size", msg.size_),
                                f.field("factor", msg.factor_),
                                f.field("sample", msg.sample_),
                                f.field("sampleTime", msg.sampleTime_),
                                f.field("sampleLocation", msg.sampleLocation_),
                                f.field("consumer", msg.consumer_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PREDICATEMESSAGE_H
