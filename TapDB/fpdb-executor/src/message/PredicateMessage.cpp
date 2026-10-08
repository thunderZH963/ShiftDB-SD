#include <fpdb/executor/message/PredicateMessage.h>
#include <utility>

namespace fpdb::executor::message {

PredicateMessage::PredicateMessage(std::string sender, int location) :
    Message(PREDICATE, std::move(sender)), consumer_(""), location_(location), rows_(0), size_(0) {}

PredicateMessage::PredicateMessage(std::string sender, std::string consumer, int location, uint64_t rows, uint64_t size,int factor) :
    Message(PREDICATE, std::move(sender)), consumer_(std::move(consumer)), location_(location), rows_(rows), size_(size),factor_(factor) {}

PredicateMessage::PredicateMessage(std::string sender, bool sample,uint64_t sampleTime,int sampleLocation) :
    Message(PREDICATE, std::move(sender)), sample_(std::move(sample)), sampleTime_(sampleTime), sampleLocation_(sampleLocation) {}
std::string PredicateMessage::getTypeString() const {
  return "PredicateMessage";
}

int PredicateMessage::getLocation() const {
  return location_;
}

unsigned long long PredicateMessage::getSize() const {
  return size_;
}

int PredicateMessage::getRows() const {
  return rows_;
}

std::string PredicateMessage::getConsumer() const {
  return consumer_;
}

bool PredicateMessage::getSample() const{
  return sample_;
}
uint64_t PredicateMessage::getSampleTime() const{
  return sampleTime_;
}
int PredicateMessage::getSampleLocation() const{
  return sampleLocation_;
}

int PredicateMessage::getFactor() const {
  return factor_;
}
}