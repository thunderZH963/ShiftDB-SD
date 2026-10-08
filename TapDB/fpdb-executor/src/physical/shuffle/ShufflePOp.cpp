//
// Created by matt on 17/6/20.
//

#include <fpdb/executor/physical/shuffle/ShufflePOp.h>
#include <fpdb/executor/physical/shuffle/ShuffleKernel.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/tuple/TupleSet.h>
#include <fpdb/tuple/ColumnBuilder.h>
#include <utility>

using namespace fpdb::executor::physical::shuffle;
using namespace fpdb::tuple;

ShufflePOp::ShufflePOp(string name,
                       vector<string> projectColumnNames,
                       int nodeId,
                       vector<string> shuffleColumnNames) :
	PhysicalOp(move(name), SHUFFLE, move(projectColumnNames), nodeId),
	shuffleColumnNames_(move(shuffleColumnNames)) {}

std::string ShufflePOp::getTypeString() const {
  return "ShufflePOp";
}

void ShufflePOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
	  this->onStart();
  } else if (msg.message().type() == MessageType::TUPLE) {
    auto tupleMessage = dynamic_cast<const TupleMessage &>(msg.message());
    this->onTuple(tupleMessage);
  } else if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else if (msg.message().type() == MessageType::DEBUG) {
    this->onDebug();
  } else {
    ctx()->notifyError(fmt::format("Unrecognized message type: {}, {}" + msg.message().getTypeString(), name()));
  }
}

void ShufflePOp::produce(const shared_ptr<PhysicalOp> &operator_) {
  PhysicalOp::produce(operator_);
  consumerVec_.emplace_back(operator_->name());
}

void ShufflePOp::onStart() {
  vector<std::string> vecs; 
  for (auto consumer : consumerVec_) {
    if (ctx()->spawnAddr().at(consumer) == ctx()->opActiveAddr(consumer)) {
      vecs.emplace_back(consumer);
    }
  }
  consumerVec_.clear();
  consumerVec_.insert(consumerVec_.end(), vecs.begin(), vecs.end());
  SPDLOG_DEBUG("Starting '{}'  |  numConsumers: {}", name(), consumerVec_.size());
  buffers_.resize(consumerVec_.size(), nullopt);
}

void ShufflePOp::onComplete(const CompleteMessage &) {
  if (recvAllCompleteMsg()) {
    if (!receiveTupleSet) {
      ctx()->notifyComplete();appendOpTime();
    } else if (readAllFile()) {
      for (int partitionIndex = 0; partitionIndex < static_cast<int>(buffers_.size()); ++partitionIndex) {
        auto sendResult = send(partitionIndex, true);
        if (!sendResult)
          ctx()->notifyError(sendResult.error());
      }

      ctx()->notifyComplete();appendOpTime();
    } else {
      rowLimit_mtx.lock();
      vec.push_back(this->ctx()->operatorActor());
      rowLimit_mtx.unlock();
    }
  }
}

void ShufflePOp::onDebug() {
  while (!readAllFile()) {
    if (rowLimit.load() <= 0) {
        rowLimit_mtx.lock();
        vec.push_back(this->ctx()->operatorActor());
        rowLimit_mtx.unlock();
        return;
    }
    auto expTupleSet = readTuple();

    vector<shared_ptr<TupleSet>> shuffledTupleSets;
    // Check empty
    if(expTupleSet->numRows() == 0){
      for (size_t s = 0; s < consumerVec_.size(); ++s) {
        shuffledTupleSets.emplace_back(expTupleSet);
      }
    } else {
      // Shuffle the tuple set
      auto expectedShuffledTupleSets = ShuffleKernel::shuffle(shuffleColumnNames_, consumerVec_.size(), *expTupleSet);
      if (!expectedShuffledTupleSets.has_value()) {
        ctx()->notifyError(fmt::format("{}, {}", expectedShuffledTupleSets.error(), name()));
      }
      shuffledTupleSets = expectedShuffledTupleSets.value();
    }

    // Send the shuffled tuple sets to consumers
    int partitionIndex = 0;
    for (const auto &shuffledTupleSet: shuffledTupleSets) {
      auto bufferAndSendResult = buffer(shuffledTupleSet, partitionIndex)
        .and_then([&]() { return send(partitionIndex, true); });
      if (!bufferAndSendResult)
        ctx()->notifyError(bufferAndSendResult.error());
      ++partitionIndex;
    }
  }
  //No more file in disk
  for (int partitionIndex = 0; partitionIndex < static_cast<int>(buffers_.size()); ++partitionIndex) {
    auto sendResult = send(partitionIndex, true);
    if (!sendResult)
      ctx()->notifyError(sendResult.error());
  }

  ctx()->notifyComplete();appendOpTime();
}

tl::expected<void, string> ShufflePOp::buffer(const shared_ptr<TupleSet> &tupleSet, int partitionIndex) {
  // Add the tuple set to the buffer
  if (!buffers_[partitionIndex].has_value()) {
	buffers_[partitionIndex] = tupleSet;
  } else {
    const auto &bufferedTupleSet = buffers_[partitionIndex].value();
	const auto &concatenateResult = TupleSet::concatenate({bufferedTupleSet, tupleSet});
	if (!concatenateResult)
	  return tl::make_unexpected(concatenateResult.error());
	buffers_[partitionIndex] = concatenateResult.value();

	auto expectedTable = buffers_[partitionIndex].value()->table()
		->CombineChunks(arrow::default_memory_pool());
	if (expectedTable.ok())
	  buffers_[partitionIndex] = TupleSet::make(*expectedTable);
	else
	  return tl::make_unexpected(expectedTable.status().message());
  }

  return {};
}

tl::expected<void, string> ShufflePOp::send(int partitionIndex, bool force) {
  // If the tupleset is big enough, send it, then clear the buffer
  if (buffers_[partitionIndex].has_value() && (force || buffers_[partitionIndex].value()->numRows() >= DefaultBufferSize)) {
  auto tupleSet = TupleSet::make(buffers_[partitionIndex].value()->table());
	shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(
	        tupleSet, name());
	ctx()->send(tupleMessage, consumerVec_[partitionIndex]);
	buffers_[partitionIndex] = nullopt;
  }

  return {};
}

void ShufflePOp::onTuple(const TupleMessage &message) {
  receiveTupleSet = true;
  // Get the tuple set
  const auto &tupleSet = message.tuples();
#if OP_INFO>0
  input_row+=tupleSet->numRows();
	input_size+=tupleSet->size();
  if(input_row_first==0){
    input_row_first=tupleSet->numRows();
  }
#endif
  vector<shared_ptr<TupleSet>> shuffledTupleSets;

  if (!shouldSpawnOnRemote_ || rowLimit.load() > 0) {
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
    uint64_t stTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif
    // Check empty
    if(tupleSet->numRows() == 0){
      for (size_t s = 0; s < consumerVec_.size(); ++s) {
        shuffledTupleSets.emplace_back(tupleSet);
      }
    }

    else {
      // Shuffle the tuple set
      auto expectedShuffledTupleSets = ShuffleKernel::shuffle(shuffleColumnNames_, consumerVec_.size(), *tupleSet);
      if (!expectedShuffledTupleSets.has_value()) {
        ctx()->notifyError(fmt::format("{}, {}", expectedShuffledTupleSets.error(), name()));
      }
      shuffledTupleSets = expectedShuffledTupleSets.value();
    }
#if (OP_INFO>0) || (RUNTIME_SAMPLE_EN>0)
    uint64_t enTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  	exectime_+=enTime-stTime;
#endif
    // Send the shuffled tuple sets to consumers
    int partitionIndex = 0;
    for (const auto &shuffledTupleSet: shuffledTupleSets) {
      auto bufferAndSendResult = buffer(shuffledTupleSet, partitionIndex)
        .and_then([&]() { return send(partitionIndex, true); });
      if (!bufferAndSendResult)
        ctx()->notifyError(bufferAndSendResult.error());
      ++partitionIndex;
    }
  } else {
    dumpToFile(tupleSet);
  }
}

void ShufflePOp::clear() {
  buffers_.clear();
}
