//
// Created by czk on 09/04/22.
//

#include <fpdb/executor/ModeCtrl.h>
#include <fpdb/util/Util.h>
#include "string"

using namespace fpdb::util;

namespace fpdb::executor {
ModeCtrl::ModeCtrl(bool external) {
    if (external) {
        std::unordered_map<std::string, std::string> config = readConfig("mode.conf");
        _mapping.insert({POpType::AGGREGATE, std::stoi(config["AGGREGATE"])});
        _mapping.insert({POpType::COLLATE, std::stoi(config["COLLATE"])});
        _mapping.insert({POpType::FILE_SCAN, std::stoi(config["FILE_SCAN"])});
        _mapping.insert({POpType::FILTER, std::stoi(config["FILTER"])});
        _mapping.insert({POpType::GROUP, std::stoi(config["GROUP"])});
        _mapping.insert({POpType::HASH_JOIN_BUILD, std::stoi(config["HASH_JOIN_BUILD"])});
        _mapping.insert({POpType::HASH_JOIN_PROBE, std::stoi(config["HASH_JOIN_PROBE"])});
        _mapping.insert({POpType::SHUFFLE, std::stoi(config["SHUFFLE"])});
        _mapping.insert({POpType::SORT, std::stoi(config["SORT"])});
        _mapping.insert({POpType::PREDICATE, std::stoi(config["PREDICATE"])});
        _mapping.insert({POpType::NESTED_LOOP_JOIN, std::stoi(config["NESTED_LOOP_JOIN"])});
        _mapping.insert({POpType::LIMIT_SORT, std::stoi(config["LIMIT_SORT"])});
        _mapping.insert({POpType::SPLIT, std::stoi(config["SPLIT"])});
        _mapping.insert({POpType::PROJECT, std::stoi(config["PROJECT"])});
    } else {
        _mapping.insert({POpType::AGGREGATE, -1});
        _mapping.insert({POpType::COLLATE, -1});
        _mapping.insert({POpType::FILE_SCAN, -1});
        _mapping.insert({POpType::FILTER, -1});
        _mapping.insert({POpType::GROUP, -1});
        _mapping.insert({POpType::HASH_JOIN_BUILD, -1});
        _mapping.insert({POpType::HASH_JOIN_PROBE, -1});
        _mapping.insert({POpType::SHUFFLE, -1});
        _mapping.insert({POpType::SORT, -1});
        _mapping.insert({POpType::PREDICATE, -1});
        _mapping.insert({POpType::NESTED_LOOP_JOIN, -1});
        _mapping.insert({POpType::LIMIT_SORT, -1});
        _mapping.insert({POpType::SPLIT, -1});
        _mapping.insert({POpType::PROJECT, -1});
    }
    
}

ModeCtrl::~ModeCtrl() {
    //nop
}

int ModeCtrl::activeAddr(POpType type) {
    auto mode = _mapping.find(type);
    if (mode != _mapping.end()) {
        return mode->second;
    } else {
        return -1;
    }
}

}