//
// Created by czk on 09/04/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MODECTRL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MODECTRL_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <unordered_map>
#include <string>

using namespace std;
using namespace fpdb::executor::physical;

namespace fpdb::executor {
    /*
    Control which ops can be pushdown.
    */
class ModeCtrl {
public:
    /*
    True: Controlled by external file.
    */
    ModeCtrl(bool external);

    ~ModeCtrl();

    int activeAddr(POpType type);

private:
    unordered_map<POpType, int> _mapping;
};
}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MODECTRL_H
