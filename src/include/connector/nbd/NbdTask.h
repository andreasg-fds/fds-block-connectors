/*
 * Copyright 2016 Formation Data Systems, Inc.
 */

#ifndef NBDTASK_H_
#define NBDTASK_H_

// FDS includes
#include "connector/block/ProtoTask.h"

namespace fds {
namespace connector {
namespace nbd {

struct NbdTask : public fds::block::ProtoTask {
    using buffer_type = std::string;
    using buffer_ptr_type = std::shared_ptr<buffer_type>;

    NbdTask(uint64_t const hdl);
    ~NbdTask() = default;

    bool isRead() { return readTask; }
    void setRead() { readTask = true; }

    /// Buffer operations
    buffer_ptr_type getNextReadBuffer(uint32_t& context) {
        if (context >= bufVec.size()) {
            return nullptr;
        }
        return bufVec[context++];
    }

    std::vector<buffer_ptr_type>& getBufVec() { return bufVec; }

private:
    bool    readTask{false};

    std::vector<buffer_ptr_type>   bufVec;
};

}  // namespace scst
}  // namespace connector
}  // namespace nbd

#endif //NBDTASK_H_
