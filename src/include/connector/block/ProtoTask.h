/*
 * Copyright 2016 Formation Data Systems, Inc.
 */

#ifndef PROTOTASK_H_
#define PROTOTASK_H_

// System includes

// FDS includes
#include "xdi/ApiTypes.h"

namespace fds {
namespace block {

struct ProtoTask {
    ProtoTask(uint64_t const hdl) : handle(hdl) {}
    inline virtual ~ProtoTask() = 0;
    uint64_t getHandle() const  { return handle; }

    void setError(xdi::ApiErrorCode const& error) { opError = error; }
    xdi::ApiErrorCode getError() const      { return opError; }

    int64_t handle;

private:
    // error of the operation
    xdi::ApiErrorCode opError {xdi::ApiErrorCode::XDI_OK};
};

ProtoTask::~ProtoTask() { };

}  // namespace block
}  // namespace fds

#endif // PROTOTASK_H_
