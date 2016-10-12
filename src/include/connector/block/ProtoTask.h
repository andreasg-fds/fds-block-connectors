/*
 * ProtoTask.h
 *
 * Copyright (c) 2016, Andreas Griesshammer <andreas@formationds.com>
 * Copyright (c) 2016, Formation Data Systems
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
 * REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
 * INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
 * LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
 * OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
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
