/*
 * ScstTask.cpp
 *
 * Copyright (c) 2015, Brian Szmyd <szmyd@formationds.com>
 * Copyright (c) 2015, Formation Data Systems
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

#include "connector/scst-standalone/ScstTask.h"

extern "C" {
#include <sys/ioctl.h>
}

namespace fds {
namespace connector {
namespace scst {

ScstTask::ScstTask(uint32_t hdl, uint32_t sc) :
        ProtoTask(hdl)
{
    reply.cmd_h = hdl;
    reply.subcode = sc;
    if (SCST_USER_EXEC == sc) {
        reply.exec_reply.reply_type = SCST_EXEC_REPLY_COMPLETED;
        reply.exec_reply.status = GOOD;
    }
}


}  // namespace scst
}  // namespace connector
}  // namespace fds
