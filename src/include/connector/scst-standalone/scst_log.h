/*
 * scst_log.h
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

#include "log/Logger.h"

#ifndef SCST_SCSTLOG_H_
#define SCST_SCSTLOG_H_

namespace xdi {

static std::shared_ptr<spdlog::logger> scst_logger_;

inline static void SetScstLogger(std::shared_ptr<spdlog::logger> logger) {
    scst_logger_ = logger;
}

inline std::shared_ptr<spdlog::logger> GetLogger() {
    return scst_logger_;
}

}  // namespace xdi

#endif // SCST_SCSTLOG_H_
