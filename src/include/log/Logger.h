/*
 * Log.h
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

#include "spdlog/spdlog.h"

#ifndef LOG_LOGGER_H_
#define LOG_LOGGER_H_

#define LOGTRACE(msg, ...)     LOGGER->trace("[{}:{}:{}]-" msg, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
#define LOGDEBUG(msg, ...)     LOGGER->debug("[{}:{}:{}]-" msg, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
#define LOGINFO(msg, ...)      LOGGER->info("[{}:{}:{}]-" msg, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
#define LOGWARN(msg, ...)      LOGGER->warn("[{}:{}:{}]-" msg, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
#define LOGERROR(msg, ...)     LOGGER->error("[{}:{}:{}]-" msg, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
#define LOGCRITICAL(msg, ...)  LOGGER->critical("[{}:{}:{}]-" msg, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)

namespace xdi {

static size_t queue_size = 8192;

static std::once_flag initLogger;

inline static std::shared_ptr<spdlog::logger> createLogger(std::string const& name) {
    std::call_once(initLogger, [] () mutable
    {
        spdlog::set_async_mode(queue_size, spdlog::async_overflow_policy::block_retry, nullptr, std::chrono::seconds(2));
        spdlog::set_level(spdlog::level::debug);
    });
    std::string path = "/fds/var/logs/" + name + "log";
    return spdlog::rotating_logger_mt(name, path, 1048576*5, 3);
}

extern std::shared_ptr<spdlog::logger> GetLogger();

} // namespace xdi

#endif // LOG_LOGGER_H_
