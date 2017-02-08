/*
 * scst/ScstDevice.h
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

#ifndef SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTDEVICE_H_
#define SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTDEVICE_H_

#include <atomic>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>

#include <boost/lockfree/queue.hpp>

#include "connector/scst-standalone/ScstCommon.h"
#include "connector/scst-standalone/scst_user.h"

#undef COPY

//#include "common_types.h"

struct scst_user_get_cmd;

namespace fds {
namespace connector {
namespace scst {

struct ScstTarget;
struct ScstTask;
struct InquiryHandler;
struct ModeHandler;

struct ScstDevice {
    ScstDevice(std::string const& device_name,
               ScstTarget* target);
    ScstDevice(ScstDevice const& rhs) = delete;
    ScstDevice(ScstDevice const&& rhs) = delete;
    virtual ~ScstDevice();

    void remove();
    virtual void shutdown() = 0;
    void terminate();

    std::string getName() const { return volumeName; }

    void registerDevice(uint8_t const device_type, uint32_t const logical_block_size);
    void start(std::shared_ptr<ev::dynamic_loop> loop);

  protected:

    template<typename T>
    using unique = std::unique_ptr<T>;

    boost::lockfree::queue<ScstTask*> readyResponses;

    scst_user_get_multi* cmds {nullptr};
    scst_user_get_cmd* cmd {nullptr};

    // Utility functions to build Inquiry Pages...etc
    unique<InquiryHandler> inquiry_handler;
    virtual void setupModePages();

    // Utility functions to build Mode Pages...etc
    unique<ModeHandler> mode_handler;
    virtual void setupInquiryPages(uint64_t const volume_id);

    void devicePoke();

    void fastReply(int32_t const result);

  private:
    /// Constants
    static constexpr uint64_t invalid_session_id {UINT64_MAX};

    std::atomic_bool stopping {false};
    std::atomic_bool removed {false};

    std::string const   volumeName;
    int scstDev {-1};

    ScstTarget* scst_target;
    uint64_t reservation_session_id {invalid_session_id};
    std::set<uint64_t> sessions;

    std::unordered_map<uint32_t, unique<ScstTask>> repliedResponses;

    unique<ev::io> ioWatcher;
    unique<ev::async> asyncWatcher;

    int openScst();
    void wakeupCb(ev::async &watcher, int revents);
    void ioEvent(ev::io &watcher, int revents);
    void getAndRespond();

    void execAllocCmd();
    void execMemFree();
    void execUserCmd();
    void execCompleteCmd();
    void execTaskMgmtCmd();
    void execParseCmd();
    void execSessionCmd();

    virtual void attach() = 0;
    virtual void detach() = 0;
    virtual void execDeviceCmd(ScstTask* task) = 0;
    virtual void execDeviceRemap() = 0;
};

}  // namespace scst
}  // namespace connector
}  // namespace fds

#endif  // SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTDEVICE_H_
