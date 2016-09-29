/*
 * NbdConnection.h
 *
 * Copyright (c) 2016, Brian Szmyd <szmyd@formationds.com>
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

#ifndef SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_NBD_NBDCONNECTION_H_
#define SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_NBD_NBDCONNECTION_H_

#include <array>
#include <atomic>
#include <memory>
#include <string>
#include <utility>
extern "C" {
#include <sys/uio.h>
}
#include <boost/shared_ptr.hpp>
#include <boost/lockfree/queue.hpp>

#include "connector/nbd/common.h"
#include "connector/nbd/NbdTask.h"
#include "connector/block/BlockOperations.h"

namespace fds {
namespace connector {
namespace nbd {

struct NbdConnector;

#pragma pack(push)
#pragma pack(1)
struct attach_header {
    uint8_t magic[8];
    int32_t optSpec, length;
};

struct handshake_header {
    uint32_t ack;
};

struct request_header {
    uint8_t magic[4];
    int32_t opType;
    int64_t handle;
    int64_t offset;
    int32_t length;
};
#pragma pack(pop)

template<typename H, typename D>
struct message {
    typedef D data_type;
    typedef H header_type;
    header_type header;
    ssize_t header_off, data_off;
    data_type data;
};

struct NbdConnection : public fds::block::BlockOperations {
    NbdConnection(NbdConnector* server,
                  std::shared_ptr<ev::dynamic_loop> loop,
                  int clientsd,
                  std::shared_ptr<xdi::ApiInterface> api);
    NbdConnection(NbdConnection const& rhs) = delete;
    NbdConnection(NbdConnection const&& rhs) = delete;
    NbdConnection operator=(NbdConnection const& rhs) = delete;
    NbdConnection operator=(NbdConnection const&& rhs) = delete;
    ~NbdConnection();

    // implementation of BlockOperations::ResponseIFace
    void respondTask(fds::block::BlockTask* response) override;

    void terminate();

  private:
    template<typename T>
    using unique = std::unique_ptr<T>;
    using resp_vector_type = unique<iovec[]>;

    std::atomic_bool stopping {false};

    int clientSocket;
    size_t volume_size;
    size_t object_size;

    NbdConnector* nbd_server;

    message<attach_header, std::array<char, 1024>> attach;
    message<handshake_header, std::nullptr_t> handshake;
    message<request_header, std::shared_ptr<std::string>> request;

    resp_vector_type response;
    size_t total_blocks;
    ssize_t write_offset;

    boost::lockfree::queue<NbdTask*> readyResponses;
    std::unique_ptr<NbdTask> current_response;

    std::unique_ptr<ev::io> ioWatcher;
    std::unique_ptr<ev::async> asyncWatcher;

    /** Indicates to ev loop if it's safe to handle events on this connection */
    bool processing_ {false};

    void wakeupCb(ev::async &watcher, int revents);
    void ioEvent(ev::io &watcher, int revents);

    enum class NbdProtoState {
        INVALID   = 0,
        PREINIT   = 1,
        POSTINIT  = 2,
        AWAITOPTS = 3,
        SENDOPTS  = 4,
        DOREQS    = 5
    };

    NbdProtoState nbd_state;

    std::shared_ptr<xdi::ApiInterface> api_;

    // Handshake State
    bool handshake_start(ev::io &watcher);
    bool handshake_complete(ev::io &watcher);

    // Option Negotiation State
    void option_request(ev::io &watcher);
    bool option_reply(ev::io &watcher);

    // Data IO State
    bool io_request(ev::io &watcher);
    bool io_reply(ev::io &watcher);

    void dispatchOp();
    bool write_response();
};

}  // namespace nbd
}  // namespace connector
}  // namespace fds

#endif  // SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_NBD_NBDCONNECTION_H_
