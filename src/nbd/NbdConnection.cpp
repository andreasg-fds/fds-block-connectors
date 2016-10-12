/*
 * NbdConnection.cpp
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

#include "connector/nbd/NbdConnection.h"

#include <cerrno>
#include <cstdlib>
#include <string>
#include <type_traits>

extern "C" {
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/uio.h>
}

#include <ev++.h>

#include "connector/block/Tasks.h"
#include "connector/nbd/NbdConnector.h"
#include "xdi/ApiResponseInterface.h"
#include "log/Log.h"


/// These constants come from the Nbd Protocol
/// ******************************************
static constexpr uint8_t NBD_MAGIC[]    = { 0x49, 0x48, 0x41, 0x56, 0x45, 0x4F, 0x50, 0x54 };
static constexpr char    NBD_MAGIC_PWD[]  {'N', 'B', 'D', 'M', 'A', 'G', 'I', 'C'};  // NOLINT
static constexpr uint8_t NBD_REQUEST_MAGIC[]    = { 0x25, 0x60, 0x95, 0x13 };
static constexpr uint8_t NBD_RESPONSE_MAGIC[]   = { 0x67, 0x44, 0x66, 0x98 };
static constexpr uint8_t NBD_PROTO_VERSION[]    = { 0x00, 0x01 };
static constexpr int32_t NBD_OPT_EXPORT         = 1;
static constexpr int16_t NBD_FLAG_HAS_FLAGS     = 0b000001;
static constexpr int16_t NBD_FLAG_READ_ONLY     = 0b000010;
static constexpr int16_t NBD_FLAG_SEND_FLUSH    = 0b000100;
static constexpr int16_t NBD_FLAG_SEND_FUA      = 0b001000;
static constexpr int16_t NBD_FLAG_ROTATIONAL    = 0b010000;
static constexpr int16_t NBD_FLAG_SEND_TRIM     = 0b100000;
static constexpr int32_t NBD_CMD_READ           = 0;
static constexpr int32_t NBD_CMD_WRITE          = 1;
static constexpr int32_t NBD_CMD_DISC           = 2;
static constexpr int32_t NBD_CMD_FLUSH          = 3;
static constexpr int32_t NBD_CMD_TRIM           = 4;
/// ******************************************


/// Some useful constants for us
/// ******************************************
static constexpr size_t Ki = 1024;
static constexpr size_t Mi = Ki * Ki;
static constexpr size_t Gi = Ki * Mi;
static constexpr ssize_t max_block_size = 8 * Mi;
/// ******************************************

template<typename T>
constexpr auto to_iovec(T* t) -> typename std::remove_cv<T>::type*
{ return const_cast<typename std::remove_cv<T>::type*>(t); }

static constexpr bool ensure(bool b)
{ return (!b ? throw fds::block::BlockError::connection_closed : true); }

static std::array<std::string, 5> const io_to_string = {
    { "READ", "WRITE", "DISCONNECT", "FLUSH", "TRIM" }
};

static std::array<std::string, 6> const state_to_string = {
    { "INVALID", "PREINIT", "POSTINIT", "AWAITOPTS", "SENDOPTS", "DOREQS" }
};

namespace fds {
namespace connector {
namespace nbd {

template<typename M>
bool get_message_header(int fd, M& message);

template<typename M>
bool get_message_payload(int fd, M& message);

NbdConnection::NbdConnection(NbdConnector* server,
                             std::shared_ptr<ev::dynamic_loop> loop,
                             int clientsd,
                             std::shared_ptr<xdi::ApiInterface> api)
        : fds::block::BlockOperations(api),
          nbd_server(server),
          clientSocket(clientsd),
          volume_size{0},
          object_size{0},
          nbd_state(NbdProtoState::PREINIT),
          handshake({ { 0x01u },  0x00ull, 0x00ull, nullptr }),
          response(nullptr),
          total_blocks(0ull),
          write_offset(-1ll),
          readyResponses(4000),
          current_response(nullptr)
{
    memset(&attach, '\0', sizeof(attach));
    memset(&request, '\0', sizeof(request));

    ioWatcher = std::unique_ptr<ev::io>(new ev::io());
    ioWatcher->set(*loop);
    ioWatcher->set<NbdConnection, &NbdConnection::ioEvent>(this);
    ioWatcher->start(clientSocket, ev::READ | ev::WRITE);

    asyncWatcher = std::unique_ptr<ev::async>(new ev::async());
    asyncWatcher->set(*loop);
    asyncWatcher->set<NbdConnection, &NbdConnection::wakeupCb>(this);
    asyncWatcher->start();

    GLOGNORMAL << "socket:" << clientSocket << " new NBD client";
}

NbdConnection::~NbdConnection() {
    GLOGNORMAL << "socket:" << clientSocket << " NBD client disconnected";
    asyncWatcher->stop();
    ioWatcher->stop();
    ::shutdown(clientSocket, SHUT_RDWR);
    close(clientSocket);
}

void
NbdConnection::terminate() {
    stopping = true;
    asyncWatcher->send();
}

bool
NbdConnection::write_response() {
    static_assert(EAGAIN == EWOULDBLOCK, "EAGAIN != EWOULDBLOCK");
    assert(response);
    assert(total_blocks <= IOV_MAX);

    // Figure out which block we left off on
    size_t current_block = 0ull;
    iovec old_block = response[0];
    if (write_offset > 0) {
        size_t written = write_offset;

        while (written >= response[current_block].iov_len)
            written -= response[current_block++].iov_len;

        // Adjust the block so we don't re-write the same data
        old_block = response[current_block];
        response[current_block].iov_base =
            reinterpret_cast<uint8_t*>(response[current_block].iov_base) + written;
        response[current_block].iov_len -= written;
    }

    // Do the write
    ssize_t nwritten = 0;
    do {
        nwritten = writev(ioWatcher->fd,
                          response.get() + current_block,
                          total_blocks - current_block);
    } while ((0 > nwritten) && (EINTR == errno));

    // Calculate amount we should have written so we can verify the return value
    ssize_t to_write = response[current_block].iov_len;
    // Return the block back to original in case we have to repeat
    response[current_block++] = old_block;
    for (; current_block < total_blocks; ++current_block){
        to_write += response[current_block].iov_len;
    }

    // Check return value
    if (to_write != nwritten) {
        if (nwritten < 0) {
            if (EAGAIN != errno) {
                GLOGERROR << "socket write error:" << strerror(errno);
                throw fds::block::BlockError::connection_closed;
            }
        } else {
            // Didn't write all the data yet, update offset
            write_offset += nwritten;
        }
        return false;
    }
    total_blocks = 0;
    write_offset = -1;
    return true;
}

// Send initial message to client with NBD magic and proto version
bool NbdConnection::handshake_start(ev::io &watcher) {
    // Vector always starts from this state
    static iovec const vectors[] = {
        { to_iovec(NBD_MAGIC_PWD),       sizeof(NBD_MAGIC_PWD)      },
        { to_iovec(NBD_MAGIC),           sizeof(NBD_MAGIC)          },
        { to_iovec(NBD_PROTO_VERSION),   sizeof(NBD_PROTO_VERSION)  },
    };

    if (!response) {
        // First pass (fingers crossed, the only), std::default_deleter
        // is good enough for a unique_ptr (no custom deleter needed).
        write_offset = 0;
        total_blocks = std::extent<decltype(vectors)>::value;
        response = resp_vector_type(new iovec[total_blocks]);
        memcpy(response.get(), vectors, sizeof(vectors));
    }

    // Try and write the response, if it fails to write ALL
    // the data we'll continue later
    if (!write_response()) {
        return false;
    }

    response.reset();
    return true;
}

bool NbdConnection::handshake_complete(ev::io &watcher) {
    if (!get_message_header(watcher.fd, handshake))
        return false;
    ensure(0 == handshake.header.ack);
    return true;
}

void NbdConnection::option_request(ev::io &watcher) {
    if (attach.header_off >= 0) {
        if (!get_message_header(watcher.fd, attach))
            return;
        ensure(0 == memcmp(NBD_MAGIC, attach.header.magic, sizeof(NBD_MAGIC)));
        attach.header.optSpec = ntohl(attach.header.optSpec);
        ensure(NBD_OPT_EXPORT == attach.header.optSpec);
        attach.header.length = ntohl(attach.header.length);

        // Just for sanities sake and protect against bad data
        ensure(attach.data.size() >= static_cast<size_t>(attach.header.length));
    }
    if (!get_message_payload(watcher.fd, attach))
        return;

    // In case volume name is not NULL terminated.
    auto volumeName = std::string(attach.data.begin(),
                                  attach.data.begin() + attach.header.length);
    try {
        auto vol_desc = nbd_server->lookupVolume(volumeName);
        object_size = vol_desc->maxObjectSize;
        volume_size = __builtin_bswap64(vol_desc->capacity * Mi);
        GLOGNORMAL << "vol:" << volumeName
                  << " capacity:" << volume_size
                  << " objsize:" << object_size
                  << " attached to volume";
        nbd_state = NbdProtoState::SENDOPTS;
        init(volumeName, vol_desc->volumeId, object_size);
    } catch (std::runtime_error& e) {
        GLOGNOTIFY << "Could not attach to:" << volumeName
                  << " error:" << e.what();
        throw fds::block::BlockError::connection_closed;
    }
    asyncWatcher->send();
}

bool
NbdConnection::option_reply(ev::io &watcher) {
    static char const zeros[124]{0};  // NOLINT
    static int16_t const optFlags =
        ntohs(NBD_FLAG_HAS_FLAGS);
    static iovec const vectors[] = {
        { nullptr,             sizeof(volume_size) },
        { to_iovec(&optFlags), sizeof(optFlags)    },
        { to_iovec(zeros),     sizeof(zeros)       },
    };

    // Verify we got a valid volume size from attach
    if (0 == volume_size) {
        throw fds::block::BlockError::connection_closed;
    }

    if (!response) {
        write_offset = 0;
        total_blocks = std::extent<decltype(vectors)>::value;
        response = resp_vector_type(new iovec[total_blocks]);
        memcpy(response.get(), vectors, sizeof(vectors));
        response[0].iov_base = &volume_size;
    }

    // Try and write the response, if it fails to write ALL
    // the data we'll continue later
    if (!write_response()) {
        return false;
    }

    response.reset();
    return true;
}

bool NbdConnection::io_request(ev::io &watcher) {
    if (request.header_off >= 0) {
        if (!get_message_header(watcher.fd, request))
            return false;
        ensure(0 == memcmp(NBD_REQUEST_MAGIC, request.header.magic, sizeof(NBD_REQUEST_MAGIC)));
        request.header.opType = ntohl(request.header.opType);
        request.header.offset = __builtin_bswap64(request.header.offset);
        request.header.length = ntohl(request.header.length);

        if (max_block_size < request.header.length) {
            GLOGWARN << "blocksize:" << request.header.length
                    << " maxblocksize:" << max_block_size
                    << " client used larger blocksize than supported";
            throw fds::block::BlockError::shutdown_requested;
        }

        // Construct Buffer for Write Payload
        if (NBD_CMD_WRITE == request.header.opType) {
            request.data = std::make_shared<std::string>(request.header.length, '\0');
        }
    }

    if (NBD_CMD_WRITE == request.header.opType) {
        if (!get_message_payload(watcher.fd, request))
            return false;
    }
    request.header_off = 0;
    request.data_off = -1;

    GLOGIO << "op:" << io_to_string[request.header.opType]
          << " handle:" << request.header.handle
          << " offset:" << request.header.offset
          << " length:" << request.header.length;

    dispatchOp();
    request.data.reset();
    return true;
}

bool
NbdConnection::io_reply(ev::io &watcher) {
    static int32_t const error_ok = htonl(0);
    static int32_t const error_bad = htonl(-1);

    // We can reuse this from now on since we don't go to any state from here
    if (!response) {
        response = resp_vector_type(new iovec[(max_block_size / object_size) + 3]);
        response[0].iov_base = to_iovec(NBD_RESPONSE_MAGIC);
        response[0].iov_len = sizeof(NBD_RESPONSE_MAGIC);
        response[1].iov_base = to_iovec(&error_ok); response[1].iov_len = sizeof(error_ok);
        response[2].iov_base = nullptr; response[2].iov_len = 0;
        response[3].iov_base = nullptr; response[3].iov_len = 0ull;
    }

    if (write_offset == -1) {
        if (readyResponses.empty())
            { return false; }

        total_blocks = 3;

        NbdTask* resp = nullptr;
        ensure(readyResponses.pop(resp));
        current_response.reset(resp);

        response[2].iov_base = &current_response->handle;
        response[2].iov_len = sizeof(current_response->handle);
        response[1].iov_base = to_iovec(&error_ok);
        xdi::ApiErrorCode err = current_response->getError();
        if (xdi::ApiErrorCode::XDI_OK != err) {
            response[1].iov_base = to_iovec(&error_bad);
            GLOGERROR << "returning error:" << static_cast<std::underlying_type<xdi::ApiErrorCode>::type>(err);
        } else if (true == current_response->isRead()) {
            uint32_t context = 0;
            auto buf = current_response->getNextReadBuffer(context);
            while (buf != NULL) {
                GLOGDEBUG << "handle:" << current_response->handle
                         << " size:" << buf->length()
                         << " buffer:" << context;
                response[total_blocks].iov_base = to_iovec(buf->c_str());
                response[total_blocks].iov_len = buf->length();
                ++total_blocks;
                // get next buffer
                buf = current_response->getNextReadBuffer(context);
            }
        }
        write_offset = 0;
    }
    assert(response[2].iov_base != nullptr);
    // Try and write the response, if it fails to write ALL
    // the data we'll continue later
    if (!write_response()) {
        return false;
    }
    response[2].iov_base = nullptr;
    current_response.reset();

    return true;
}

void
NbdConnection::dispatchOp() {
    auto& handle = request.header.handle;
    auto& offset = request.header.offset;
    auto& length = request.header.length;

    switch (request.header.opType) {
        case NBD_CMD_READ:
            {
                auto ptask = new NbdTask(handle);
                auto task = new fds::block::ReadTask(ptask);
                task->set(offset, length);
                executeTask(task);
            }
            break;
        case NBD_CMD_WRITE:
            {
                assert(request.data);
                auto ptask = new NbdTask(handle);
                auto task = new fds::block::WriteTask(ptask);
                task->set(offset, length);
                task->setWriteBuffer(request.data);
                executeTask(task);
            }
            break;
        case NBD_CMD_FLUSH:
            break;
        case NBD_CMD_DISC:
            GLOGNORMAL << "got disconnect";
        default:
            throw fds::block::BlockError::shutdown_requested;
            break;
    }
}

void
NbdConnection::wakeupCb(ev::async &watcher, int revents) {
    if (stopping) {
        shutdown();
    }

    // It's ok to keep writing responses if we've started shutdown
    if (!readyResponses.empty()) {
        ioEvent(*ioWatcher, ev::WRITE);
    }

    if (stopping) {
        nbd_server->deviceDone(clientSocket);
    } else {
        auto writting = (nbd_state == NbdProtoState::SENDOPTS ||
                         current_response ||
                         !readyResponses.empty()) ? ev::WRITE : ev::NONE;

        ioWatcher->set(writting | ev::READ);
        ioWatcher->start();
    }
}

void
NbdConnection::ioEvent(ev::io &watcher, int revents) {
    if (EV_ERROR & revents) {
        GLOGERROR << "invalid libev event";
        return;
    }

    ioWatcher->stop();
    try {
    if (revents & EV_READ) {
        switch (nbd_state) {
            case NbdProtoState::POSTINIT:
                if (handshake_complete(watcher))
                    { nbd_state = NbdProtoState::AWAITOPTS; }
                break;
            case NbdProtoState::AWAITOPTS:
                option_request(watcher);
                break;
            case NbdProtoState::DOREQS:
                // Read all the requests off the socket
                while (io_request(watcher))
                    { continue; }
                break;
            default:
                GLOGDEBUG << "asked to read in state:"
                         << state_to_string[static_cast<uint32_t>(nbd_state)];
                // We could have read and writes waiting and are not in the
                // correct state to handle more requests...yet
                break;
        }
    }

    if (revents & EV_WRITE) {
        switch (nbd_state) {
            case NbdProtoState::PREINIT:
                if (handshake_start(watcher)) {
                    nbd_state = NbdProtoState::POSTINIT;
                }
                break;
            case NbdProtoState::SENDOPTS:
                if (option_reply(watcher)) {
                    nbd_state = NbdProtoState::DOREQS;
                    GLOGDEBUG << "done with NBD handshake";
                }
                break;
            case NbdProtoState::DOREQS:
                // Write everything we can
                while (io_reply(watcher))
                    { continue; }
                break;
            default:
                abort();
        }
    }
    } catch(fds::block::BlockError const& e) {
        stopping = true;
    }
    asyncWatcher->send();
}

void
NbdConnection::respondTask(fds::block::BlockTask* response) {
    auto task = static_cast<NbdTask*>(response->getProtoTask());
    fds::block::TaskVisitor v;
    if (fds::block::TaskType::READ == response->match(&v)) {
        auto btask = static_cast<fds::block::ReadTask*>(response);
        task->setRead();
        btask->swapReadBuffers(task->getBufVec());
    }
    // add to queue
    readyResponses.push(task);

    // We have something to write, so poke the loop
    asyncWatcher->send();
}

ssize_t retry_read(int fd, void* buf, size_t count) {
    ssize_t e = 0;
    do {
        e = read(fd, buf, count);
    } while ((0 > e) && (EINTR == errno));
    return e;
}

template<typename M>
ssize_t read_from_socket(int fd, M& buffer, ssize_t off, ssize_t len);

template<>
ssize_t read_from_socket(int fd, uint8_t*& buffer, ssize_t off, ssize_t len)
{ return retry_read(fd, buffer + off, len); }

template<>
ssize_t read_from_socket(int fd, std::array<char, 1024>& buffer, ssize_t off, ssize_t len)
{ return retry_read(fd, buffer.data() + off, len); }

template<>
ssize_t read_from_socket(int fd, std::shared_ptr<std::string>& buffer, ssize_t off, ssize_t len)
{ return retry_read(fd, &(*buffer)[0] + off, len); }

template<typename D>
bool nbd_read(int fd, D& data, ssize_t& off, ssize_t const len)
{
    static_assert(EAGAIN == EWOULDBLOCK, "EAGAIN != EWOULDBLOCK");
    // If we've nothing to read, done
    assert(0 != len); // Know about this in DEBUG...logic error?
    if (0 == len) return true;

    ssize_t nread = read_from_socket(fd, data, off, len);
    if (0 > nread) {
        switch (0 > nread ? errno : EPIPE) {
            case EAGAIN:
                GLOGTRACE << "payload not there";
                return false; // We were optimistic the payload was ready.
            case EPIPE:
                GLOGNOTIFY << "client disconnected";
            default:
                GLOGERROR << "socket read error:" << strerror(errno);
                throw fds::block::BlockError::shutdown_requested;
        }
    } else if (0 == nread) {
        // Orderly shutdown of the TCP connection
        GLOGNORMAL << "client disconnected";
        throw fds::block::BlockError::connection_closed;
    } else if (nread < len) {
        // Only received some of the data so far
        off += nread;
        return false;
    }
    return true;
}

template<typename M>
bool get_message_header(int fd, M& message) {
    assert(message.header_off >= 0);
    ssize_t to_read = sizeof(typename M::header_type) - message.header_off;

    auto buffer = reinterpret_cast<uint8_t*>(&message.header);
    if (nbd_read(fd, buffer, message.header_off, to_read))
    {
        message.header_off = -1;
        message.data_off = 0;
        return true;
    }
    return false;
}

template<typename M>
bool get_message_payload(int fd, M& message) {
    assert(message.data_off >= 0);
    ssize_t to_read = message.header.length - message.data_off;

    return nbd_read(fd, message.data, message.data_off, to_read);
}

}  // namespace nbd
}  // namespace connector
}  // namespace fds
