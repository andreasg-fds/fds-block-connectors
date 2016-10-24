/*
 * NbdConnector.cpp
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

#include "connector/nbd/NbdConnector.h"

#include <set>
#include <string>
#include <thread>

extern "C" {
#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/uio.h>
}

#include <ev++.h>
#include <boost/shared_ptr.hpp>

#include "connector/nbd/NbdConnection.h"
#include "log/Log.h"

namespace xdi {
    fds_log* g_fdslog = new fds_log("NbdConnector", LOG_LOCATION, fds_log::normal);

    fds_log* GetLog() {
        return g_fdslog;
    }
} // namespace xdi

namespace fds {
namespace connector {
namespace nbd {

// The singleton
std::shared_ptr<NbdConnector> NbdConnector::instance_ {nullptr};

void NbdConnector::start(std::shared_ptr<xdi::ApiInterface> api) {
    static std::once_flag init;
    // Initialize the singleton
    std::call_once(init, [api] () mutable
    {
        instance_.reset(new NbdConnector(api));
        // Start the main server thread
        auto t = std::thread(&NbdConnector::lead, instance_.get());
        t.detach();
    });
}

void NbdConnector::shutdown() {
    if (instance_) {
        instance_->startShutdown();
    }
}

NbdConnector::NbdConnector(std::shared_ptr<xdi::ApiInterface> api)
        : api_(api) {
    initialize();
}

void NbdConnector::startShutdown() {
    std::lock_guard<std::mutex> g(connection_lock);
    stopping = true;
    for (auto& connection_pair : connection_map) {
        connection_pair.second->terminate();
    }
    asyncWatcher->send();
}

void NbdConnector::initialize() {
    // TODO(bszmyd): Thu 29 Sep 2016 09:25:58 AM MDT
    // Configure no_delay, keepalive and nbdPort here

    // Shutdown the socket if we are reinitializing
    if (0 <= nbdSocket)
        { reset(); }

    // Bind to NBD listen port
    nbdSocket = createNbdSocket();
    if (nbdSocket < 0) {
        GLOGERROR << "could not bind to NBD port";
        return;
    }

    // Setup event loop
    if (!evLoop && !evIoWatcher) {
        GLOGNORMAL << "port:" << nbdPort << " accepting connections";
        evLoop = std::unique_ptr<ev::dynamic_loop>(new ev::dynamic_loop());
        evIoWatcher = std::unique_ptr<ev::io>(new ev::io());
        if (!evLoop || !evIoWatcher) {
            GLOGERROR << "failed to initialize lib_ev";
            return;
        }
        evIoWatcher->set(*evLoop);
        evIoWatcher->set<NbdConnector, &NbdConnector::nbdAcceptCb>(this);
    }
    evIoWatcher->set(nbdSocket, ev::READ);
    evIoWatcher->start(nbdSocket, ev::READ);

    // This is our async event watcher for shutdown
    if (!asyncWatcher) {
        asyncWatcher = std::unique_ptr<ev::async>(new ev::async());
        asyncWatcher->set(*evLoop);
        asyncWatcher->set<NbdConnector, &NbdConnector::reset>(this);
        asyncWatcher->start();
    }

    if (!volumeRefresher) {
        volumeRefresher = std::unique_ptr<ev::timer>(new ev::timer());
        volumeRefresher->set(*evLoop);
        volumeRefresher->set<NbdConnector, &NbdConnector::discoverTargets>(this);
        volumeRefresher->set(0, 2);
        volumeRefresher->again();
    }
}

void
NbdConnector::deviceDone(int const socket) {
    std::lock_guard<std::mutex> g(connection_lock);
    auto it = connection_map.find(socket);
    if (connection_map.end() == it) return;

    connection_map.erase(it);
}

NbdConnector::volume_ptr
NbdConnector::lookupVolume(std::string const& volume_name) {
    std::lock_guard<std::mutex> g(connection_lock);
    auto it = volume_id_map.find(volume_name);
    if (volume_id_map.end() != it) return it->second;
    throw std::runtime_error("Volume not found");
}

void NbdConnector::reset() {
    if (0 <= nbdSocket) {
        evIoWatcher->stop();
        ::shutdown(nbdSocket, SHUT_RDWR);
        close(nbdSocket);
        nbdSocket = -1;
    }
}

void NbdConnector::configureSocket(int fd) const {
    // Enable Non-Blocking mode
    if (0 > fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK)) {
        GLOGWARN << "failed to set NON-BLOCK on NBD connection";
    }

    // Disable Nagle's algorithm, we do our own Corking
    if (cfg_no_delay) {
        GLOGDEBUG << "disabling Nagle's algorithm";
        int opt_val = 1;
        if (0 > setsockopt(fd, SOL_TCP, TCP_NODELAY, &opt_val, sizeof(opt_val))) {
            GLOGWARN << "failed to set socket NON-BLOCKING on NBD connection";
        }
    }

    // Keep-alive
    // Discover dead peers and prevent network disconnect due to inactivity
    if (0 < cfg_keep_alive) {
        // The number of retry attempts
        static int const ka_probes = 9;
        // The time between retries
        int ka_intvl = (cfg_keep_alive / ka_probes) + 1;

        // Configure timeout
        if (setsockopt(fd, SOL_TCP, TCP_KEEPIDLE, &cfg_keep_alive, sizeof(cfg_keep_alive)) < 0) {
            GLOGWARN << "failed to set KEEPALIVE_IDLE on NBD connection";
        }
        if (setsockopt(fd, SOL_TCP, TCP_KEEPINTVL, &ka_intvl, sizeof(ka_intvl)) < 0) {
            GLOGWARN << "failed to set KEEPALIVE_INTVL on NBD connection";
        }
        if (setsockopt(fd, SOL_TCP, TCP_KEEPCNT, &ka_probes, sizeof(ka_probes)) < 0) {
            GLOGWARN << "failed to set KEEPALIVE_CNT on NBD connection";
        }

        // Enable KEEPALIVE on socket
        int optval = 1;
        if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval)) < 0) {
            GLOGWARN << "failed to set KEEPALIVE on NBD connection";
        }
    }
}

void
NbdConnector::nbdAcceptCb(ev::io &watcher, int revents) {
    if (stopping) return;
    if (EV_ERROR & revents) {
        GLOGERROR << "invalid libev event";
        return;
    }

    int clientsd = 0;
    while (0 <= clientsd) {
        socklen_t client_len = sizeof(sockaddr_in);
        sockaddr_in client_addr;

        // Accept a new NBD client connection
        do {
            clientsd = accept(watcher.fd,
                              (sockaddr *)&client_addr,
                              &client_len);
        } while ((0 > clientsd) && (EINTR == errno));

        if (0 <= clientsd) {
            std::lock_guard<std::mutex> g(connection_lock);
            // Setup some TCP options on the socket
            configureSocket(clientsd);

            // Create a handler for this NBD connection
            // Will delete itself when connection dies
            connection_map[clientsd] = std::make_shared<NbdConnection>(this, evLoop, clientsd, api_);
            GLOGNORMAL << "created client connection";
        } else {
            switch (errno) {
            case ENOTSOCK:
            case EOPNOTSUPP:
            case EINVAL:
            case EBADF:
                // Reinitialize server
                GLOGWARN << "accept error:" << strerror(errno);
                nbdSocket = -1;
                initialize();
                break;
            default:
                break; // Nothing special, no more clients
            }
        }
    };
}

int
NbdConnector::createNbdSocket() {
    sockaddr_in serv_addr;
    memset(&serv_addr, '0', sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(nbdPort);

    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) {
        GLOGERROR << "failed to create NBD socket";
        return listenfd;
    }

    // If we crash this allows us to reuse the socket before it's fully closed
    int optval = 1;
    if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
        GLOGWARN << "failed to set REUSEADDR on NBD socket";
    }

    if (bind(listenfd,
             (sockaddr*)&serv_addr,
             sizeof(serv_addr)) == 0) {
        fcntl(listenfd, F_SETFL, fcntl(listenfd, F_GETFL, 0) | O_NONBLOCK);
        listen(listenfd, 10);
    } else {
        GLOGERROR << "bind to listening socket failed:" << strerror(errno);
        listenfd = -1;
    }

    return listenfd;
}

void
NbdConnector::lead() {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGPIPE);
    if (0 != pthread_sigmask(SIG_BLOCK, &set, nullptr)) {
        GLOGWARN << "failed to enable SIGPIPE mask on NBD server";
    }
    evLoop->run(0);
}

void
NbdConnector::discoverTargets() {
    xdi::RequestHandle requestId{0,0};
    xdi::Request r{requestId, xdi::RequestType::LIST_ALL_VOLUMES_TYPE, this};
    xdi::ListAllVolumesRequest req;
    api_->listAllVolumes(r, req);
}

void
NbdConnector::listAllVolumesResp(xdi::RequestHandle const&, xdi::ListAllVolumesResponse const& resp, xdi::ApiErrorCode const& e) {
    if (xdi::ApiErrorCode::XDI_OK == e) {
        std::lock_guard<std::mutex> g(connection_lock);
        volume_id_map.clear();
        for (auto const& vol : resp.volumes) {
            xdi::VolumeDescriptorVisitor v;
            if ((xdi::VolumeType::ISCSI_VOLUME_TYPE == vol->match(&v))) {
                auto currVol = std::static_pointer_cast<xdi::IscsiVolumeDescriptor>(vol);
                volume_id_map.emplace(std::make_pair(vol->volumeName, currVol));
            }
        }
    }
}

}  // namespace nbd
}  // namespace connector
}  // namespace fds

extern "C" {
    void start(std::shared_ptr<xdi::ApiInterface>* api) {
        fds::connector::nbd::NbdConnector::start(*api);
    }

    void stop() {
        fds::connector::nbd::NbdConnector::shutdown();
    }
}
