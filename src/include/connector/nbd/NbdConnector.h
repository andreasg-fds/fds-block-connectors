/*
 * NbdConnector.h
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

#ifndef SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_NBD_NBDCONNECTOR_H_
#define SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_NBD_NBDCONNECTOR_H_

#include <memory>
#include <mutex>

#include "connector/nbd/common.h"

#include "xdi/ApiResponseInterface.h"

namespace xdi
{
class IscsiVolumeDescriptor;
}  // namespace xdi

namespace fds {
namespace connector {
namespace nbd {

struct NbdConnection;

struct NbdConnector : public xdi::ApiResponseInterface
{
    using xdi_handle = xdi::RequestHandle;
    using xdi_error = xdi::ApiErrorCode;
    using volume_ptr = std::shared_ptr<xdi::IscsiVolumeDescriptor>;

    NbdConnector(NbdConnector const& rhs) = delete;
    NbdConnector& operator=(NbdConnector const& rhs) = delete;
    ~NbdConnector() = default;

    static void start(std::shared_ptr<xdi::ApiInterface> api);
    static void shutdown();

    void deviceDone(int const socket);
    volume_ptr lookupVolume(std::string const& volume_name);

    void listResp(xdi_handle const& requestId, xdi::ListBlobsResponse const& resp, xdi_error const& e) override {};
    void readVolumeMetaResp(xdi_handle const& requestId, xdi::VolumeMetadata const& metadata, xdi_error const& e) override { }
    void writeVolumeMetaResp(xdi_handle const& requestId, xdi_error const& e) override { }
    void readBlobResp(xdi_handle const& requestId, xdi::ReadBlobResponse const& resp, xdi_error const& e) override {};
    void writeBlobResp(xdi_handle const& requestId, xdi::WriteBlobResponse const& resp, xdi_error const& e) override {};
    void upsertBlobMetadataCasResp(xdi_handle const& requestId, bool const& resp, xdi_error const& e) override {};
    void upsertBlobObjectCasResp(xdi_handle const& requestId, bool const& resp, xdi_error const& e) override {};
    void readObjectResp(xdi_handle const& requestId, xdi::BufferPtr const& resp, xdi_error const& e) override {};
    void writeObjectResp(xdi_handle const& requestId, xdi::ObjectId const& resp, xdi_error const& e) override {};
    void deleteBlobResp(xdi_handle const& requestId, bool const& resp, xdi_error const& e) override {};
    void diffBlobResp(xdi_handle const& requestId, xdi::DiffBlobResponse const& resp, xdi_error const& e) override {};
    void diffAllBlobsResp(xdi_handle const& requestId, xdi::DiffAllBlobsResponse const& resp, xdi_error const& e) override {};
    void diffVolumesResp(xdi_handle const& requestId, xdi::DiffVolumesResponse const& resp, xdi_error const& e) override {};
    void statVolumeResp(xdi_handle const& requestId, xdi::VolumeStatusPtr const& resp, xdi_error const& e) override {};
    void listAllVolumesResp(xdi_handle const& requestId, xdi::ListAllVolumesResponse const& resp, xdi_error const& e) override;

 protected:
    void lead();

 private:
    uint32_t nbdPort {10809};
    int32_t nbdSocket {-1};
    bool cfg_no_delay {true};
    uint32_t cfg_keep_alive {30};

    template<typename T>
    using shared = std::shared_ptr<T>;
    using connection_ptr = shared<NbdConnection>;

    using conn_map_type = std::map<int, connection_ptr>;
    using vol_map_type = std::map<std::string, volume_ptr>;

    static std::shared_ptr<NbdConnector> instance_;

    bool stopping {false};

    std::mutex connection_lock;
    conn_map_type connection_map;
    vol_map_type volume_id_map;

    std::shared_ptr<ev::dynamic_loop> evLoop;
    std::unique_ptr<ev::io> evIoWatcher;
    std::unique_ptr<ev::async> asyncWatcher;
    std::unique_ptr<ev::timer> volumeRefresher;
    std::shared_ptr<xdi::ApiInterface> api_;

    NbdConnector(std::shared_ptr<xdi::ApiInterface> api);

    int createNbdSocket();
    void configureSocket(int fd) const;
    void discoverTargets();
    void initialize();
    void reset();
    void nbdAcceptCb(ev::io &watcher, int revents);
    void startShutdown();
};

}  // namespace nbd
}  // namespace connector
}  // namespace fds

#endif  // SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_NBD_NBDCONNECTOR_H_
