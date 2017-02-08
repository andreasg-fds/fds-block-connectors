/*
 * scst/ScstConnector.h
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

#ifndef SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTCONNECTOR_H_
#define SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTCONNECTOR_H_

#include <condition_variable>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "xdi/ApiResponseInterface.h"
#include "connector/scst-standalone/ScstCommon.h"
#include "spdlog/spdlog.h"

namespace xdi
{
class IscsiVolumeDescriptor;
}  // namespace xdi

namespace fds {
namespace connector {
namespace scst {

struct ScstTarget;

struct ScstConnector : public xdi::ApiResponseInterface
{
    using xdi_handle = xdi::RequestHandle;
    using xdi_error = xdi::ApiErrorCode;
    using volume_ptr = std::shared_ptr<xdi::IscsiVolumeDescriptor>;

    ScstConnector(ScstConnector const& rhs) = delete;
    ScstConnector& operator=(ScstConnector const& rhs) = delete;
    ~ScstConnector() = default;

    static void start(std::shared_ptr<xdi::ApiInterface> api);
    static void shutdown();

    std::string targetPrefix() const { return target_prefix; }

    /***
     * Used by the ScstTarget to tell the Connector
     * it is safe to remove
     */
    void targetDone(const std::string target_name);

    void listAllVolumesResp(xdi_handle const& requestId, xdi::ListAllVolumesResponse const& resp, xdi_error const& e) override;
    void listResp(xdi_handle const&, xdi::ListBlobsResponse const&, xdi_error const&) override {};
    void enumBlobsResp(xdi_handle const&, xdi::EnumBlobsResponse const&, xdi_error const&) override {};
    void readVolumeMetaResp(xdi_handle const&, xdi::VolumeMetadata const&, xdi_error const&) override {}
    void writeVolumeMetaResp(xdi_handle const&, bool const&, xdi_error const&) override {}
    void readBlobResp(xdi_handle const&, xdi::ReadBlobResponse const&, xdi_error const&) override {};
    void writeBlobResp(xdi_handle const&, xdi::WriteBlobResponse const&, xdi_error const&) override {};
    void upsertBlobMetadataCasResp(xdi_handle const&, bool const&, xdi_error const&) override {};
    void upsertBlobObjectCasResp(xdi_handle const&, bool const&, xdi_error const&) override {};
    void readObjectResp(xdi_handle const&, xdi::BufferPtr const&, xdi_error const&) override {};
    void writeObjectResp(xdi_handle const&, xdi::ObjectId const&, xdi_error const&) override {};
    void deleteBlobResp(xdi_handle const&, bool const&, xdi_error const&) override {};
    void diffBlobResp(xdi_handle const&, xdi::DiffBlobResponse const&, xdi_error const&) override {};
    void diffAllBlobsResp(xdi_handle const&, xdi::DiffAllBlobsResponse const&, xdi_error const&) override {};
    void diffVolumesResp(xdi_handle const&, xdi::DiffVolumesResponse const&, xdi_error const&) override {};
    void statVolumeResp(xdi_handle const&, xdi::VolumeStatusPtr const&, xdi_error const&) override {};

 private:
    template<typename T>
    using shared = std::shared_ptr<T>;

    static shared<ScstConnector> instance_;

    bool stopping {false};
    bool getting_list {false};

    std::mutex target_lock_;
    std::condition_variable listing_condition_;
    std::condition_variable stopping_condition_;
    std::condition_variable done_condition_;
    std::map<volume_ptr, std::unique_ptr<ScstTarget>> targets_;
    std::set<xdi::VolumeId> black_listed_vols;
    std::list<volume_ptr> list_response_;

    ScstConnector(std::string const& prefix,
                  size_t const queue_depth,
                  std::shared_ptr<xdi::ApiInterface> api);

    std::shared_ptr<xdi::ApiInterface> api_;

    std::string target_prefix;
    size_t queue_depth {0};

    bool addTarget(volume_ptr const& volDesc);
    void discoverTargets();
    void terminate();
};

}  // namespace scst
}  // namespace connector
}  // namespace fds

#endif  // SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTCONNECTOR_H_
