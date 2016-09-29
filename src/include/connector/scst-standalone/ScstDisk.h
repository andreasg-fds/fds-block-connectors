/*
 * scst/ScstDisk.h
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

#ifndef SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTDISK_H_
#define SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTDISK_H_

#include <memory>

#include "connector/scst-standalone/ScstDevice.h"
#include "connector/block/BlockOperations.h"

namespace xdi {
class IscsiVolumeDescriptor;
}

namespace fds {
namespace connector {
namespace scst {

struct ScstTarget;
struct ScstTask;

struct ScstDisk : public ScstDevice,
                  public fds::block::BlockOperations {
    using volume_ptr = std::shared_ptr<xdi::IscsiVolumeDescriptor>;

    ScstDisk(volume_ptr& vol_desc, ScstTarget* target, std::shared_ptr<xdi::ApiInterface> api);
    ScstDisk(ScstDisk const& rhs) = delete;
    ScstDisk(ScstDisk const&& rhs) = delete;
    ScstDisk operator=(ScstDisk const& rhs) = delete;
    ScstDisk operator=(ScstDisk const&& rhs) = delete;

    // implementation of ScstDevice
    void shutdown() override { fds::block::BlockOperations::shutdown(); }

    // implementation of BlockOperations
    void respondTask(fds::block::BlockTask* response) override;

  private:
    size_t volume_size {0};
    uint64_t volume_id {0};
    uint32_t logical_block_size {512};
    uint32_t physical_block_size {0};

    void setupModePages() override;
    void setupInquiryPages(uint64_t const volume_id) override;

    // Some specific command handlers
    void read_capacity(ScstTask* task) const;

    void attach() override;
    void detach() override;
    void execDeviceCmd(ScstTask* task) override;
    void execDeviceRemap() override;
};

}  // namespace scst
}  // namespace connector
}  // namespace fds

#endif  // SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTDISK_H_
