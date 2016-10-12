/*
 * ScstDisk.cpp
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

#include "connector/scst-standalone/ScstDisk.h"

#include <climits>
#include <algorithm>
#include <string>

extern "C" {
#include <endian.h>
#include <unistd.h>
#include "connector/scst-standalone/scst_user.h"
}

#include <boost/make_shared.hpp>

#include "log/Log.h"

#include "connector/scst-standalone/ScstTask.h"
#include "connector/scst-standalone/ScstInquiry.h"
#include "connector/scst-standalone/ScstMode.h"

#include "connector/block/Tasks.h"

/// Some useful constants for us
/// ******************************************
static constexpr size_t Ki = 1024;
static constexpr size_t Mi = Ki * Ki;
static constexpr size_t Gi = Ki * Mi;
static constexpr ssize_t max_block_size = 8 * Mi;
/// ******************************************

namespace fds {
namespace connector {
namespace scst {

ScstDisk::ScstDisk(volume_ptr& vol_desc,
                   ScstTarget* target,
                   std::shared_ptr<xdi::ApiInterface> api)
        : ScstDevice(vol_desc->volumeName, target),
          fds::block::BlockOperations(api),
          volume_id(vol_desc->volumeId),
          logical_block_size(512ul)
{
    {
        // TODO(bszmyd): Thu 29 Sep 2016 10:14:06 AM MDT
        // This should be configurable
        logical_block_size = logical_block_size;
    }

    // capacity is in MB
    volume_size = (vol_desc->capacity * Mi);
    physical_block_size = vol_desc->maxObjectSize;

    setupModePages();
    setupInquiryPages(vol_desc->volumeId);
    registerDevice(TYPE_DISK, logical_block_size);
}

void ScstDisk::setupModePages()
{
    ScstDevice::setupModePages();
    mode_handler->setBlockDescriptor((volume_size / logical_block_size), logical_block_size);

    CachingModePage caching_page;
    caching_page &= CachingModePage::DiscontinuityNoTrunc;
    caching_page &= CachingModePage::SegmentSize;
    caching_page &= CachingModePage::SegmentSizeInBlocks;
    uint32_t blocks_per_object = physical_block_size / logical_block_size;
    caching_page.setPrefetches(blocks_per_object, blocks_per_object, blocks_per_object, UINT64_MAX);
    mode_handler->addModePage(caching_page);

    ReadWriteRecoveryPage recovery_page;
    recovery_page &= ReadWriteRecoveryPage::IgnoreReadRecovery;
    recovery_page &= ReadWriteRecoveryPage::TerminateOnRecovery;
    recovery_page &= ReadWriteRecoveryPage::CorrectionPrevented;
    mode_handler->addModePage(recovery_page);
}

void
ScstDisk::setupInquiryPages(uint64_t const volume_id) {
    ScstDevice::setupInquiryPages(volume_id);

    BlockLimitsParameters limits_parameters;
    limits_parameters &= BlockLimitsParameters::WSNonZeroSupport;
    limits_parameters &= BlockLimitsParameters::NoUnmapGranAlignSupport;
    limits_parameters.setMaxATSCount(255u); // No maximum
    limits_parameters.setOptTransferGranularity(std::min(getpagesize() / logical_block_size, 1u));
    limits_parameters.setMaxTransferLength(max_block_size);
    limits_parameters.setOptTransferLength(std::max((size_t)physical_block_size, 1 * Mi));
    limits_parameters.setMaxWSCount(256 * Mi / logical_block_size); // 256Mi is from SCST itself
    VPDPage blk_limits_page;
    blk_limits_page.writePage(0xb0, &limits_parameters, sizeof(BlockLimitsParameters));
    inquiry_handler->addVPDPage(blk_limits_page);

    LogicalBlockParameters lb_parameters;
    lb_parameters &= LogicalBlockParameters::UnmapSupport;
    lb_parameters &= LogicalBlockParameters::WriteSameSupport;
    lb_parameters &= LogicalBlockParameters::WriteSame10Support;
    lb_parameters &= LogicalBlockParameters::ReadZerosSupport;
    lb_parameters &= LogicalBlockParameters::NoAnchorSupport;
    lb_parameters &= LogicalBlockParameters::NoDescriptorPresent;
    lb_parameters &= LogicalBlockParameters::ThinlyProvisioned;
    VPDPage lb_provisioning_page;
    lb_provisioning_page.writePage(0xb2, &lb_parameters, sizeof(LogicalBlockParameters));
    inquiry_handler->addVPDPage(lb_provisioning_page);
}

void ScstDisk::attach() {
    try {
    init(getName(), volume_id, physical_block_size);
    } catch (fds::block::BlockError const e) {
        throw ScstError::scst_error;
    }
}

void ScstDisk::detach() {
    detachVolume();
}

void ScstDisk::execDeviceCmd(ScstTask* task) {
    auto& scsi_cmd = cmd->exec_cmd;
    auto& op_code = scsi_cmd.cdb[0];

    // All buffer allocations have already happened
    auto buffer = task->getResponseBuffer();
    size_t buflen = task->getResponseBufferLen();

    // Poor man's goto
    do {
    if (0 == volume_size) {
        task->checkCondition(SCST_LOAD_SENSE(scst_sense_no_medium));
        continue;
    }

    switch (op_code) {
    case FORMAT_UNIT:
        {
            GLOGTRACE << "format unit received";
            bool fmtpinfo = (0x00 != (scsi_cmd.cdb[1] & 0x80));
            bool fmtdata = (0x00 != (scsi_cmd.cdb[1] & 0x10));

            // Mutually exclusive (and not supported)
            if (fmtdata || fmtpinfo) {
                task->checkCondition(SCST_LOAD_SENSE(scst_sense_invalid_field_in_cdb));
                continue;
            }
            // Nothing to do as we don't support data patterns...done!
            break;
        }
    case READ_6:
    case READ_10:
    case READ_12:
    case READ_16:
        {
            // If we are anything but READ_6 read the PR and FUA bits
            bool fua = false;
            uint8_t rdprotect = 0x00;
            if (READ_6 != op_code) {
                rdprotect = (0x07 & (scsi_cmd.cdb[1] >> 5));
                fua = (0x00 != (scsi_cmd.cdb[1] & 0x08));
            }

            GLOGIO << "iotype:read"
                  << " lba:" << scsi_cmd.lba
                  << " length:" << scsi_cmd.bufflen
                  << " fua:" << fua
                  << " pr:" << (uint32_t)rdprotect
                  << " handle:" << cmd->cmd_h;

            // We do not support rdprotect data
            if (0x00 != rdprotect) {
                task->checkCondition(SCST_LOAD_SENSE(scst_sense_invalid_field_in_cdb));
                continue;
            }

            auto readTask = new fds::block::ReadTask(task);

            uint64_t offset = scsi_cmd.lba * logical_block_size;
            readTask->set(offset, scsi_cmd.bufflen);
            try {
                executeTask(readTask);
            } catch (fds::block::BlockError const e) {
                throw ScstError::scst_error;
            }
            return;
        }
        break;
    case READ_CAPACITY:     // READ_CAPACITY(10)
        {
            read_capacity(task);
        }
        break;
    case SERVICE_ACTION_IN_16:
        {
            uint8_t action = (scsi_cmd.cdb[1] & 0x1F);
            switch (action) {
            case SAI_READ_CAPACITY_16:
                {
                    read_capacity(task);
                }
                break;
            default:
                {
                    GLOGIO << "unsupported SAI:" << (uint32_t)action;
                    task->checkCondition(SCST_LOAD_SENSE(scst_sense_invalid_field_in_cdb));
                }
                break;
            }
        }
        break;
    case WRITE_6:
    case WRITE_10:
    case WRITE_12:
    case WRITE_16:
        {
            // If we are anything but READ_6 read the PR and FUA bits
            bool fua = false;
            uint8_t wrprotect = 0x00;
            if (WRITE_6 != op_code) {
                wrprotect = (0x07 & (scsi_cmd.cdb[1] >> 5));
                fua = (0x00 != (scsi_cmd.cdb[1] & 0x08));
            }

            GLOGIO << "iotype:write"
                  << " lba:" << scsi_cmd.lba
                  << " length:" << scsi_cmd.bufflen
                  << " fua:" << fua
                  << " pr:" << (uint32_t)wrprotect
                  << " handle:" << cmd->cmd_h;

            // We do not support wrprotect data
            if (0x00 != wrprotect) {
                task->checkCondition(SCST_LOAD_SENSE(scst_sense_invalid_field_in_cdb));
                continue;
            }

            auto writeTask = new fds::block::WriteTask(task);

            uint64_t offset = scsi_cmd.lba * logical_block_size;
            writeTask->set(offset, scsi_cmd.bufflen);
            // Right now our API expects the data in a shared_ptr :(
            auto write_buffer = std::make_shared<std::string>((char*) buffer, buflen);
            writeTask->setWriteBuffer(write_buffer);
            try {
                executeTask(writeTask);
            } catch (fds::block::BlockError const e) {
                throw ScstError::scst_error;
            }
            return;
        }
        break;
    case WRITE_SAME:
    case WRITE_SAME_16:
        {
           bool unmap = (0 != (scsi_cmd.cdb[1] & 0x08));
           bool ndob = (0 != (scsi_cmd.cdb[1] & 0x01));
           uint32_t lbas = be32toh(*(uint32_t*)(scsi_cmd.cdb + 10));

           GLOGIO << "WriteSame:" << scsi_cmd.lba
                  << " length:" << scsi_cmd.bufflen
                  << " ndob: " << ndob
                  << " unmap:" << unmap
                  << " lbs:" << lbas;
            uint64_t offset = scsi_cmd.lba * logical_block_size;
            auto writeSameTask = new fds::block::WriteSameTask(task);
            writeSameTask->set(offset, scsi_cmd.bufflen * lbas);
            std::shared_ptr<std::string> write_buffer;
            if (true == ndob) {
                if (false == unmap) {
                    task->checkCondition(SCST_LOAD_SENSE(scst_sense_invalid_field_in_cdb));
                    continue;
                } else {
                    write_buffer = std::make_shared<std::string>(buflen, '\0');
                }
            } else {
                write_buffer = std::make_shared<std::string>((char*) buffer, buflen);
            }
            writeSameTask->setWriteBuffer(write_buffer);
            try {
                executeTask(writeSameTask);
            } catch (fds::block::BlockError const e) {
                throw ScstError::scst_error;
            }
            return;
        }
        break;
    case UNMAP:
        {
            bool anchored = (0 != (scsi_cmd.cdb[1] & 0x01));
            auto listLength = be16toh(*(uint16_t*)(scsi_cmd.cdb + 7));
            auto unmap_data_length = be16toh(*(uint16_t*)(buffer));
            auto unmap_block_descriptor_data_length = be16toh(*(uint16_t*)(buffer + 2));
            GLOGIO << "unmap:" << listLength
                   << " unmapdatalength:" << unmap_data_length
                   << " unmapblockdescriptordatalength:" << unmap_block_descriptor_data_length;
            fds::block::UnmapTask::unmap_vec_ptr write_vec(new fds::block::UnmapTask::unmap_vec);
            for (int i = 8; i < 8 + unmap_block_descriptor_data_length && i + 16 <= unmap_block_descriptor_data_length + 8; i += 16) {
                auto unmap_lba = be64toh(*(uint64_t*)(buffer + i));
                auto num_lba = be32toh(*(uint32_t*)(buffer + i + 8));
                fds::block::UnmapTask::UnmapRange range;
                range.offset = unmap_lba * logical_block_size;
                range.length = num_lba * logical_block_size;
                write_vec->push_back(range);
            }
            if (true == write_vec->empty()) return;
            auto unmapTask = new fds::block::UnmapTask(task, std::move(write_vec));
            try {
                executeTask(unmapTask);
            } catch (fds::block::BlockError const e) {
                throw ScstError::scst_error;
            }
            return;
        }
        break;
    default:
        GLOGIO    << "iotype:unsupported"
                 << " opcode:" << (uint32_t)(op_code)
                 << " cdblength:" << scsi_cmd.cdb_len;
        task->checkCondition(SCST_LOAD_SENSE(scst_sense_invalid_opcode));
        break;
    }
    } while (false);
    readyResponses.push(task);
}

void ScstDisk::read_capacity(ScstTask* task) const {
    GLOGTRACE << "iotype:readcapacity";
    auto& scsi_cmd = cmd->exec_cmd;
    auto& op_code = scsi_cmd.cdb[0];

    // All buffer allocations have already happened
    auto buffer = task->getResponseBuffer();
    size_t buflen = task->getResponseBufferLen();
    uint64_t last_lba = (volume_size / logical_block_size) - 1;
    uint32_t blocks_per_object = physical_block_size / logical_block_size;

    memset(buffer, '\0', buflen);
    if (READ_CAPACITY == op_code && 8 >= buflen) {
        *reinterpret_cast<uint32_t*>(&buffer[0]) = htobe32(std::min(last_lba, (uint64_t)UINT_MAX));
        *reinterpret_cast<uint32_t*>(&buffer[4]) = htobe32(logical_block_size);
        task->setResponseLength(8);
    } else if (32 >= buflen) {
        *reinterpret_cast<uint64_t*>(&buffer[0]) = htobe64(last_lba);
        *reinterpret_cast<uint32_t*>(&buffer[8]) = htobe32(logical_block_size);
        // Number of logic blocks per object as a power of 2
        buffer[13] = (uint8_t)__builtin_ctz(blocks_per_object) & 0xFF;
        buffer[14] = 0b11000000; // (thinly provisioned)
        task->setResponseLength(32);
    } else {
        task->checkCondition(SCST_LOAD_SENSE(scst_sense_invalid_field_in_cdb));
    }
}

void ScstDisk::execDeviceRemap() {
    // TODO(bszmyd): Wed 06 Apr 2016 02:04:05 PM MDT
    // Support true thin provisioning MAP/UNMAP
    GLOGIO << "iotype:remap"
          << " src-lba:" << cmd->remap_cmd.data_descr.src_lba
          << " dst-lba:" << cmd->remap_cmd.data_descr.dst_lba
          << " length:" << cmd->remap_cmd.data_descr.data_len;
    auto task = new ScstTask(cmd->cmd_h, cmd->subcode);
    task->setResult(&cmd->remap_cmd.data_descr);
    readyResponses.push(task);
}

void ScstDisk::respondTask(fds::block::BlockTask* response) {
    auto task = static_cast<ScstTask*>(response->getProtoTask());
    auto const& err = response->getProtoTask()->getError();
    fds::block::TaskVisitor v;
    if (xdi::ApiErrorCode::XDI_OK != err) {
        if (xdi::ApiErrorCode::XDI_MISSING_VOLUME == err) {
            // Volume may have been removed, shutdown and destroy target
            GLOGNOTIFY << "lun destroyed";
            task->checkCondition(SCST_LOAD_SENSE(scst_sense_lun_not_supported));
        } else if ((fds::block::TaskType::READ == response->match(&v)) &&
                   (false == isRetryable(err))) {
            auto btask = static_cast<fds::block::ReadTask*>(response);
            GLOGCRITICAL << "iotype:read"
                        << " handle:" << task->getHandle()
                        << " offset:" << btask->getOffset()
                        << " length:" << btask->getLength()
                        << " had critical failure.";
            task->checkCondition(SCST_LOAD_SENSE(scst_sense_read_error));
        } else if (false == isRetryable(err)) {
            auto btask = static_cast<fds::block::WriteTask*>(response);
            GLOGCRITICAL << "iotype:write"
                        << " handle:" << task->getHandle()
                        << " offset:" << btask->getOffset()
                        << " length:" << btask->getLength()
                        << " had critical failure.";
            task->checkCondition(SCST_LOAD_SENSE(scst_sense_write_error));
        } else {
            // Non-catastrophic (retriable) error
            GLOGIO << "iotype:" << (fds::block::TaskType::READ == response->match(&v) ? "read" : "write")
                  << " handle:" << task->getHandle()
                  << " had retriable failure.";
            task->checkCondition(SCST_LOAD_SENSE(scst_sense_internal_failure));
        }
    } else if (fds::block::TaskType::READ == response->match(&v)) {
        auto btask = static_cast<fds::block::ReadTask*>(response);
        auto buffer = task->getResponseBuffer();
        uint32_t i = 0, context = 0;
        std::shared_ptr<std::string> buf = btask->getNextReadBuffer(context);
        while (buf != NULL) {
            memcpy(buffer + i, buf->c_str(), buf->length());
            i += buf->length();
            buf = btask->getNextReadBuffer(context);
        }
        task->setResponseLength(i);
    }

    // add to queue
    readyResponses.push(task);

    // We have something to write, so poke the loop
    devicePoke();
}

}  // namespace scst
}  // namespace connector
}  // namespace fds
