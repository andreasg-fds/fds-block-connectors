/*
 * ScstDevice.cpp
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

#include "connector/scst-standalone/ScstDevice.h"

#include <cerrno>
#include <string>
#include <type_traits>

extern "C" {
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
}

#include <ev++.h>

//#include "log/Log.h"

#include "connector/scst-standalone/ScstTarget.h"
extern "C" {
#include "connector/scst-standalone/scst_user.h"
}

#include "connector/scst-standalone/ScstTask.h"
#include "connector/scst-standalone/ScstMode.h"
#include "connector/scst-standalone/ScstInquiry.h"

static uint32_t const ieee_oui = 0x88A084;
static uint8_t const vendor_name[] = { 'F', 'D', 'S', ' ', ' ', ' ', ' ', ' ' };

constexpr uint16_t max_cmd_transfer = 256u;

enum class BlockError : uint8_t {
    connection_closed,
    shutdown_requested,
};

static constexpr bool ensure(bool b)
{ return (!b ? throw BlockError::connection_closed : true); }

namespace fds {
namespace connector {
namespace scst {

ScstDevice::ScstDevice(std::string const&  device_name,
                       ScstTarget*        target)
        : readyResponses(4000),
          inquiry_handler(new InquiryHandler()),
          mode_handler(new ModeHandler()),
          volumeName(device_name),
          scst_target(target)
{ }

void ScstDevice::registerDevice(uint8_t const device_type, uint32_t const logical_block_size) {
    // Open the SCST user device
    if (0 > (scstDev = openScst())) {
        throw ScstError::scst_not_found;
    }

    // REGISTER a device
    scst_user_dev_desc scst_descriptor {
        (unsigned long)DEV_USER_VERSION, // Constant
        (unsigned long)"GPL",       // License string
        device_type,                  // Device type
        1, 0, 0, 0,                 // SGV enabled
        {                           // SCST options
                SCST_USER_PARSE_STANDARD,                   // parse type
                SCST_USER_ON_FREE_CMD_CALL,                 // command on-free type
                SCST_USER_MEM_REUSE_ALL,                    // buffer reuse type
                SCST_USER_PARTIAL_TRANSFERS_NOT_SUPPORTED,  // partial transfer type
                0,                                          // partial transfer length
                SCST_TST_0_SINGLE_TASK_SET,                 // task set sharing
                0,                                          // task mgmt only (on fault)
                SCST_QUEUE_ALG_1_UNRESTRICTED_REORDER,      // maintain consistency in reordering
                SCST_QERR_0_ALL_RESUME,                     // fault does not abort all cmds
                1, 0, 0, 0,                                 // TAS/SWP/DSENSE/ORDER MGMT,
                1                                           // Copy/Unmap support
        },
        logical_block_size,         // Block size
        0,                          // PR cmd Notifications
        {},
        "bare_am",
    };
    snprintf(scst_descriptor.name, SCST_MAX_NAME, "%s", volumeName.c_str());
    auto res = ioctl(scstDev, SCST_USER_REGISTER_DEVICE, &scst_descriptor);
    if (0 > res) {
        //GLOGERROR << "Failed to register the device! [" << strerror(errno) << "]";
        throw ScstError::scst_error;
    }

    //GLOGNORMAL << "New SCST device [" << volumeName
          //     << "] BlockSize[" << logical_block_size << "]";
}

void ScstDevice::setupModePages()
{
    ControlModePage control_page;
    control_page &= ControlModePage::DefaultProtectionDisabled;
    control_page &= ControlModePage::FixedFormatSense;
    control_page &= ControlModePage::UnrestrictedCommandOrdering;
    control_page &= ControlModePage::NoUAOnRelease;
    control_page &= ControlModePage::AbortStatusOnTMF;
    control_page &= ControlModePage::SingleTaskSet;
    mode_handler->addModePage(control_page);
}

void ScstDevice::setupInquiryPages(uint64_t const volume_id)
{
    // Setup the standard inquiry page
    auto inquiry = inquiry_handler->getStandardInquiry();
    inquiry.setVendorId("FDS");
    inquiry.setProductId("FormationOne");
    inquiry.setRevision("BETA");
    inquiry_handler->setStandardInquiry(inquiry);

    // Write the Serial Number (0x80) Page
    char serial_number[17];
    snprintf(serial_number, sizeof(serial_number), "%.16lX", volume_id);
    VPDPage serial_page;
    serial_page.writePage(0x80, serial_number, 16);
    inquiry_handler->addVPDPage(serial_page);

    // Build our device identification descriptors
    DescriptorBuilder builder;
    builder &= VendorSpecificIdentifier(volumeName);
    builder &= T10Designator("FDS");
    builder &= NAADesignator(ieee_oui, volume_id);

    // Write the Device Identification (0x83) Page
    VPDPage device_id_page;
    device_id_page.writePage(0x83, builder.data(), builder.length());
    inquiry_handler->addVPDPage(device_id_page);

    // Write the Extended Inquiry (0x86) Page
    ExtVPDParameters evpd_parameters;
    evpd_parameters &= ExtVPDParameters::HeadAttrSupport;
    evpd_parameters &= ExtVPDParameters::SimpleAttrSupport;
    evpd_parameters &= ExtVPDParameters::OrderedAttrSupport;
    VPDPage evpd_page;
    evpd_page.writePage(0x86, &evpd_parameters, sizeof(ExtVPDParameters));
    inquiry_handler->addVPDPage(evpd_page);
}

void
ScstDevice::devicePoke() {
    asyncWatcher->send();
}

void
ScstDevice::start(std::shared_ptr<ev::dynamic_loop> loop) {
    // Initialize the incoming and outgoing command/response arrays
    cmds = (decltype(cmds))calloc(1, sizeof(*cmds) + (sizeof(scst_user_get_cmd) * max_cmd_transfer));
    cmds->preplies = (unsigned long)calloc(max_cmd_transfer, sizeof(scst_user_reply_cmd));

    ioWatcher = std::unique_ptr<ev::io>(new ev::io());
    ioWatcher->set(*loop);
    ioWatcher->set<ScstDevice, &ScstDevice::ioEvent>(this);
    ioWatcher->start(scstDev, ev::READ);

    asyncWatcher = std::unique_ptr<ev::async>(new ev::async());
    asyncWatcher->set(*loop);
    asyncWatcher->set<ScstDevice, &ScstDevice::wakeupCb>(this);
    asyncWatcher->start();
}

ScstDevice::~ScstDevice() {
    if (0 <= scstDev) {
        close(scstDev);
        scstDev = -1;
    }
    if (cmds) {
        free((void*)cmds->preplies);
        free(cmds);
        cmds = nullptr;
    }
    //GLOGNORMAL << "SCSI device " << volumeName << " stopped.";
}

void
ScstDevice::remove() {
    removed = true;
    terminate();
}

void
ScstDevice::terminate() {
    stopping = true;
    asyncWatcher->send();
}

int
ScstDevice::openScst() {
    int dev = open(DEV_USER_PATH DEV_USER_NAME, O_RDWR | O_NONBLOCK);
    if (0 > dev) {
        //GLOGERROR << "Opening the SCST device failed: " << strerror(errno);
    }
    return dev;
}

void
ScstDevice::wakeupCb(ev::async&, int) {
    if (stopping) {
        shutdown();
        asyncWatcher->stop();
        ioWatcher.reset();
        close(scstDev);
        scstDev = -1;
        scst_target->deviceDone(volumeName, removed);
        return;
    }

    if (!readyResponses.empty()) {
        ioEvent(*ioWatcher, ev::WRITE);
    }
}

void ScstDevice::execAllocCmd() {
    auto& length = cmd->alloc_cmd.alloc_len;
    //GLOGTRACE << "Allocation of [0x" << std::hex << length << std::dec << "] bytes requested.";

    // Allocate a page aligned memory buffer for Scst usage
    void* buffer;
    ensure(0 == posix_memalign((void**)&buffer, sysconf(_SC_PAGESIZE), length));
#ifdef DEBUG
    memset(buffer, '\0', length); // quiet valgrind
#endif
    auto task = new ScstTask(cmd->cmd_h, cmd->subcode);
    task->setResult(buffer);
    readyResponses.push(task);
}

void ScstDevice::execMemFree() {
    //GLOGTRACE << "Deallocation requested.";
    free((void*)cmd->on_cached_mem_free.pbuf);
    fastReply(0); // Setup the reply for the next ioctl
}

void ScstDevice::execCompleteCmd() {
    //GLOGTRACE << "Command complete: [0x" << std::hex << cmd->cmd_h << std::dec << "]";

    auto it = repliedResponses.find(cmd->cmd_h);
    if (repliedResponses.end() != it) {
        if (xdi::ApiErrorCode::XDI_MISSING_VOLUME == it->second->getError()) {
            // Now that we've told the initiator we are unmapped destroy ourself
            throw ScstError::scst_target_destroyed;
        }
        repliedResponses.erase(it);
    }
    if (!cmd->on_free_cmd.buffer_cached && 0 < cmd->on_free_cmd.pbuf) {
        free((void*)cmd->on_free_cmd.pbuf);
    }
    fastReply(0); // Setup the reply for the next ioctl
}

void ScstDevice::execParseCmd() {
    //GLOGWARN << "Need parsing help.";
    assert(0 == 1);
    fastReply(0); // Setup the reply for the next ioctl
}

void ScstDevice::execSessionCmd() {
    auto attaching = (SCST_USER_ATTACH_SESS == cmd->subcode) ? true : false;
    auto& sess = cmd->sess;
    //GLOGNOTIFY << "type:" << (attaching ? "attach" : "detach")
          //     << " handle:" << sess.sess_h
          //     << " initiator:" << sess.initiator_name
          //     << " volume:" << volumeName;

    if (attaching &&
        0 != strcmp(sess.initiator_name, "copy_manager_sess")) {
        if (sessions.empty()) {
            attach();
        }
        sessions.emplace(sess.sess_h);
    } else {
        sessions.erase(sess.sess_h);
        if (sessions.empty()) {
            detach();
        }
    }
    fastReply(0); // Setup the reply for the next ioctl
}

void ScstDevice::execTaskMgmtCmd() {
    auto& tmf_cmd = cmd->tm_cmd;
    auto done = (SCST_USER_TASK_MGMT_DONE == cmd->subcode) ? true : false;
    if (SCST_TARGET_RESET == tmf_cmd.fn) {
        //GLOGNOTIFY << "Target Reset request:"
            //       << " [0x" << std::hex << tmf_cmd.fn << std::dec
            //       << "] on [" << volumeName << "]"
            //       << " " << (done ? "Done." : "Received.");
    } else {
        //GLOGIO << "Task Management request:"
            //   << " [0x" << std::hex << tmf_cmd.fn << std::dec
            //   << "] on [" << volumeName << "]"
            //   << " " << (done ? "Done." : "Received.");
    }

    if (done) {
        // Reset the reservation if we get a target reset
        switch (tmf_cmd.fn) {
        case SCST_TARGET_RESET:
        case SCST_LUN_RESET:
        case SCST_PR_ABORT_ALL:
            {
                reservation_session_id = invalid_session_id;
            }
        default:
            break;;
        }
    }
    fastReply(0); // Setup the reply for the next ioctl
}

void ScstDevice::execUserCmd() {
    auto& scsi_cmd = cmd->exec_cmd;
    auto& op_code = scsi_cmd.cdb[0];
    auto task = new ScstTask(cmd->cmd_h, SCST_USER_EXEC);

    // We may need to allocate a buffer for SCST, if we do and fail we'll need
    // to clean it up rather than expect a release command for it
    auto buffer = (uint8_t*)scsi_cmd.pbuf;
    size_t buflen = scsi_cmd.bufflen;
    if (!buffer && 0 < scsi_cmd.alloc_len) {
        ensure(0 == posix_memalign((void**)&buffer, sysconf(_SC_PAGESIZE), scsi_cmd.alloc_len));
        memset((void*) buffer, 0x00, scsi_cmd.alloc_len);
        task->setResponseBuffer(buffer, buflen, false);
    } else {
        task->setResponseBuffer(buffer, buflen, true);
    }

    // Poor man's goto
    do {
    // These commands do not require a reservation if one exists
    bool ignore_res = false;
    switch (op_code) {
    case INQUIRY:
    case LOG_SENSE:
    case RELEASE:
    case TEST_UNIT_READY:
      ignore_res = true;
    default:
      break;
    }

    // Check current reservation
    if (!ignore_res &&
        invalid_session_id != reservation_session_id &&
        reservation_session_id != scsi_cmd.sess_h) {
        task->reservationConflict();
        continue;
    }

    switch (op_code) {
    case TEST_UNIT_READY:
        //GLOGTRACE << "Test Unit Ready received.";
        break;
    case INQUIRY:
        {
            memset(buffer, '\0', buflen);

            // Check EVPD bit
            if (scsi_cmd.cdb[1] & 0x01) {
                auto& page = scsi_cmd.cdb[2];
                //GLOGTRACE << "Page: [0x" << std::hex << (uint32_t)(page) << std::dec << "] requested.";
                inquiry_handler->writeVPDPage(task, page);
            } else {
                //GLOGTRACE << "Standard inquiry requested.";
                inquiry_handler->writeStandardInquiry(task);
            }
        }
        break;
    case MODE_SENSE:
    case MODE_SENSE_10:
        {
            bool dbd = (0x00 != (scsi_cmd.cdb[1] & 0x08));
            uint8_t pc = scsi_cmd.cdb[2] / 0x40;
            uint8_t page_code = scsi_cmd.cdb[2] % 0x40;
            uint8_t& subpage = scsi_cmd.cdb[3];
            //GLOGTRACE << "Mode Sense: "
               //       << " dbd[" << std::hex << dbd
                //      << "] pc[" << std::hex << (uint32_t)pc << std::dec
                //      << "] page_code[" << (uint32_t)page_code
                //      << "] subpage[" << (uint32_t)subpage << "]";

            // We do not support any persistent pages, subpages
            if (0x01 & pc || 0x00 != subpage) {
                task->checkCondition(SCST_LOAD_SENSE(scst_sense_invalid_field_in_cdb));
                continue;
            }

            memset(buffer, '\0', buflen);

            if (MODE_SENSE == op_code) {
                mode_handler->writeModeParameters6(task, !dbd, page_code);
            } else {
                mode_handler->writeModeParameters10(task, !dbd, page_code);
            }
        }
        break;
    case RESERVE:
        //GLOGIO << "Reserving device [" << volumeName << "]";
        reservation_session_id = scsi_cmd.sess_h;
        break;
    case RELEASE:
        if (reservation_session_id == scsi_cmd.sess_h) {
            //GLOGIO << "Releasing device [" << volumeName << "]";
            reservation_session_id = invalid_session_id;
        } else {
            //GLOGTRACE << "Initiator tried to release [" << volumeName <<"] with no reservation held.";
        }
        break;
    default:
        // See if this is a device specific command
        execDeviceCmd(task);
        return;
    }
    } while (false);
    readyResponses.push(task);
}

void
ScstDevice::getAndRespond() {
    cmds->replies_cnt = 0;
    cmds->replies_done = 0;
    do {
        cmds->cmds_cnt = max_cmd_transfer;
        // Retry any unsent responses
        auto cmd_replies = (scst_user_reply_cmd*)cmds->preplies;
        for (auto i = cmds->replies_done; cmds->replies_cnt > i; ++i) {
           memcpy(cmd_replies + (i - cmds->replies_done),
                  (cmd_replies + i),
                  sizeof(scst_user_reply_cmd));
        }
        cmds->replies_cnt = std::max(0, cmds->replies_cnt - cmds->replies_done);

        while (!readyResponses.empty() && max_cmd_transfer > cmds->replies_cnt) {
            ScstTask* resp {nullptr};
            ensure(readyResponses.pop(resp));
            auto const& reply = *((scst_user_reply_cmd*)resp->getReply());

            memcpy(cmd_replies + cmds->replies_cnt++,
                   &reply,
                   sizeof(scst_user_reply_cmd));
            //GLOGTRACE << "Responding to: "
                 //     << "[0x" << std::hex << reply.cmd_h << std::dec
                 //     << "] sc [0x" << reply.subcode
                 //     << "] result [0x" << reply.result << "]";
            if (SCST_USER_EXEC == reply.subcode) {
                repliedResponses[resp->getHandle()].reset(resp);
            } else {
                delete resp;
            }
        }

        int res = 0;
        do { // Make sure and finish the ioctl
            res = ioctl(scstDev, SCST_USER_REPLY_AND_GET_MULTI, cmds);
        } while ((0 > res) && (EINTR == errno));

        if (0 != res) {
            switch (errno) {
            case ENOTTY:
            case EBADF:
                //GLOGNOTIFY << "Scst device no longer has a valid handle to the kernel, terminating.";
                throw BlockError::connection_closed;
            case EFAULT:
            case EINVAL:
                //GLOGERROR << "vol:" << volumeName << " invalid scst argument";
                throw ScstError::scst_error;
            case EAGAIN:
                continue;
            default:
                return;
            }
        }

        for (auto i = 0; cmds->cmds_cnt > i; ++i) {
            cmd = &(cmds->cmds[i]);
            //GLOGTRACE << "Received SCST command: "
                //      << "[0x" << std::hex << cmd->cmd_h << std::dec
                //      << "] sc [0x" << cmd->subcode << "]";
            switch (cmd->subcode) {
            case SCST_USER_ATTACH_SESS:
            case SCST_USER_DETACH_SESS:
                execSessionCmd();
                break;
            case SCST_USER_ON_FREE_CMD:
                execCompleteCmd();
                break;
            case SCST_USER_TASK_MGMT_RECEIVED:
            case SCST_USER_TASK_MGMT_DONE:
                execTaskMgmtCmd();
                break;
            case SCST_USER_ON_CACHED_MEM_FREE:
                execMemFree();
                break;
            case SCST_USER_ALLOC_MEM:
                execAllocCmd();
                break;
            case SCST_USER_EXT_COPY_REMAP:
                execDeviceRemap();
                break;
            case SCST_USER_PARSE:
                execParseCmd();
                break;
            case SCST_USER_EXEC:
                execUserCmd();
                break;
            default:
                //GLOGWARN << "Received unknown Scst subcommand: [" << cmd->subcode << "]";
                break;
            }
        }
    } while (!readyResponses.empty()
             || (cmds->replies_cnt > cmds->replies_done)
             || (0 > cmds->cmds_cnt)); // Keep replying while we have responses or requests
}

void
ScstDevice::ioEvent(ev::io&, int revents) {
    if (EV_ERROR & revents) {
        //GLOGERROR << "Got invalid libev event";
        return;
    }

    // We are guaranteed to be the only thread acting on this file descriptor
    try {
        // Get the next command, and/or reply to any existing finished commands
        getAndRespond();
    } catch(ScstError const& e) {
        stopping = true;
        if (e == ScstError::scst_target_destroyed) {
            // Should we tear down the LUN too?
            removed = true;
        }
    }
}

void
ScstDevice::fastReply(int32_t const result) {
    auto task = new ScstTask(cmd->cmd_h, cmd->subcode);
    task->setResult(result);
    readyResponses.push(task);
}

}  // namespace scst
}  // namespace connector
}  // namespace fds
