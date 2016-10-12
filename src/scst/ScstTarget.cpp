/*
 * ScstTarget.cpp
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

#include "connector/scst-standalone/ScstTarget.h"

#include <algorithm>
#include <iterator>
#include <fstream>
#include <thread>

#include <ev++.h>

#include "connector/scst-standalone/ScstConnector.h"
#include "connector/scst-standalone/ScstDisk.h"
#include "log/Log.h"

namespace fds {
namespace connector {
namespace scst {

ScstTarget::ScstTarget(ScstConnector* parent_connector,
                       std::string const& name,
                       size_t const queue_depth,
                       std::shared_ptr<xdi::ApiInterface> api) :
    connector(parent_connector),
    api_(api),
    target_name(name)
{
    // TODO(bszmyd): Sat 12 Sep 2015 12:14:19 PM MDT
    // We can support other target-drivers than iSCSI...TBD
    // Create an iSCSI target in the SCST mid-ware for our handler
    GLOGDEBUG << "target:" << target_name << " creating iSCSI target";
    ScstAdmin::addToScst(target_name);
    ScstAdmin::setQueueDepth(target_name, queue_depth);

    // Start watching the loop
    evLoop = std::make_shared<ev::dynamic_loop>(ev::NOENV | ev::POLL);
    if (!evLoop) {
        GLOGERROR << "failed to initialize lib_ev";
        throw ScstError::scst_error;
    }

    asyncWatcher = std::unique_ptr<ev::async>(new ev::async());
    asyncWatcher->set(*evLoop);
    asyncWatcher->set<ScstTarget, &ScstTarget::wakeupCb>(this);
    asyncWatcher->start();

    // Spawn a leader thread to start this target handling requests
    auto t = std::thread(&ScstTarget::lead, this);
    t.detach();
}

ScstTarget::~ScstTarget() = default;

void
ScstTarget::addDevice(volume_ptr& vol_desc) {
    std::unique_lock<std::mutex> l(deviceLock);

    // Check if we have a device with this name already
    if (device_map.end() != device_map.find(vol_desc->volumeName)) {
        GLOGDEBUG << "vol:" << vol_desc->volumeName << " already have device for volume";
        return;
    }

    // Get the next available LUN number
    auto lun_it = std::find_if_not(lun_table.begin(),
                                   lun_table.end(),
                                   [] (ScstAdmin::device_ptr& p) -> bool { return (!!p); });
    if (lun_table.end() == lun_it) {
        GLOGNOTIFY << "target:" << target_name << " exhausted all LUNs";
        return;
    }
    int32_t lun_number = std::distance(lun_table.begin(), lun_it);
    GLOGDEBUG << "vol:" << vol_desc->volumeName
             << " lun:" << lun_number
             << " mapping LUN";

    *lun_it = std::make_shared<ScstDisk>(vol_desc, this, api_);

    device_map[vol_desc->volumeName] = lun_it;

    devicesToStart.push_back(lun_number);
    asyncWatcher->send();
    // Wait for devices to start before continuing
    deviceStartCv.wait(l, [this] () -> bool { return devicesToStart.empty(); });
}

void
ScstTarget::deviceDone(std::string const& volume_name, bool const and_removed) {
    std::lock_guard<std::mutex> g(deviceLock);
    auto it = device_map.find(volume_name);
    if (device_map.end() != it) {
        ScstAdmin::removeDevice(target_name, it->second);
        it->second->reset();
        device_map.erase(it);
    }

    if (device_map.empty()) {
        // If the device closed because the volume was removed, destroy the tgt
        if (and_removed) {
            state = State::REMOVED;
        }
        disable();
        asyncWatcher->stop();
        evLoop->break_loop();
    }
}

void ScstTarget::removeDevice(std::string const& volume_name) {
    std::lock_guard<std::mutex> g(deviceLock);
    auto it = device_map.find(volume_name);
    if (device_map.end() == it) return;
    state = State::REMOVED;
    (*it->second)->remove();
}

void ScstTarget::shutdown() {
    std::lock_guard<std::mutex> g(deviceLock);
    state = State::STOPPED;
    for (auto& device : lun_table) {
        if (device) device->terminate();
    }
}

void
ScstTarget::setCHAPCreds(ScstAdmin::credential_map& incoming_credentials,
                         ScstAdmin::credential_map& outgoing_credentials) {
    std::lock_guard<std::mutex> l(deviceLock);

    // Remove all existing Incoming/Outgoing Users if not in new creds
    // or secret is different
    for (auto const& cred : ScstAdmin::currentIncomingUsers(target_name)) {
        auto it = incoming_credentials.find(cred.first);
        if ((incoming_credentials.end() != it) && (it->second == cred.second)) {
            // Already have this login
            incoming_credentials.erase(it);
            continue;
        }
        ScstAdmin::removeIncomingUser(target_name, cred.first);
    }

    for (auto const& cred : ScstAdmin::currentOutgoingUsers(target_name)) {
        auto it = outgoing_credentials.find(cred.first);
        if ((outgoing_credentials.end() != it) && (it->second == cred.second)) {
            // Already have this login
            outgoing_credentials.erase(it);
            continue;
        }
        ScstAdmin::removeOutgoingUser(target_name, cred.first);
    }

    // Apply all credentials
    for (auto const& cred : incoming_credentials) {
        ScstAdmin::addIncomingUser(target_name, cred.first, cred.second);
    }
    for (auto const& cred : outgoing_credentials) {
        ScstAdmin::addOutgoingUser(target_name, cred.first, cred.second);
    }
}

void
ScstTarget::setInitiatorMasking(ScstAdmin::initiator_set const& new_members) {
    std::lock_guard<std::mutex> l(deviceLock);
    if ((ini_members == new_members) && luns_mapped) {
        return;
    }

    if (ScstAdmin::applyMasking(target_name, new_members)) {
        ini_members = new_members;
    }

    if (!luns_mapped) {
        luns_mapped = ScstAdmin::mapDevices(target_name,
                                            device_map,
                                            lun_table.begin());
    }

}

// Starting the devices has to happen inside the ev-loop
void
ScstTarget::startNewDevices() {
    {
        std::lock_guard<std::mutex> g(deviceLock);
        for (auto& lun: devicesToStart) {
            auto& device = lun_table[lun];

            // Persist changes in the LUN table and device map
            device->start(evLoop);
        }
        devicesToStart.clear();
    }
    deviceStartCv.notify_all();
}

void
ScstTarget::wakeupCb(ev::async &watcher, int revents) {
    if (enabled()) {
        startNewDevices();
    }
}

void
ScstTarget::lead() {
    evLoop->run(0);
    GLOGNORMAL <<  "target:" << target_name << " shutdown";
    // If we were removed, disconnect sessions, otherwise we are just restarting
    {
        std::lock_guard<std::mutex> g(deviceLock);
        if (State::REMOVED == state) {
            ScstAdmin::removeFromScst(target_name);
        }
    }
    connector->targetDone(target_name);
}

}  // namespace scst
}  // namespace connector
}  // namespace fds
