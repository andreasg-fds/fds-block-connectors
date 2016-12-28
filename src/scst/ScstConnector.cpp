/*
 * ScstConnector.cpp
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

#include <set>
#include <string>
#include <thread>

#include <iostream>

#include "connector/scst-standalone/ScstConnector.h"
#include "connector/scst-standalone/ScstTarget.h"
#include "connector/scst-standalone/scst_log.h"

namespace xdi {
    std::shared_ptr<spdlog::logger> scst_logger_;

    std::shared_ptr<spdlog::logger> GetLogger() {
        return scst_logger_;
    }
}

namespace fds {
namespace connector {
namespace scst {

static constexpr size_t minimum_chap_password_len {12};
static std::string      iscsi_name_invalid_characters {"/"};

static bool validateTargetName(std::string const& target_name) {
    return std::string::npos == target_name.find(iscsi_name_invalid_characters);
}

// The singleton
std::shared_ptr<ScstConnector> ScstConnector::instance_ {nullptr};

void ScstConnector::start(std::shared_ptr<xdi::ApiInterface> api) {
    static std::once_flag init;
    // Initialize the singleton
    std::call_once(init, [api] () mutable
    {
        // TODO(bszmyd): Thu 29 Sep 2016 10:06:49 AM MDT
        // This should be configurable
        auto target_prefix = "iqn.2012-05.com.formationds:";
        auto queue_depth = 64;
        instance_.reset(new ScstConnector(target_prefix, queue_depth, api));
        auto t = std::thread(&ScstConnector::discoverTargets, instance_.get());
        t.detach();
    });
}

void ScstConnector::shutdown() {
    if (instance_) {
        instance_->terminate();
    }
}

void ScstConnector::terminate() {
    std::unique_lock<std::mutex> lk(target_lock_);
    stopping = true;
    stopping_condition_.notify_all();
    for (auto& target_pair : targets_) {
        target_pair.second->shutdown();
    }
    done_condition_.wait(lk, [this] () -> bool { return targets_.empty(); });
}

void ScstConnector::targetDone(const std::string target_name) {
    std::lock_guard<std::mutex> lk(target_lock_);
    for (auto const& target_pair : targets_) {
        auto it_target_name = target_prefix + target_pair.first->volumeName;
        if (it_target_name == target_name) {
            targets_.erase(target_pair.first);
            LOGINFO("vol:{} connector removed target", target_name);
            break;
        }
    }
    done_condition_.notify_one();
}

bool ScstConnector::addTarget(volume_ptr& volDesc) {
    // Create target if it does not already exist
    auto target_name = target_prefix + volDesc->volumeName;

    auto it = targets_.begin();
    for (; targets_.end() != it; ++it) {
        if (it->first->volumeName == volDesc->volumeName &&
            it->first->volumeId == volDesc->volumeId) {
            break;
        }
    }

    bool target_added {false};
    if (targets_.end() == it) {
        std::tie(it, target_added) = targets_.emplace(volDesc, nullptr);
        try {
            it->second.reset(new ScstTarget(this,
                                            target_name,
                                            queue_depth,
                                            api_));
            it->second->addDevice(volDesc);
        } catch (ScstError& e) {
            LOGINFO("vol:{} failed to initialize target which will be blacklisted", volDesc->volumeName);
            if (it->second) {
                it->second->deviceDone(volDesc->volumeName, true);
            }
            targets_.erase(it);
            // Add this to our black list since another attempt will most likely
            // fail similarly
            black_listed_vols.emplace(volDesc->volumeId);
            return false;
        }
    }

    auto& target = *it->second;

    // If we already had a target, and it's shutdown...wait for it to complete
    // before trying to apply the apparently new descriptor
    if (!target.enabled()) {
        LOGINFO("vol:{} waiting for existing target to complete shutdown", target_name);
        return false;
    }

    // Setup initiator masking
    std::set<std::string> initiator_list;
    for (auto const& ini : volDesc->initiators) {
        initiator_list.emplace(ini);
    }
    target.setInitiatorMasking(initiator_list);

    // Setup CHAP
    std::unordered_map<std::string, std::string> incoming_credentials;
    for (auto const& cred : volDesc->incomingCredentials) {
        auto password = cred.password;
        if (minimum_chap_password_len > password.size()) {
            LOGWARN("user:{} length:{} minlength:{} extending undersized password",
                    cred.username,
                    password.size(),
                    minimum_chap_password_len);
            password.resize(minimum_chap_password_len, '*');
        }
        auto cred_it = incoming_credentials.end();
        bool happened;
        std::tie(cred_it, happened) = incoming_credentials.emplace(cred.username, password);
        if (!happened) {
            LOGWARN("user:{} duplicate", cred.username);
        }
    }

    // Outgoing credentials only support a single entry, just take the last one
    std::unordered_map<std::string, std::string> outgoing_credentials;
    if (!volDesc->outgoingCredentials.username.empty()) {
        auto const& cred = volDesc->outgoingCredentials;
        auto password = cred.password;
        if (minimum_chap_password_len > password.size()) {
            LOGWARN("user:{} length:{} minlength:{} extending undersized password",
                    cred.username,
                    password.size(),
                    minimum_chap_password_len);
            password.resize(minimum_chap_password_len, '*');
        }
        outgoing_credentials.emplace(cred.username, password);
    }

    target.setCHAPCreds(incoming_credentials, outgoing_credentials);

    target.enable();
    return target_added;
}

void ScstConnector::removeTarget(volume_ptr const& volDesc) {
    for (auto const& target_pair : targets_) {
        if (target_pair.first->volumeName == volDesc->volumeName &&
            target_pair.first->volumeId == volDesc->volumeId) {
            target_pair.second->removeDevice(volDesc->volumeName);
        }
    }
}

ScstConnector::ScstConnector(std::string const& prefix,
                             size_t const depth,
                             std::shared_ptr<xdi::ApiInterface> api)
        : api_(api),
          target_prefix(prefix),
          queue_depth(depth)
{
    xdi::SetScstLogger(xdi::createLogger("scst"));
    LOGDEBUG("ScstConnector constructor");
}

static auto const rediscovery_delay = std::chrono::seconds(10);

void
ScstConnector::discoverTargets() {
    ScstAdmin::toggleDriver(false);
    while (!stopping) {
        LOGTRACE("Discovering iSCSI volumes to export.");
        xdi::RequestHandle requestId{0,0};
        xdi::Request r{requestId, xdi::RequestType::LIST_ALL_VOLUMES_TYPE, this};
        xdi::ListAllVolumesRequest req;
        getting_list = true;
        api_->listAllVolumes(r, req);
        std::unique_lock<std::mutex> lk(target_lock_);
        listing_condition_.wait(lk, [this] () -> bool { return !getting_list; });
        stopping_condition_.wait_for(lk, rediscovery_delay);
    }
    ScstAdmin::toggleDriver(false);
    LOGINFO("Shutdown discovery loop");
}

void
ScstConnector::listAllVolumesResp(xdi::RequestHandle const&,
                                  xdi::ListAllVolumesResponse const& resp,
                                  xdi::ApiErrorCode const& e) {
    {
        std::lock_guard<std::mutex> lk(target_lock_);
        getting_list = false;

        if (xdi::ApiErrorCode::XDI_OK == e) {
            xdi::VolumeDescriptorVisitor v;
            for (auto const& vol : resp.volumes) {
                if ((xdi::VolumeType::ISCSI_VOLUME_TYPE == vol->match(&v)) &&
                    (0 == black_listed_vols.count(vol->volumeId)) &&
                    (validateTargetName(vol->volumeName)))
                {
                    auto currVol = std::static_pointer_cast<xdi::IscsiVolumeDescriptor>(vol);
                    // Volume mgmt is a little flaky, wait till any old target of
                    // the same name is removed before adding the replacement.
                    auto should_add = true;
                    for (auto const& target_pair : targets_) {
                        if (target_pair.first->volumeName == vol->volumeName &&
                            target_pair.first->volumeId != vol->volumeId) {
                            LOGINFO("vol:{} skipping while target shuts down", vol->volumeName);
                            removeTarget(target_pair.first);
                            should_add = false;
                            break;
                        }
                    }
                    if (should_add) {
                        if (addTarget(currVol)) {
                            LOGINFO("vol:{} added", vol->volumeName);
                        }
                    }
                }

            }
            ScstAdmin::toggleDriver(true);
        }
    }
    listing_condition_.notify_one();
}

}  // namespace scst
}  // namespace connector
}  // namespace fds

extern "C" {
    void start(std::shared_ptr<xdi::ApiInterface>* api) {
        fds::connector::scst::ScstConnector::start(*api);
    }

    void stop() {
        fds::connector::scst::ScstConnector::shutdown();
    }
}
