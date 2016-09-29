/*
 * scst/ScstAdmin.h
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

#ifndef SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTADMIN_H_
#define SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTADMIN_H_

#include <array>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>

#include "connector/scst-standalone/ScstCommon.h"

namespace fds {
namespace connector {
namespace scst {

struct ScstDevice;

/**
 * A set of static routines to manipulate the SCST layer.
 */
struct ScstAdmin
{
    template<typename T>
    using shared = std::shared_ptr<T>;
    using device_ptr = shared<ScstDevice>;

    using lun_table_type = std::array<device_ptr, 255>;
    using lun_iterator = lun_table_type::iterator;
    template<typename T>
    using map_type = std::unordered_map<std::string, T>;
    using device_map_type = map_type<lun_iterator>;

    using initiator_set = std::set<std::string>;

    using credential_map = map_type<std::string>;

    ScstAdmin() = delete;
    ~ScstAdmin() = delete;

    /**
     * Is the driver enabled
     */
    static bool driverEnabled();

    static void toggleDriver(bool const enable);

    /**
     * Is the target enabled
     */
    static bool targetEnabled(std::string const& target_name);

    static void toggleTarget(std::string const& target_name, bool const enable);

    /**
     * Build a map of the current users known to SCST for a given target
     */
    static credential_map currentIncomingUsers(std::string const& target_name);
    static credential_map currentOutgoingUsers(std::string const& target_name);

    static void currentInitiators(std::string const& target_name, initiator_set& current_set);

    /**
     * Add the given credential to the target's IncomingUser attributes
     */
    static void addIncomingUser(std::string const& target_name,
                                std::string const& user_name,
                                std::string const& password);

    static void addOutgoingUser(std::string const& target_name,
                                std::string const& user_name,
                                std::string const& password);

    /**
     * When we're done with the target from it from SCST to cause
     * any existing sessions to be terminated.
     */
    static void addToScst(std::string const& target_name);

    static bool groupExists(std::string const& target_name, std::string const& group_name);

    /**
     * Remove the given credential from the target's attributes
     */
    static void removeIncomingUser(std::string const& target_name,
                                   std::string const& user_name);
    static void removeOutgoingUser(std::string const& target_name,
                                   std::string const& user_name);

    static bool mapDevices(std::string const& target_name,
                           device_map_type const& device_map,
                           lun_iterator const lun_start);

    static void removeDevice(std::string const& target_name,
                             lun_iterator const lun_rem);

    static bool applyMasking(std::string const& target_name,
                             initiator_set const& new_set);

    /**
     * Before a target can be removed, all sessions must be closed
     */
    static void removeInitiators(std::string const& target_name);

    /**
     * When we're done with the target from it from SCST to cause
     * any existing sessions to be terminated.
     */
    static void removeFromScst(std::string const& target_name);

    /**
     * Remove the given credential to the target's IncomingUser attributes
     */
    static void setQueueDepth(std::string const& target_name, uint32_t const queue_depth);
};

}  // namespace scst
}  // namespace connector
}  // namespace fds

#endif  // SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTADMIN_H_
