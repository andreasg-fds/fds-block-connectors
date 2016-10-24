/*
 * Tasks.cpp
 *
 * Copyright (c) 2016, Andreas Griesshammer <andreas@formationds.com>
 * Copyright (c) 2015, Brian Szmyd <szmyd@formationds.com>
 * Copyright (c) 2015-2016, Formation Data Systems
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

#include "connector/block/Tasks.h"

namespace fds {
namespace block {

void
ReadTask::handleReadResponse(std::vector<std::shared_ptr<std::string>>& buffers,
                              std::shared_ptr<std::string>& empty_buffer) {
    // acquire the buffers
    bufVec.swap(buffers);

    uint32_t len {0};

    // Fill in any missing wholes with zero data, this is a special *block*
    // semantic for NULL objects.
    for (auto& buf: bufVec) {
        if (!buf || 0 == buf->size()) {
            buf = empty_buffer;
            len += maxObjectSizeInBytes;
        } else {
            len += buf->size();
        }
    }

    // return zeros for uninitialized objects, again a special *block*
    // semantic to PAD the read to the required length.
    uint32_t iOff = getOffset() % maxObjectSizeInBytes;
    if (len < (getLength() + iOff)) {
        for (ssize_t zero_data = (getLength() + iOff) - len; 0 < zero_data; zero_data -= maxObjectSizeInBytes) {
            bufVec.push_back(empty_buffer);
        }
    }

    // Trim the data as needed from the front...
    auto firstObjLen = std::min(getLength(), maxObjectSizeInBytes - iOff);
    if (maxObjectSizeInBytes != firstObjLen) {
        bufVec.front() = std::make_shared<std::string>(bufVec.front()->data() + iOff, firstObjLen);
    }

    // ...and the back
    if (getLength() > firstObjLen) {
        auto padding = (2 < bufVec.size()) ? (bufVec.size() - 2) * maxObjectSizeInBytes : 0;
        auto lastObjLen = getLength() - firstObjLen - padding;
        if (0 < lastObjLen && maxObjectSizeInBytes != lastObjLen) {
            bufVec.back() = std::make_shared<std::string>(bufVec.back()->data(), lastObjLen);
        }
    }
}

std::shared_ptr<std::string>
WriteTask::handleRMWResponse(std::shared_ptr<std::string> const& retBuf,
                             sequence_type seqId) {

    auto w_itr = writeOffsetInBlockMap.find(seqId);
    uint32_t iOff = (w_itr != writeOffsetInBlockMap.end()) ? w_itr->second % maxObjectSizeInBytes : 0;
    auto& writeBytes = bufVec[seqId];
    std::shared_ptr<std::string> fauxBytes;
    if (!retBuf || (0 == retBuf->size())) {
        // we tried to read unwritten block, so create
        // an empty block buffer to place the data
        fauxBytes = std::make_shared<std::string>(maxObjectSizeInBytes, '\0');
        fauxBytes->replace(iOff, writeBytes->length(),
                           writeBytes->c_str(), writeBytes->length());
    } else {
        // Need to copy retBut into a modifiable buffer since retBuf is owned
        // by the connector and should not be modified here.
        fauxBytes = std::make_shared<std::string>(retBuf->c_str(), retBuf->length());
        fauxBytes->replace(iOff, writeBytes->length(),
                           writeBytes->c_str(), writeBytes->length());
    }
    // Update the resp so the next in the chain can grab the buffer
    writeBytes = fauxBytes;
    return fauxBytes;
}

}  // namespace block
}  // namespace fds
