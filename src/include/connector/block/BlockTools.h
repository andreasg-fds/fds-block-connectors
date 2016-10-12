/*
 * BlockTools.h
 *
 * Copyright (c) 2016, Andreas Griesshammer <andreas@formationds.com>
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

#ifndef BLOCKTOOLS_H_
#define BLOCKTOOLS_H_

// System includes
#include <iostream>

// FDS includes

namespace fds {
namespace block {

struct OffsetInfo {
    // This structure is used to calculate block offsets for a given offset and length
    // startBlockOffset and endBlockOffset shall always be valid
    // startDiffOffset and endDiffOffset shall always be valid
    uint32_t       startBlockOffset;
    uint32_t       endBlockOffset;
    // Offset in the first object to location of start
    uint32_t       startDiffOffset {0};
    // Amount of data in last object if not a complete object
    uint32_t       endDiffOffset {0};
    // If spansFullBlocks is true then fullStartBlockOffset and fullEndBlockOffset shall be valid
    bool           spansFullBlocks {false};
    uint32_t       numFullBlocks {0};
    uint32_t       fullStartBlockOffset {0};
    uint32_t       fullEndBlockOffset {0};

    bool isSingleObject() { return startBlockOffset == endBlockOffset; }
    uint32_t numTotalBlocks() { return endBlockOffset - startBlockOffset + 1; }
};

std::ostream& operator<< (std::ostream &os, const OffsetInfo &oi);

void calculateOffsets(OffsetInfo& info, uint64_t const& offset, uint32_t const& length, uint32_t const& maxObjectSizeInBytes);

}  // namespace block
}  // namespace fds

#endif // BLOCKTOOLS_H_
