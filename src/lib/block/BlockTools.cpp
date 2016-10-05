/*
 * BlockTools.cpp
 *
 * Copyright (c) 2016, Andreas Griesshammer <andreas@formartionds.com>
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

#include "connector/block/BlockTools.h"

namespace fds {
namespace block {

std::ostream& operator<< (std::ostream &os, const OffsetInfo &oi) {
    os << "startBlockOffset:"      << oi.startBlockOffset
       << " endBlockOffset:"       << oi.endBlockOffset
       << " startDiffOffset:"      << oi.startDiffOffset
       << " endDiffOffset:"        << oi.endDiffOffset
       << " spansFullBlocks:"      << ((true == oi.spansFullBlocks) ? "true" : "false")
       << " fullStartBlockOffset:" << oi.fullStartBlockOffset
       << " fullEndBlockOffset:"   << oi.fullEndBlockOffset;
    return os;
}

void calculateOffsets
(
  OffsetInfo&      info,
  uint64_t const&  offset,
  uint32_t const&  length,
  uint32_t const&  maxObjectSizeInBytes
)
{
    if (maxObjectSizeInBytes == 0) return;
    auto absoluteEndOffset = offset + length - 1;
    info.startBlockOffset = offset / maxObjectSizeInBytes;
    info.endBlockOffset = absoluteEndOffset / maxObjectSizeInBytes;
    auto remaining = length;
    // Calculate the absolute offsets of start and end of the entire range of blocks
    // Used to determine block alignment
    auto absoluteBlockStartOffset = (uint64_t)info.startBlockOffset * maxObjectSizeInBytes;
    auto absoluteBlockEndOffset = ((uint64_t)(info.endBlockOffset + 1) * maxObjectSizeInBytes) - 1;
    // Determine if we're starting aligned and if not by how much
    auto startDiff = offset - absoluteBlockStartOffset;
    // Determine if we're ending aligned and if not by how much
    auto endDiff = (absoluteBlockEndOffset >= absoluteEndOffset) ? (absoluteBlockEndOffset - absoluteEndOffset) : 0;

    if ((info.startBlockOffset == info.endBlockOffset) && (length < maxObjectSizeInBytes)) {
        info.startDiffOffset = startDiff;
        info.endDiffOffset = endDiff;
    } else {
        remaining -= (0 != startDiff) ? (maxObjectSizeInBytes - startDiff) : 0;
        remaining -= (0 != endDiff) ? (maxObjectSizeInBytes - endDiff) : 0;

        // Determine if this range spans any full blocks
        auto numFullObjects = remaining / maxObjectSizeInBytes;
        if (0 < numFullObjects) {
            info.spansFullBlocks = true;
            info.numFullBlocks = numFullObjects;
            info.startDiffOffset = startDiff;
            info.fullStartBlockOffset = (offset + ((0 != startDiff) ? (maxObjectSizeInBytes - startDiff) : 0)) / maxObjectSizeInBytes;
            info.fullEndBlockOffset = info.fullStartBlockOffset + numFullObjects - 1;
            info.endDiffOffset = endDiff;
        } else {
            info.startDiffOffset = startDiff;
            info.endDiffOffset = endDiff;
        }
    }
}

}  // namespace block
}  // namespace fds
