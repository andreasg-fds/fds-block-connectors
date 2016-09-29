/**
 * Copyright 2016 by Formation Data Systems, Inc.
 */

// FDS includes
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
