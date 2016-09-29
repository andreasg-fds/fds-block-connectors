/**
 * Copyright 2016 by Formation Data Systems, Inc.
 */
#include "connector/block/WriteContext.h"
#include "Log.h"

namespace fds {
namespace block {

WriteContext::WriteContext(VolumeId volId, std::string& name, uint32_t size) : _path(volId, name), _objectSize(size) {
}

WriteContext::~WriteContext() {

}

bool WriteContext::checkOverlappingAwaitingBlobWrite
(
  ObjectOffsetVal const&  startOffset,
  ObjectOffsetVal const&  endOffset,
  BlockTask*              task
)
{
    // Check if we awaiting a response on a BlobWrite for this range
    auto awaitingItr = _awaitingBlobWrites.begin();
    while (_awaitingBlobWrites.end() != awaitingItr) {
        auto awaitingStartOffset = awaitingItr->first;
        auto awaitingEndOffset = awaitingItr->first + awaitingItr->second.numObjects - 1;
        if (true == checkForOverlap(startOffset, endOffset, awaitingStartOffset, awaitingEndOffset)) {
            awaitingItr->second.pendingTasks.push(task);
            return true;
        }
        ++awaitingItr;
    }
    return false;
}

/**********************************************************
* Determine if existing range overlaps with new range.
* Returns - true if overlap
*           false if no overlap
*
* X = newStart
* Y = newEnd
* S = origStart
* E = origEnd
* case 1:   newStart is between origStart and origEnd
*           SoooooooooooooE
*                     XooooooooooooooY
*
*                   or
*
*           SoooooooooooooE
*                  XoooY
*
* case 2:    newEnd is between origStart and origEnd
*                     SooooooooooooooE
*            XoooooooooooooY
*
* case 3:    origStart and origEnd are between newStart and newEnd
*                   SooooooooE
*             XooooooooooooooooooooY
**********************************************************/
bool WriteContext::checkForOverlap
(
  ObjectOffsetVal const&         newStart,
  ObjectOffsetVal const&         newEnd,
  ObjectOffsetVal const&         origStart,
  ObjectOffsetVal const&         origEnd
)
{
    if (((newStart >= origStart) && (newStart <= origEnd)) || // case 1
        ((newEnd >= origStart) && (newEnd <= origEnd))     || // case 2
        ((newStart < origStart) && (newEnd > origEnd)))       // case 3
    {
        return true;
    }
    return false;
}

void WriteContext::mergeRanges
(
  ObjectOffsetVal const&                 newStart,
  ObjectOffsetVal const&                 newEnd,
  BlockTask*                             task
)
{
    auto lowestStart = newStart;
    PendingBlobWrite newPendingBlobWrite;

    // Any existing overlapping entry gets merged into this new one
    auto itr = _pendingBlobWrites.begin();
    while ((_pendingBlobWrites.end() != itr) && (itr->first <= newEnd)) {
        auto origStart = itr->first;
        auto origEnd = itr->first + itr->second.numObjects - 1;
        if (true == checkForOverlap(newStart, newEnd, origStart, origEnd))
        {
            if (origStart < lowestStart) {
                lowestStart = origStart;
            }
            auto erase_itr = itr;
            ++itr;
            newPendingBlobWrite += std::move(erase_itr->second);
            _pendingBlobWrites.erase(erase_itr);
        } else {
            ++itr;
        }
    }

    // Fill in any remaining gaps for the full range
    for (auto i = newStart; i <= newEnd; ++i) {
        auto offsetItr = newPendingBlobWrite.offsetStatus.find(i);
        if (offsetItr == newPendingBlobWrite.offsetStatus.end()) {
            PendingOffsetWrite pow;
            pow.isStable = false;
            auto addedOffset = newPendingBlobWrite.offsetStatus.emplace(i, std::move(pow));
            if (true == std::get<1>(addedOffset)) {
                ++(newPendingBlobWrite.numObjects);
            }
        }
    }

    // Insert this new entry into the map
    newPendingBlobWrite.pendingBlobReads.emplace(task);
    bool happened {false};
    auto emplace_itr = _pendingBlobWrites.begin();
    std::tie(emplace_itr, happened) = _pendingBlobWrites.emplace(lowestStart, std::move(newPendingBlobWrite));
    if (false == happened) {
        LOGERROR << "emplace failed" << std::endl;
    }
}

bool WriteContext::addReadBlob
(
  ObjectOffsetVal const&     startOffset,
  ObjectOffsetVal const&     endOffset,
  BlockTask*                 task
)
{
    if (true == checkOverlappingAwaitingBlobWrite(startOffset, endOffset, task)) return false;
    mergeRanges(startOffset, endOffset, task);

    return true;
}

// Check to see if this write overlaps with an already pending
// range of writes.  If so, merge those writes together.
// Returns false if the pending write should not continue and has been queued
// up to continue later.
bool WriteContext::addPendingWrite(ObjectOffsetVal const& newStart, ObjectOffsetVal const& newEnd, BlockTask* task) {
    auto b_itr = _pendingBlobWrites.begin();
    while (_pendingBlobWrites.end() != b_itr) {
        auto origStart = b_itr->first;
        auto origEnd = b_itr->first + b_itr->second.numObjects - 1;
        if (true == checkForOverlap(newStart, newEnd, origStart, origEnd)) {
            for (auto i = newStart; i <= newEnd; ++i) {
                auto o_itr = b_itr->second.offsetStatus.find(i);
                if (b_itr->second.offsetStatus.end() == o_itr) {
                    LOGERROR << "offset:" << i << " missing";
                } else {
                    o_itr->second.isStable = false;
                }
            }
            b_itr->second.pendingBlobReads.erase(task);
            b_itr->second.pendingTasks.push(task);
            return true;
        }
        ++b_itr;
    }
    return false;
}

void WriteContext::triggerWrite(ObjectOffsetVal const& offset) {
    auto itr = _pendingBlobWrites.begin();
    while (_pendingBlobWrites.end() != itr) {
        if ((itr->first <= offset) && (itr->first + itr->second.numObjects > offset)) {
            itr->second.offsetStatus[offset].id = "";
            itr->second.offsetStatus[offset].isStable = false;
            itr->second.offsetStatus[offset].hasPendingWrite = true;
            return;
        }
        ++itr;
    }
    LOGERROR << "offset:" << offset << " missing";
}

void WriteContext::updateOffset(ObjectOffsetVal const& offset, ObjectId const& id) {
    auto itr = _pendingBlobWrites.begin();
    while (_pendingBlobWrites.end() != itr) {
        if ((itr->first <= offset) && (itr->first + itr->second.numObjects > offset)) {
            itr->second.offsetStatus[offset].id = id;
            if ((nullptr == itr->second.offsetStatus[offset].updateChain) ||
                ((nullptr != itr->second.offsetStatus[offset].updateChain) && (true == itr->second.offsetStatus[offset].updateChain->empty()))) {
                itr->second.offsetStatus[offset].isStable = true;
            }
            itr->second.offsetStatus[offset].hasPendingWrite = false;
            return;
        }
        ++itr;
    }
    LOGERROR << "offset:" << offset << " missing";
}

void WriteContext::setOffsetObjectBuffer(ObjectOffsetVal const& offset, std::shared_ptr<std::string> buf) {
    auto itr = _pendingBlobWrites.begin();
    while (_pendingBlobWrites.end() != itr) {
        if ((itr->first <= offset) && (itr->first + itr->second.numObjects > offset)) {
            auto o_itr = itr->second.offsetStatus.find(offset);
            if (itr->second.offsetStatus.end() != o_itr) {
                o_itr->second.buf = buf;
            }
            return;
        }
        ++itr;
    }
}

std::shared_ptr<std::string> WriteContext::getOffsetObjectBuffer(ObjectOffsetVal const& offset) {
    auto itr = _pendingBlobWrites.begin();
    while (_pendingBlobWrites.end() != itr) {
        if ((itr->first <= offset) && (itr->first + itr->second.numObjects > offset)) {
            auto o_itr = itr->second.offsetStatus.find(offset);
            if (itr->second.offsetStatus.end() != o_itr) {
                return o_itr->second.buf;
            }
        }
        ++itr;
    }
    return std::make_shared<std::string>();
}

// getWriteBlobRequest() is used to generate a WriteBlobRequest that contains the offset
// being passed in.
// If the function returns false then req should not be used because it's in an
// indeterminate state.
// If the function returns true then the WriteBlobRequest should be sent out and the range
// it covers will be removed from the WriteContext.
bool WriteContext::getWriteBlobRequest(ObjectOffsetVal const& offset, WriteBlobRequest& req, std::queue<BlockTask*>& queue) {
    auto itr = _pendingBlobWrites.begin();
    while (_pendingBlobWrites.end() != itr) {
        if ((offset >= itr->first) && (offset < itr->first + itr->second.numObjects)) {
            if (0 < itr->second.pendingBlobReads.size()) {
                return false;
            }
            req.blob.blobInfo.path = _path;
            req.blob.objects.clear();
            for (auto const& o : itr->second.offsetStatus) {
                if (false == o.second.isStable) {
                    return false;
                } else if ((nullptr != o.second.updateChain) && (!o.second.updateChain->empty())) {
                    return false;
                }
                ObjectDescriptor od;
                od.objectId = o.second.id;
                od.length = _objectSize;
                req.blob.objects.emplace(o.first, od);
            }
            AwaitingBlobWrite awaiting;
            awaiting.numObjects = itr->second.numObjects;
            _awaitingBlobWrites.emplace(itr->first, std::move(awaiting));
            queue = std::move(itr->second.pendingTasks);
            _pendingBlobWrites.erase(itr);
            return true;
        }
        ++itr;
    }
    return false;
}

bool WriteContext::failWriteBlobRequest(ObjectOffsetVal const& offset, PendingTasks& queue) {
    auto itr = _pendingBlobWrites.begin();
    while (_pendingBlobWrites.end() != itr) {
        if ((offset >= itr->first) && (offset < itr->first + itr->second.numObjects)) {
            queue = std::move(itr->second.pendingTasks);
            // Any pendingBlobRead tasks will need to be failed as well
            for (auto& t : itr->second.pendingBlobReads) {
                queue.push(t);
            }
            _pendingBlobWrites.erase(itr);
            return true;
        }
        ++itr;
    }
    return false;
}

WriteContext::QueueResult WriteContext::queue_update(ObjectOffsetVal const& offset, RequestHandle handle, bool fullBlock) {
   auto b_itr = _pendingBlobWrites.begin();
   while (_pendingBlobWrites.end() != b_itr) {
       if ((offset >= b_itr->first) && (offset < b_itr->first + b_itr->second.numObjects)) {
           auto off_itr = b_itr->second.offsetStatus.find(offset);
           if (b_itr->second.offsetStatus.end() != off_itr) {
               if ((nullptr == off_itr->second.updateChain) || (true == fullBlock)) {
                   off_itr->second.updateChain.reset(new std::queue<RequestHandle>());
                   off_itr->second.hasPendingWrite = true;
                   return QueueResult::FirstEntry;
               } else if ((false == off_itr->second.hasPendingWrite) && (true == off_itr->second.updateChain->empty())) {
                   off_itr->second.updateChain->push(handle);
                   off_itr->second.hasPendingWrite = true;
                   return QueueResult::UpdateStable;
               } else {
                   off_itr->second.updateChain->push(handle);
                   return QueueResult::AddedEntry;
               }
           } else {
               LOGERROR << "offset:" << offset << " missing";
               return QueueResult::Failure;
           }
       }
       ++b_itr;
   }
   LOGERROR << "offset: " << offset << " missing";
   return QueueResult::Failure;
}

std::pair<bool, RequestHandle> WriteContext::pop(ObjectOffsetVal const& offset) {
   static std::pair<bool, RequestHandle> const no = {false, RequestHandle()};
   auto itr = _pendingBlobWrites.begin();
   while (_pendingBlobWrites.end() != itr) {
      if ((offset >= itr->first) && (offset < itr->first + itr->second.numObjects)) {
          auto off_itr = itr->second.offsetStatus.find(offset);
          if ((itr->second.offsetStatus.end() != off_itr) && (nullptr != off_itr->second.updateChain)) {
              if (!off_itr->second.updateChain->empty()) {
                  auto val = off_itr->second.updateChain->front();
                  off_itr->second.updateChain->pop();
                  return std::make_pair(true, val);
              }
          } else {
              LOGERROR << "offset:" << offset << " missing";
              return no;
          }
       }
      ++itr;
    }
    return no;
}

void WriteContext::completeBlobWrite(ObjectOffsetVal const& offset, PendingTasks& queue) {
   auto awaitingItr = _awaitingBlobWrites.begin();
   while (_awaitingBlobWrites.end() != awaitingItr) {
      if ((awaitingItr->first <= offset) && (awaitingItr->first + awaitingItr->second.numObjects > offset)) {
          queue = std::move(awaitingItr->second.pendingTasks);
          _awaitingBlobWrites.erase(awaitingItr);
          return;
      }
      ++awaitingItr;
   }
}

} // namespace block
} // namespace fds
