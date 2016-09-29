/**
 * Copyright 2016 by Formation Data Systems, Inc.
 */
#ifndef _WRITECONTEXT_H
#define _WRITECONTEXT_H

// System includes
#include <atomic>
#include <queue>
#include <map>
#include <mutex>
#include <set>

// FDS includes
#include "xdi/ApiTypes.h"

using namespace xdi;

namespace fds {
namespace block {

class BlockTask;

class WriteContext {

public:
    enum class QueueResult { FirstEntry, AddedEntry, UpdateStable, Failure };

    using PendingTasks = std::queue<BlockTask*>;
    WriteContext(VolumeId volId, std::string& name, uint32_t size);
    ~WriteContext();

    bool addPendingWrite(ObjectOffsetVal const& newStart, ObjectOffsetVal const& newEnd, BlockTask* task);
    bool addReadBlob(ObjectOffsetVal const& startOffset, ObjectOffsetVal const& endOffset, BlockTask* task);

    bool getWriteBlobRequest(ObjectOffsetVal const& offset, WriteBlobRequest& req, PendingTasks& queue);
    bool failWriteBlobRequest(ObjectOffsetVal const& offset, PendingTasks& queue);
    void triggerWrite(ObjectOffsetVal const& offset);

    int getNumPendingBlobs() { return _pendingBlobWrites.size(); };

    QueueResult queue_update(ObjectOffsetVal const& offset, RequestHandle handle, bool fullBlock = false);
    std::pair<bool, RequestHandle> pop(ObjectOffsetVal const& offset);

    void completeBlobWrite(ObjectOffsetVal const& offset, PendingTasks& queue);

    void setOffsetObjectBuffer(ObjectOffsetVal const& offset, std::shared_ptr<std::string> buf);
    std::shared_ptr<std::string> getOffsetObjectBuffer(ObjectOffsetVal const& offset);

    void updateOffset(ObjectOffsetVal const& offset, ObjectId const& id);

private:
    BlobPath                     _path;
    uint32_t                     _objectSize;

    /********************************************
    ** The following structures are used to track
    ** an outstanding Blob and the status off each
    ** offset in that Blob.
    ********************************************/
    struct PendingOffsetWrite {
        ObjectId    id;
        //TODO: isStable could just be absence of ObjectId
        bool        isStable {false};
        bool        hasPendingWrite {true};
        std::unique_ptr<std::queue<RequestHandle>>    updateChain;
        std::shared_ptr<std::string>                  buf;
    };

    struct PendingBlobWrite {
        PendingBlobWrite() : numObjects(0) {};
        PendingBlobWrite(PendingBlobWrite&& other) :   numObjects(other.numObjects),
                                                       pendingTasks(std::move(other.pendingTasks)),
                                                       pendingBlobReads(std::move(other.pendingBlobReads)),
                                                       offsetStatus(std::move(other.offsetStatus)) {};

        PendingBlobWrite& operator+=(PendingBlobWrite&& other) {
            while (false == other.pendingTasks.empty()) {
                auto t = other.pendingTasks.front();
                pendingTasks.push(t);
                other.pendingTasks.pop();
            }
            for (auto& t2 : other.pendingBlobReads) {
                pendingBlobReads.emplace(std::move(t2));
            }
            other.pendingBlobReads.clear();
            for (auto& s : other.offsetStatus) {
                offsetStatus.emplace(s.first, std::move(s.second));
                ++numObjects;
            }
            other.offsetStatus.clear();
            return *this;
        }

        int                                            numObjects;
        PendingTasks                                   pendingTasks;
        std::set<BlockTask*>                           pendingBlobReads;
        std::map<ObjectOffsetVal, PendingOffsetWrite>  offsetStatus;
    };

    std::map<ObjectOffsetVal, PendingBlobWrite>     _pendingBlobWrites;

    /********************************************
    ** The following structures are used to track
    ** a BlobWrite that we're waiting for the
    ** response on and any further task waiting
    ** for that offset range.
    ********************************************/
    struct AwaitingBlobWrite {
        int                                numObjects;
        PendingTasks                       pendingTasks;
    };

    std::map<ObjectOffsetVal, AwaitingBlobWrite>    _awaitingBlobWrites;

    bool checkOverlappingAwaitingBlobWrite
    (
      ObjectOffsetVal const&      startOffset,
      ObjectOffsetVal const&      endOffset,
      BlockTask*                  task
    );

    bool checkForOverlap
    (
      ObjectOffsetVal const&      newStart,
      ObjectOffsetVal const&      newEnd,
      ObjectOffsetVal const&      origStart,
      ObjectOffsetVal const&      origEnd
    );

    void mergeRanges
    (
      ObjectOffsetVal const&      newStart,
      ObjectOffsetVal const&      newEnd,
      BlockTask*                  task
    );

};

} // namespace block
} // namespace fds

#endif // _WRITECONTEXT_H
