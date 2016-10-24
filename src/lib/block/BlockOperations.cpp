/*
 * BlockOperations.cpp
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

// System includes
#include <algorithm>
#include <deque>
#include <map>
#include <string>
#include <thread>
#include <utility>

// FDS includes
#include "connector/block/Tasks.h"
#include "connector/block/BlockOperations.h"
#include "connector/block/WriteContext.h"
#include "log/Log.h"

namespace fds {
namespace block {

/**
 * Since multiple connections can serve the same volume we need
 * to keep this association information somewhere so we can
 * properly detach from the volume when unused.
 */
static std::unordered_map<std::string, std::uint_fast16_t> assoc_map {};
static std::mutex assoc_map_lock {};

static const uint32_t ZERO_OFFSET = 0;

BlockOperations::BlockOperations(std::shared_ptr<ApiInterface> interface)
        : volumeName(nullptr),
          blobName(new std::string("BlockBlob")),
          domainName(new std::string("TestDomain")),
          blobMode(new int32_t(0)),
          emptyMeta(new std::map<std::string, std::string>()),
          api(interface)
{
}

// We can't initialize this in the constructor since we want to pass
// a shared pointer to ourselves (and the Connection already started one).
void
BlockOperations::init(std::string                    vol_name,
                      uint64_t const                 vol_id,
                      uint32_t const                 obj_size)
{
    assert(obj_size >= 512);
    volumeName = std::make_shared<std::string>(vol_name);
    volumeId = vol_id;

    maxObjectSizeInBytes = obj_size;
    empty_buffer = std::make_shared<std::string>(maxObjectSizeInBytes, '\0');
    // Reference count this association
    std::lock_guard<std::mutex> lk(assoc_map_lock);
    ++assoc_map[*volumeName];
    ctx.reset(new WriteContext(volumeId, *blobName, maxObjectSizeInBytes));
}


void
BlockOperations::detachVolume() {
    if (volumeName) {
        // Only close the volume if it's the last connection
        std::lock_guard<std::mutex> lk(assoc_map_lock);
        auto it = assoc_map.find(*volumeName);
        if (assoc_map.end() != it) {
            if (0 == --it->second) {
                assoc_map.erase(it);
            }
        }
    }
}

void
BlockOperations::executeTask(RWTask* task) {
    task->setMaxObjectSize(maxObjectSizeInBytes);
    {   // add response that we will fill in with data
        std::unique_lock<std::mutex> l(respLock);
        if (false == responses.emplace(std::make_pair(task->getProtoTask()->getHandle(), task)).second)
            { throw BlockError::connection_closed; }
    }
    _executeTask(task);
}

void
BlockOperations::_executeTask(RWTask* task) {
    auto length = task->getLength();
    auto offset = task->getOffset();
    OffsetInfo blockRange;
    calculateOffsets(blockRange, offset, length, maxObjectSizeInBytes);
    auto const& numBlocks = blockRange.numTotalBlocks();
    task->setNumBlocks(numBlocks);
    task->setStartBlockOffset(blockRange.startBlockOffset);
    xdi_handle reqId{task->getProtoTask()->getHandle(), 0};
    bool reservedRange {false};

    TaskVisitor v;
    auto taskType = task->match(&v);
    std::string taskString;
    switch (taskType) {
    case TaskType::READ:
        taskString = "read";
        break;
    case TaskType::WRITE:
        taskString = "write";
        break;
    case TaskType::WRITESAME:
        reservedRange = true;
        taskString = "writesame";
        break;
    case TaskType::UNMAPTASK:
        reservedRange = true;
        taskString = "unmap";
        break;
    default:
        taskString = "unknown";
        break;
    }

    LOGDEBUG << "handle:" << task->getProtoTask()->getHandle()
             << " op:" << taskString
             << " startoffset:" << blockRange.startBlockOffset
             << " endoffset:" << blockRange.endBlockOffset
             << " blocks:" << numBlocks
             << " absoluteoffset:" << offset
             << " length:" << length;

    if (1 <= numBlocks) {
       ReadBlobRequest readReq;
       readReq.path.blobName = *blobName;
       readReq.path.volumeId = volumeId;
       readReq.range.startObjectOffset = blockRange.startBlockOffset;
       readReq.range.endObjectOffset = blockRange.endBlockOffset;
       if (TaskType::READ != taskType) {
           std::unique_lock<std::mutex> l(drainChainMutex);

           auto result = ctx->addReadBlob(blockRange.startBlockOffset, blockRange.endBlockOffset, task, reservedRange);
           if (WriteContext::ReadBlobResult::PENDING == result) {
               LOGDEBUG << "handle:" << task->getProtoTask()->getHandle() << " will be restarted";
               // Task will be restarted
               return;
           } else if (WriteContext::ReadBlobResult::UNAVAILABLE == result) {
               LOGDEBUG << "handle:" << task->getProtoTask()->getHandle() << " offset range unavailable";
               l.unlock();
               task->getProtoTask()->setError(ApiErrorCode::XDI_SERVICE_NOT_READY);
               finishResponse(task);
               return;
           }
       }
       Request r{reqId, RequestType::READ_BLOB_TYPE, this};
       api->readBlob(r, readReq);
   }
}

// NOTE: drainChainMutex should be held when calling this
std::pair<bool,std::shared_ptr<std::string>>
BlockOperations::drainUpdateChain
(
  RequestHandle const&          requestId,
  uint64_t const                offset
)
{
    bool haveNewObject {false};
    bool update_queued {true};
    xdi_handle queued_handle;
    BlockTask* queued_task = nullptr;

    std::tie(update_queued, queued_handle) = ctx->pop(offset);

    auto buf = ctx->getOffsetObjectBuffer(offset);
    if (nullptr == buf) {
        LOGERROR << "offset:" << offset << " no buffer found";
        return std::make_pair(haveNewObject, buf);
    }

    while (update_queued) {
        queued_task = findResponse(queued_handle.handle);
        if (queued_task) {
            LOGTRACE << "handle:" << requestId.handle << " queued:" << queued_task->getProtoTask()->getHandle() << " offset:" << offset << " draining";
            auto writeTask = static_cast<WriteTask*>(queued_task);
            auto new_data = writeTask->getBuffer(queued_handle.seq);
            if (nullptr != new_data) {
                if (maxObjectSizeInBytes > new_data->length()) {
                    new_data = writeTask->handleRMWResponse(buf,
                                                            queued_handle.seq);
                }
                buf = new_data;
                haveNewObject = true;
            }
        }

        std::tie(update_queued, queued_handle) = ctx->pop(offset);
    }

    return std::make_pair(haveNewObject, buf);
}


void BlockOperations::finishResponse
(
  BlockTask*    response
)
{
    // block connector will free resp, just accounting here
    bool response_removed;
    {
        std::unique_lock<std::mutex> l(respLock);
        response_removed = (1 == responses.erase(response->getProtoTask()->getHandle()));
    }
    if (response_removed) {
        respondTask(response);
        delete response;
    }
}

void BlockOperations::shutdown()
{
    std::unique_lock<std::mutex> l(respLock);
    if (!shutting_down) {
        shutting_down = true;
        responses.clear();
        detachVolume();
    }
}

BlockTask* BlockOperations::findResponse
(
  uint64_t handle
)
{
    std::unique_lock<std::mutex> l(respLock);
    // if we are not waiting for this response, we probably already
    // returned an error
    auto it = responses.find(handle);
    if (responses.end() == it) {
        LOGWARN << "handle:" << handle << " not waiting for response";
        return nullptr;
    }
    return it->second;
}

void BlockOperations::enqueueOperations(BlockTask* task, read_map const& r, write_map const& w) {
    for (auto& o_read : r) {
        auto readSeqId = o_read.first;
        xdi_handle reqId{task->getProtoTask()->getHandle(), readSeqId};
        Request r{reqId, RequestType::READ_OBJECT_TYPE, this};
        ReadObjectRequest req;
        req.id = o_read.second;
        req.volId = volumeId;
        api->readObject(r, req);
    }
    for (auto& o_write : w) {
        WriteObjectRequest writeReq;
        writeReq.buffer = o_write.second;
        writeReq.volId = volumeId;
        auto writeSeqId = o_write.first;
        xdi_handle reqId{task->getProtoTask()->getHandle(), writeSeqId};
        Request r{reqId, RequestType::WRITE_OBJECT_TYPE, this};
        api->writeObject(r, writeReq);
    }
}

void BlockOperations::readBlobResp
(
  RequestHandle const&           requestId,
  ReadBlobResponse const&        resp,
  ApiErrorCode const&            e
)
{
    auto task = findResponse(requestId.handle);
    if (nullptr == task) return;
    TaskVisitor v;
    if (TaskType::READ == task->match(&v)) {
        performRead(requestId, resp, e);
    } else if (TaskType::WRITE == task->match(&v)) {
        performWrite(requestId, resp, e);
    } else if (TaskType::WRITESAME == task->match(&v)) {
        performWriteSame(requestId, resp, e);
    } else if (TaskType::UNMAPTASK == task->match(&v)) {
        performUnmap(requestId, resp, e);
    }
}

void BlockOperations::performRead
(
  RequestHandle const&           requestId,
  ReadBlobResponse const&        resp,
  ApiErrorCode const&            e
)
{
    auto task = findResponse(requestId.handle);
    if (nullptr == task) return;
    auto isNewBlob = (ApiErrorCode::XDI_MISSING_BLOB == e);
    auto readTask = static_cast<ReadTask*>(task);
    auto startOffset = readTask->getStartBlockOffset();
    auto numBlocks = readTask->getNumBlocks();

    std::unique_lock<std::mutex> l(readObjectsLock);
    bool happened {false};
    auto itr = readObjects.end();
    std::tie(itr, happened) = readObjects.emplace(std::make_pair(requestId.handle, std::make_shared<read_objects>(numBlocks)));

    if (true == isNewBlob) {
        readTask->handleReadResponse(*(itr->second), empty_buffer);
        readObjects.erase(itr);
        l.unlock();
        finishResponse(task);
    } else if (ApiErrorCode::XDI_OK == e) {
        LOGDEBUG << "size:" << resp.blob.objects.size();
        for (auto const& o : resp.blob.objects) {
            LOGTRACE << "offset:" << o.first << " id:" << o.second;
        }

        read_map   objectsToRead;
        write_map  objectsToWrite;
        uint32_t   seqId = 0;
        for (auto i = startOffset; i < startOffset + numBlocks; ++i) {
            auto o_itr = resp.blob.objects.find(i);
            if (resp.blob.objects.end() == o_itr) {
                objectsToRead.emplace(seqId, EMPTY_ID);
            } else {
                objectsToRead.emplace(seqId, o_itr->second);
            }
            ++seqId;
        }
        l.unlock();
        enqueueOperations(task, objectsToRead, objectsToWrite);
    } else {
        readObjects.erase(itr);
        l.unlock();
        LOGDEBUG << "error:" << static_cast<std::underlying_type<ApiErrorCode>::type>(e) << " read blob error";
        task->getProtoTask()->setError(e);
        finishResponse(task);
    }
}

void BlockOperations::performWrite
(
  RequestHandle const&           requestId,
  ReadBlobResponse const&        resp,
  ApiErrorCode const&            e
)
{
    auto task = findResponse(requestId.handle);
    if (nullptr == task) return;
    auto isNewBlob = (ApiErrorCode::XDI_MISSING_BLOB == e);
    std::shared_ptr<std::string> bytes;
    auto writeTask = static_cast<WriteTask*>(task);
    writeTask->getWriteBuffer(bytes);
    auto length = writeTask->getLength();
    auto offset = writeTask->getOffset();
    auto startOffset = writeTask->getStartBlockOffset();
    auto endOffset = writeTask->getStartBlockOffset() + writeTask->getNumBlocks() - 1;
    writeTask->setObjectCount(writeTask->getNumBlocks());

    std::unique_lock<std::mutex> l(drainChainMutex);
    LOGDEBUG << "handle:" << requestId.handle << " numObjects:" << resp.blob.objects.size() << " startOffset:" << writeTask->getStartBlockOffset();
    if (false == ctx->addPendingWrite(startOffset, endOffset, task)) {
        LOGERROR << "unable to add pending write";
        return;
    }

    size_t amBytesWritten = 0;
    uint32_t seqId = 0;
    read_map objectsToRead;
    write_map objectsToWrite;
    while (amBytesWritten < length) {
        uint64_t curOffset = offset + amBytesWritten;
        uint64_t objectOff = curOffset / maxObjectSizeInBytes;
        uint32_t iOff = curOffset % maxObjectSizeInBytes;
        size_t iLength = length - amBytesWritten;

        if ((iLength + iOff) >= maxObjectSizeInBytes) {
            iLength = maxObjectSizeInBytes - iOff;
        }

        LOGTRACE  << "offset:" << curOffset
                  << " length:" << iLength;

        auto objBuf = (iLength == bytes->length()) ?
            bytes : std::make_shared<std::string>(*bytes, amBytesWritten, iLength);

        auto partial_write = (iLength != maxObjectSizeInBytes);
        if (true == partial_write) {
           queuePartialWrite(requestId, resp, writeTask, seqId, objectsToRead, objectsToWrite, objBuf, objectOff, iOff, isNewBlob);
        } else {
           writeTask->keepBufferForWrite(seqId, objectOff, ZERO_OFFSET, objBuf);
           xdi_handle reqId{task->getProtoTask()->getHandle(), seqId};
           auto queueResp = ctx->queue_update(objectOff, reqId);
           if (WriteContext::QueueResult::FirstEntry == queueResp) {
              ctx->setOffsetObjectBuffer(objectOff, objBuf);
              ctx->triggerWrite(objectOff);
              objectsToWrite.emplace(seqId, objBuf);
           } else if (WriteContext::QueueResult::UpdateStable == queueResp) {
               bool haveNewObject {false};
               std::shared_ptr<std::string> newBuf;
               std::tie(haveNewObject, newBuf) = drainUpdateChain(requestId, objectOff);

               if (true == haveNewObject) {
                   ctx->setOffsetObjectBuffer(objectOff, newBuf);
                   ctx->triggerWrite(objectOff);
                   objectsToWrite.emplace(seqId, newBuf);
               }
           }
           ++seqId;
        }
        amBytesWritten += iLength;
    }
    l.unlock();
    enqueueOperations(task, objectsToRead, objectsToWrite);
}

/***********************************************
** performWriteSame will trigger at most 3 object writes.
** This could require at most 2 RMWs as well as
** a single write of an object of the repeating pattern.
** Depending on the length and offset of the write,
** this could be any almost any combination of the
** three writes.
***********************************************/
void BlockOperations::performWriteSame
(
  RequestHandle const&           requestId,
  ReadBlobResponse const&        resp,
  ApiErrorCode const&            e
)
{
    auto task = findResponse(requestId.handle);
    if (nullptr == task) return;
    auto writeTask = static_cast<WriteSameTask*>(task);
    auto length = writeTask->getLength();
    auto offset = writeTask->getOffset();
    auto isNewBlob = (ApiErrorCode::XDI_MISSING_BLOB == e);
    writeTask->setObjectCount(writeTask->getNumBlocks());

    OffsetInfo newOffset;
    calculateOffsets(newOffset, offset, length, maxObjectSizeInBytes);
    GLOGIO << newOffset;

    std::unique_lock<std::mutex> l(drainChainMutex);
    LOGDEBUG << "handle:" << requestId.handle << " numObjects:" << resp.blob.objects.size() << " startOffset:" << newOffset.startBlockOffset;
    if (false == ctx->addPendingWrite(newOffset.startBlockOffset, newOffset.endBlockOffset, task)) {
        LOGERROR << "unable to add pending write";
        return;
    }

    std::shared_ptr<std::string> bytes;
    writeTask->getWriteBuffer(bytes);

    auto bufsize = bytes->size();

    // Override the number of objects will full objects being written.
    writeTask->setNumBlocks(newOffset.numFullBlocks);

    read_map objectsToRead;
    write_map objectsToWrite;

    uint32_t seqId = 0;
    // Determine if we need to write a full object
    if (0 < newOffset.numFullBlocks) {
        GLOGTRACE << "fullobjects:" << newOffset.numFullBlocks << " blockoffset:" << newOffset.fullStartBlockOffset;
        auto writeBuf = std::make_shared<std::string>();
        for (unsigned int i = 0; i < maxObjectSizeInBytes / bufsize; ++i) {
            *writeBuf += *bytes;
        }
        writeTask->keepBufferForWrite(seqId, newOffset.fullStartBlockOffset, ZERO_OFFSET, writeBuf);
        writeTask->setRepeatingBlock(seqId);
        xdi_handle reqId{task->getProtoTask()->getHandle(), seqId};
        for (unsigned int o = 0; o < newOffset.numFullBlocks; ++o) {
            auto currentOffset = newOffset.fullStartBlockOffset + o;
            auto queueResp = ctx->queue_update(currentOffset, reqId);
            if (WriteContext::QueueResult::FirstEntry == queueResp) {
                ctx->setOffsetObjectBuffer(currentOffset, writeBuf);
                ctx->triggerWrite(currentOffset);
            } else {
                LOGERROR << "handle:" << requestId.handle << " requires exclusive access to range";
                return;
            }
        }
        objectsToWrite.emplace(seqId, writeBuf);
        seqId++;
    }
    // Determine if we need a RMW for the first block
    if (0 < newOffset.startDiffOffset) {
        auto const& startBlockOffset = newOffset.startBlockOffset;
        GLOGDEBUG << "offset:" << startBlockOffset;
        auto writeBuf = std::make_shared<std::string>();
        auto writeLength = (maxObjectSizeInBytes - newOffset.startDiffOffset);
        if (true == newOffset.isSingleObject()) {
            writeLength -= newOffset.endDiffOffset;
        }
        for (unsigned int i = 0; i <  writeLength / bufsize; ++i) {
            *writeBuf += *bytes;
        }
        queuePartialWrite(requestId, resp, writeTask, seqId, objectsToRead, objectsToWrite, writeBuf, startBlockOffset, newOffset.startDiffOffset, isNewBlob);
    }
    // Determine if we need a RMW for the last block
    if (((false == newOffset.isSingleObject()) || (0 == newOffset.startDiffOffset)) && (0 < newOffset.endDiffOffset)) {
        auto const& endBlockOffset = newOffset.endBlockOffset;
        GLOGDEBUG << "offset:" << endBlockOffset;
        auto writeBuf = std::make_shared<std::string>();
        for (unsigned int i = 0; i < (maxObjectSizeInBytes - newOffset.endDiffOffset) / bufsize; ++i) {
            *writeBuf += *bytes;
        }
        queuePartialWrite(requestId, resp, writeTask, seqId, objectsToRead, objectsToWrite, writeBuf, endBlockOffset, ZERO_OFFSET, isNewBlob);
    }
    l.unlock();
    enqueueOperations(task, objectsToRead, objectsToWrite);

}

void BlockOperations::performUnmap
(
  RequestHandle const&           requestId,
  ReadBlobResponse const&        resp,
  ApiErrorCode const&            e
)
{
    auto task = findResponse(requestId.handle);
    if (nullptr == task) return;
    auto unmapTask = static_cast<UnmapTask*>(task);
    uint32_t context = 0;
    uint64_t offset;
    uint32_t length;
    auto totalStartBlockOffset = unmapTask->getStartBlockOffset();
    auto totalEndBlockOffset = unmapTask->getStartBlockOffset() + unmapTask->getNumBlocks() - 1;
    auto isNewBlob = (ApiErrorCode::XDI_MISSING_BLOB == e);

    std::unique_lock<std::mutex> l(drainChainMutex);
    LOGDEBUG << "handle:" << requestId.handle << " numObjects:" << resp.blob.objects.size() << " startOffset:" << totalStartBlockOffset;
    if (false == ctx->addPendingWrite(totalStartBlockOffset, totalEndBlockOffset, task)) {
        LOGERROR << "unable to add pending write";
        return;
    }

    read_map objectsToRead;
    write_map objectsToWrite;
    UnmapTask::block_offset_list fullObjects;
    uint32_t seqId = 0;
    while (true == unmapTask->getNextRange(context, offset, length)) {
        OffsetInfo newOffset;
        calculateOffsets(newOffset, offset, length, maxObjectSizeInBytes);
        GLOGIO << newOffset;
        if ((true == newOffset.isSingleObject()) && (length < maxObjectSizeInBytes)) {
            auto writeBuf = std::make_shared<std::string>(length, '\0');
            queuePartialWrite(requestId, resp, unmapTask, seqId, objectsToRead, objectsToWrite, writeBuf, newOffset.startBlockOffset, newOffset.startDiffOffset, isNewBlob);
        } else {
            if (true == newOffset.spansFullBlocks) {
                for (auto i = newOffset.fullStartBlockOffset; i <= newOffset.fullEndBlockOffset; ++i) {
                    fullObjects.emplace(i);
                }
            }
            if (0 < newOffset.startDiffOffset) {
                auto writeBuf = std::make_shared<std::string>(maxObjectSizeInBytes - newOffset.startDiffOffset, '\0');
                queuePartialWrite(requestId, resp, unmapTask, seqId, objectsToRead, objectsToWrite, writeBuf, newOffset.startBlockOffset, newOffset.startDiffOffset, isNewBlob);
            }
            if (0 < newOffset.endDiffOffset) {
                auto writeBuf = std::make_shared<std::string>(maxObjectSizeInBytes - newOffset.endDiffOffset, '\0');
                queuePartialWrite(requestId, resp, unmapTask, seqId, objectsToRead, objectsToWrite, writeBuf, newOffset.endBlockOffset, ZERO_OFFSET, isNewBlob);
            }
        }
    }
    if (0 < fullObjects.size()) {
        auto writeBuf = std::make_shared<std::string>(maxObjectSizeInBytes, '\0');
        unmapTask->keepBufferForWrite(seqId, *(fullObjects.begin()), ZERO_OFFSET, writeBuf);
        unmapTask->setRepeatingBlock(seqId);
        xdi_handle reqId{task->getProtoTask()->getHandle(), seqId};
        for (auto const& o : fullObjects) {
            auto queueResp = ctx->queue_update(o, reqId);
            if (WriteContext::QueueResult::FirstEntry != queueResp) {
               LOGERROR << "handle:" << requestId.handle << " requires exclusive access to range";
               return;
            }
            ctx->setOffsetObjectBuffer(o, writeBuf);
            ctx->triggerWrite(o);
        }
        unmapTask->swapFullBlockOffsets(fullObjects);
        objectsToWrite.emplace(seqId, writeBuf);
    }
    l.unlock();
    enqueueOperations(task, objectsToRead, objectsToWrite);
}

void BlockOperations::queuePartialWrite
(
  RequestHandle const& requestId,
  ReadBlobResponse const& resp,
  WriteTask* task,
  uint32_t& seqId,
  read_map& rmap,
  write_map& wmap,
  std::shared_ptr<std::string>& buf,
  uint32_t const& blockOffset,
  uint32_t const& writeOffset,
  bool const isNewBlob
)
{
    task->keepBufferForWrite(seqId, blockOffset, writeOffset, buf);
    auto o_itr = resp.blob.objects.find(blockOffset);
    xdi_handle reqId{task->getProtoTask()->getHandle(), seqId};
    auto queueResp = ctx->queue_update(blockOffset, reqId);
    if (WriteContext::QueueResult::FirstEntry == queueResp) {
        if ((resp.blob.objects.end() == o_itr) || (true == isNewBlob)) {
            rmap.emplace(seqId, EMPTY_ID);
        } else {
            rmap.emplace(seqId, o_itr->second);
        }
    } else if (WriteContext::QueueResult::UpdateStable == queueResp) {
        bool haveNewObject {false};
        std::shared_ptr<std::string> newBuf;
        std::tie(haveNewObject, newBuf) = drainUpdateChain(requestId, blockOffset);

        if (true == haveNewObject) {
            wmap.emplace(seqId, newBuf);
            ctx->setOffsetObjectBuffer(blockOffset, newBuf);
            ctx->triggerWrite(blockOffset);
        }
    }
    ++seqId;
}

void BlockOperations::writeBlobResp
(
  RequestHandle const&           requestId,
  WriteBlobResponse const&,
  ApiErrorCode const&            e
)
{
    auto task = findResponse(requestId.handle);
    if (nullptr == task) return;
    auto writeTask = static_cast<WriteTask*>(task);
    auto offset = writeTask->getOffset(requestId.seq);
    std::queue<BlockTask*> responseQueue;
    std::queue<BlockTask*> awaitingQueue;
    task->getChain(responseQueue);
    LOGTRACE << "handle:" << requestId.handle << " queuesize:" << responseQueue.size();
    respondToWrites(responseQueue, e);
    {
        std::lock_guard<std::mutex> lg(drainChainMutex);
        ctx->completeBlobWrite(offset, awaitingQueue);
    }
    if (ApiErrorCode::XDI_OK != e) {
        respondToWrites(awaitingQueue, e);
    } else {
        while (false == awaitingQueue.empty()) {
            auto t = awaitingQueue.front();
            auto rwt = static_cast<RWTask*>(t);
            awaitingQueue.pop();
            _executeTask(rwt);
        }
    }
}

void BlockOperations::readObjectResp
(
  RequestHandle const&           requestId,
  BufferPtr const&               resp,
  ApiErrorCode const&            e
)
{
    auto task = findResponse(requestId.handle);
    if (nullptr == task) return;
    TaskVisitor v;
    if ((TaskType::WRITE == task->match(&v)) ||
        (TaskType::WRITESAME == task->match(&v)) ||
        (TaskType::UNMAPTASK == task->match(&v)))  {
        auto writeTask = static_cast<WriteTask*>(task);
        auto offset = writeTask->getOffset(requestId.seq);
        auto new_data = writeTask->handleRMWResponse(resp, requestId.seq);
        bool haveNewObject {false};
        std::shared_ptr<std::string> newBuf;
        WriteObjectRequest writeReq;
        writeReq.volId = volumeId;
        {
            std::lock_guard<std::mutex> l(drainChainMutex);
            ctx->setOffsetObjectBuffer(offset, new_data);
            std::tie(haveNewObject, newBuf) = drainUpdateChain(requestId, offset);

            if (true == haveNewObject) {
                writeReq.buffer = newBuf;
                ctx->setOffsetObjectBuffer(offset, newBuf);
            } else {
                writeReq.buffer = new_data;
            }
            ctx->triggerWrite(offset);
        }
        Request r{requestId, RequestType::WRITE_OBJECT_TYPE, this};
        api->writeObject(r, writeReq);
        return;
    } else if (TaskType::READ == task->match(&v)) {
        std::unique_lock<std::mutex> l(readObjectsLock);
        auto readTask = static_cast<ReadTask*>(task);
        auto itr = readObjects.find(requestId.handle);
        if (readObjects.end() == itr) {
            LOGERROR << "handle:" << requestId.handle << " missing readObject entry";
            return;
        } else if (ApiErrorCode::XDI_OK == e) {
            (*itr->second)[requestId.seq] = resp;
        } else {
            LOGTRACE << "err:" << static_cast<std::underlying_type<ApiErrorCode>::type>(e) << " offset:" << readTask->getStartBlockOffset() + requestId.seq;
            (*itr->second)[requestId.seq] = empty_buffer;
        }
        readTask->increaseReadBlockCount();

        if (true == readTask->haveReadAllObjects()) {
            readTask->handleReadResponse(*(itr->second), empty_buffer);
            readObjects.erase(itr);
            l.unlock();
            finishResponse(task);
        }
    }
}

void BlockOperations::writeObjectResp
(
  RequestHandle const&           requestId,
  ObjectId const&                resp,
  ApiErrorCode const&            e
)
{
    auto task = findResponse(requestId.handle);
    if (nullptr == task) return;
    auto writeTask = static_cast<WriteTask*>(task);
    auto offset = writeTask->getOffset(requestId.seq);
    std::unique_lock<std::mutex> l(drainChainMutex);
    TaskVisitor v;
    if (ApiErrorCode::XDI_OK != e) {
        std::queue<BlockTask*> queue;
        ctx->failWriteBlobRequest(offset, queue);
        l.unlock();
        respondToWrites(queue, e);
    } else {
        if ((TaskType::WRITESAME == task->match(&v)) && (true == writeTask->checkRepeatingBlock(requestId.seq))) {
            for (unsigned int i = 0; i < writeTask->getNumBlocks(); ++i) {
                ctx->updateOffset(offset + i, resp);
            }
        } else if ((TaskType::UNMAPTASK == task->match(&v)) && (true == writeTask->checkRepeatingBlock(requestId.seq))) {
            UnmapTask::block_offset_list fullObjects;
            auto unmapTask = static_cast<UnmapTask*>(task);
            unmapTask->swapFullBlockOffsets(fullObjects);
            for (auto const& o : fullObjects) {
                ctx->updateOffset(o, resp);
            }
        } else {
            ctx->updateOffset(offset, resp);
        }
        LOGDEBUG << "handle:" << requestId.handle << " objectId:" << resp << " offset:" << offset;

        // Unblock other updates on the same object if they exist
        bool haveNewObject {false};
        std::shared_ptr<std::string> newBuf;
        std::tie(haveNewObject, newBuf) = drainUpdateChain(requestId, offset);

        if (true == haveNewObject) {
            WriteObjectRequest writeReq;
            writeReq.volId = volumeId;
            writeReq.buffer = newBuf;
            ctx->setOffsetObjectBuffer(offset, newBuf);
            ctx->triggerWrite(offset);
            l.unlock();
            Request r{requestId, RequestType::WRITE_OBJECT_TYPE, this};
            api->writeObject(r, writeReq);
            return;
        } else {
            WriteBlobRequest req;
            std::queue<BlockTask*> queue;
            if (true == ctx->getWriteBlobRequest(offset, req, queue)) {
                LOGDEBUG << "numobjects:" << req.blob.objects.size();
                for (auto const& o : req.blob.objects) {
                    LOGTRACE << "offset:" << o.first << " id:" << o.second.objectId;
                }
                task->setChain(std::move(queue));
                l.unlock();
                Request r{requestId, RequestType::WRITE_BLOB_TYPE, this};
                api->writeBlob(r, req);
            }
        }
    }
}

void BlockOperations::respondToWrites
(
  std::queue<BlockTask*>&   q,
  ApiErrorCode const&       e
)
{
    while (false == q.empty()) {
        auto t = q.front();
        q.pop();
        LOGTRACE << "handle:" << t->getProtoTask()->getHandle() << " responding";
        t->getProtoTask()->setError(e);
        finishResponse(t);
    }
}

}  // namespace block
}  // namespace fds
