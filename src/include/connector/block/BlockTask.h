/*
 * BlockTask.h
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

#ifndef BLOCKTASK_H_
#define BLOCKTASK_H_

// System includes
#include <atomic>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <queue>

#include "ProtoTask.h"

namespace fds {
namespace block {

struct RWTask;
struct ReadTask;
struct WriteTask;
struct WriteSameTask;
struct UnmapTask;

enum class TaskType { READ, WRITE, WRITESAME, UNMAPTASK };

struct TaskVisitor {
    virtual TaskType matchRead(ReadTask*) const { return TaskType::READ; }
    virtual TaskType matchWrite(WriteTask*) const { return TaskType::WRITE; }
    virtual TaskType matchWriteSame(WriteSameTask*) const { return TaskType::WRITESAME; }
    virtual TaskType matchUnmap(UnmapTask*) const { return TaskType::UNMAPTASK; }
};

/**
 * A BlockTask represents a single READ/WRITE operation from a storage
 * interface to a block device. The operation may encompass less than or
 * greater than a single object. This class helps deal with the buffers and
 * offset/length calculations needed during asynchronous i/o
 */
struct BlockTask {
    using buffer_type = std::string;
    using buffer_ptr_type = std::shared_ptr<buffer_type>;
    using sequence_type = uint32_t;

    explicit BlockTask(ProtoTask* p_task) : protoTask(p_task) {}
    virtual ~BlockTask() = default;

    uint32_t maxObjectSize() const { return maxObjectSizeInBytes; }

    /// Task setters
    void setMaxObjectSize(uint32_t const size) { maxObjectSizeInBytes = size; }

    void setNumBlocks(uint32_t const b) { numBlocks = b; }
    uint32_t getNumBlocks() { return numBlocks; }

    void setStartBlockOffset(uint32_t const b) { startBlockOffset = b; }
    uint32_t getStartBlockOffset() { return startBlockOffset; }

    void getChain(std::queue<BlockTask*>& q) { q.swap(chainedResponses); }
    void setChain(std::queue<BlockTask*>&& q) { chainedResponses.swap(q); }

    virtual TaskType match(const TaskVisitor* v) = 0;

    ProtoTask* getProtoTask() { return protoTask; }
    void setError(xdi::ApiErrorCode const& error) { if (nullptr != protoTask) protoTask->setError(error); }

  private:
    ProtoTask* protoTask;

    std::queue<BlockTask*>        chainedResponses;

  protected:
    // offset
    uint32_t maxObjectSizeInBytes {0};
    uint32_t numBlocks {0};
    uint32_t startBlockOffset {0};
};

}  // namespace block
}  // namespace fds

#endif  // BLOCKTASK_H_
