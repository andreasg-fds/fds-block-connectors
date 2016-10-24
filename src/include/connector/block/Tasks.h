/*
 * Tasks.h
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

#ifndef TASKS_H_
#define TASKS_H_

// System includes
#include <set>

// FDS includes
#include "BlockTask.h"

namespace fds {
namespace block {

struct RWTask : public BlockTask {
    RWTask(ProtoTask* p_task) : BlockTask(p_task)
    {
        bufVec.reserve(1);
        offVec.reserve(1);
    }
    virtual ~RWTask() = default;
    virtual TaskType match(const TaskVisitor* v) = 0;

    void set(uint64_t const off, uint32_t const bytes) {
        offset = off;
        length = bytes;
    }
    uint64_t getOffset() const  { return offset; }
    uint32_t getLength() const  { return length; }

    /// Sub-task operations
    uint64_t getOffset(sequence_type const seqId) const          { return offVec[seqId]; }
    buffer_ptr_type getBuffer(sequence_type const seqId) const   { return bufVec[seqId]; }

    void setObjectCount(size_t const count) {
        bufVec.reserve(count);
        offVec.reserve(count);
    }

private:
    uint64_t offset {0};
    uint32_t length {0};

protected:
    std::vector<buffer_ptr_type>   bufVec;
    std::vector<uint64_t>          offVec;
};

struct ReadTask : public RWTask {
    ReadTask(ProtoTask* p_task) : RWTask(p_task) {}
    virtual TaskType match(const TaskVisitor* v) { return v->matchRead(this); }

    /// Buffer operations
    buffer_ptr_type getNextReadBuffer(uint32_t& context) {
        if (context >= bufVec.size()) {
            return nullptr;
        }
        return bufVec[context++];
    }

    void swapReadBuffers(std::vector<buffer_ptr_type>& vec) {
        vec.swap(bufVec);
    }

    void increaseReadBlockCount() { ++readObjectCount; }
    bool haveReadAllObjects() { return numBlocks == readObjectCount; }

    /**
     * \return true if all responses were received or operation error
     */
    void handleReadResponse(std::vector<buffer_ptr_type>& buffers,
                            buffer_ptr_type& empty_buffer);

private:
    uint32_t readObjectCount {0};
};

struct WriteTask : public RWTask {
    WriteTask(ProtoTask* p_task) : RWTask(p_task) {}
    virtual TaskType match(const TaskVisitor* v) { return v->matchWrite(this); }

    void setWriteBuffer(std::shared_ptr<std::string>& buf) { writeBuffer = buf; }
    void getWriteBuffer(std::shared_ptr<std::string>& buf) { buf = writeBuffer; }

    void keepBufferForWrite(sequence_type const seqId,
                            uint64_t const objectOff,
                            uint32_t const writeOffset,
                            buffer_ptr_type& buf) {
        bufVec.emplace_back(buf);
        offVec.emplace_back(objectOff);
        if (0 != writeOffset) writeOffsetInBlockMap.emplace(seqId, writeOffset);
    }

    void setRepeatingBlock(sequence_type const seqId) { repeatingBlock = seqId; hasRepeatingBlock = true; }
    bool checkRepeatingBlock(sequence_type const seqId) {
        if (false == hasRepeatingBlock) return false;
        else return (seqId == repeatingBlock) ? true : false;
    }

    /**
     * Handle read response for read-modify-write
     * \return true if all responses were received or operation error
     */
    buffer_ptr_type
        handleRMWResponse(buffer_ptr_type const& retBuf, sequence_type seqId);

private:
    std::shared_ptr<std::string>  writeBuffer;

    // Track offset inside block if not aligned
    std::unordered_map<sequence_type, uint32_t>   writeOffsetInBlockMap;

    sequence_type  repeatingBlock {0};
    bool           hasRepeatingBlock {false};
};

struct WriteSameTask : public WriteTask {
    WriteSameTask(ProtoTask* p_task) : WriteTask(p_task) {}
    virtual TaskType match(const TaskVisitor* v) { return v->matchWriteSame(this); }
};

struct UnmapTask : public WriteTask {
    struct UnmapRange {
        uint64_t offset;
        uint32_t length;
    };
    using unmap_vec = std::vector<UnmapRange>;
    using unmap_vec_ptr = std::unique_ptr<unmap_vec>;
    using block_offset_list = std::set<uint32_t>;

    UnmapTask(ProtoTask* p_task, std::unique_ptr<std::vector<UnmapRange>>&& wv) :
        WriteTask(p_task),
        write_vec(std::move(wv))
    {
        uint64_t startOffset {0};
        uint64_t endOffset {0};
        if (false == write_vec->empty()) {
            startOffset = (*write_vec)[0].offset;
            endOffset = startOffset + (*write_vec)[0].length - 1;
            for (auto const& w : *write_vec) {
                if (w.offset < startOffset) startOffset = w.offset;
                if (w.offset + w.length > endOffset) endOffset = w.offset + w.length - 1;
            }
            set(startOffset, endOffset - startOffset + 1);
        } else {
            set(startOffset, endOffset);
        }

    }
    virtual TaskType match(const TaskVisitor* v) { return v->matchUnmap(this); }

    bool getNextRange(uint32_t& context, uint64_t& offset, uint32_t& length) {
        if (context >= write_vec->size()) return false;
        offset = (*write_vec)[context].offset;
        length = (*write_vec)[context].length;
        ++context;
        return true;
    }

    void swapFullBlockOffsets(block_offset_list& offsets) {
        fullBlockOffsets.swap(offsets);
    }

private:
    unmap_vec_ptr       write_vec;
    block_offset_list   fullBlockOffsets;
};

}  // namespace block
}  // namespace fds

#endif // TASKS_H_
