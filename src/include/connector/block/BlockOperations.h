/*
 * BlockOperations.h
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

#ifndef BLOCKOPERATIONS_H_
#define BLOCKOPERATIONS_H_

#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <memory>
#include <queue>

#include <boost/lockfree/queue.hpp>

#include <xdi/ApiTypes.h>
#include <xdi/ApiResponseInterface.h>
#include "BlockTask.h"
#include "BlockTools.h"

#define EMPTY_ID "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"

namespace xdi {
    class ApiInterface;
}

namespace fds {
namespace block {

class WriteContext;

enum class BlockError : uint8_t {
    connection_closed,
    shutdown_requested,
};

/**
 * The BlockOperations class provides a simple interface to a dynamic connector
 * allowing block like semantics. The interface consists of three main calls to
 * attach the volume, read data and write data. The RMW logic and operation
 * rollup all happens in here allowing block connectors to issue their requests
 * as fast as possible without having to deal with consistency themselves and
 * map I/O to XdiAsyncDataApi calls.
 */
class BlockOperations
    :   public xdi::ApiResponseInterface
{
    using string_ptr = std::shared_ptr<std::string>;
    using xdi_error = xdi::ApiErrorCode;
    using xdi_handle = xdi::RequestHandle;
    using task_type = BlockTask;

    using read_objects = std::vector<std::shared_ptr<std::string>>;

    using read_map = std::map<uint32_t, xdi::ObjectId>;
    using write_map = std::map<uint32_t, std::shared_ptr<std::string>>;

    typedef std::unordered_map<int64_t, task_type*> response_map_type;
  public:

    BlockOperations(std::shared_ptr<xdi::ApiInterface> interface, size_t const pool_size);
    explicit BlockOperations(BlockOperations const& rhs) = delete;
    BlockOperations& operator=(BlockOperations const& rhs) = delete;
    ~BlockOperations() = default;

    void init(std::string                      vol_name,
              uint64_t const                   vol_id,
              uint32_t const                   obj_size);
    void detachVolume();

    void executeTask(RWTask* task);

    void shutdown();

    virtual void respondTask(task_type* response) = 0;

    void listResp(xdi_handle const&, xdi::ListBlobsResponse const&, xdi_error const&) override {};
    void enumBlobsResp(xdi_handle const&, xdi::EnumBlobsResponse const&, xdi_error const&) override {};
    void readVolumeMetaResp(xdi_handle const&, xdi::VolumeMetadata const&, xdi_error const&) override { }
    void writeVolumeMetaResp(xdi_handle const&, bool const&, xdi_error const&) override {}
    void readBlobResp(xdi_handle const& requestId, xdi::ReadBlobResponse const& resp, xdi_error const& e) override;
    void writeBlobResp(xdi_handle const& requestId, xdi::WriteBlobResponse const& resp, xdi_error const& e) override;
    void upsertBlobMetadataCasResp(xdi_handle const&, bool const&, xdi_error const&) override {};
    void upsertBlobObjectCasResp(xdi_handle const&, bool const&, xdi_error const&) override {};
    void readObjectResp(xdi_handle const& requestId, xdi::BufferPtr const& resp, xdi_error const& e) override;
    void writeObjectResp(xdi_handle const& requestId, xdi::ObjectId const& resp, xdi_error const& e) override;
    void deleteBlobResp(xdi_handle const&, bool const&, xdi_error const&) override {};
    void diffBlobResp(xdi_handle const&, xdi::DiffBlobResponse const&, xdi_error const&) override {};
    void diffAllBlobsResp(xdi_handle const&, xdi::DiffAllBlobsResponse const&, xdi_error const&) override {};
    void diffVolumesResp(xdi_handle const&, xdi::DiffVolumesResponse const&, xdi_error const&) override {};
    void statVolumeResp(xdi_handle const&, xdi::VolumeStatusPtr const&, xdi_error const&) override {};
    void listAllVolumesResp(xdi_handle const&, xdi::ListAllVolumesResponse const&, xdi_error const&) override {};

  protected:

    boost::lockfree::queue<ReadTask*> read_task_pool;
    boost::lockfree::queue<WriteTask*> write_task_pool;

    ReadTask* acquireReadTask(ProtoTask* p_task);
    WriteTask* acquireWriteTask(ProtoTask* p_task);

    void returnReadTask(ReadTask* task)     { read_task_pool.push(task); }
    void returnWriteTask(WriteTask* task)   { write_task_pool.push(task); }

  private:
    void finishResponse(task_type* response);

    std::pair<bool,std::shared_ptr<std::string>>
     drainUpdateChain(xdi_handle const&             requestId,
                      uint64_t const                offset);

    BlockTask* findResponse(uint64_t handle);

    void respondToWrites(std::queue<BlockTask*>& q, xdi_error const& e);

    void _executeTask(RWTask* task);

    void enqueueOperations(BlockTask* task, read_map const& r, write_map const& w);

    void performRead
    (
      xdi_handle const&              requestId,
      xdi::ReadBlobResponse const&   resp,
      xdi_error const&               e
    );

    void performWrite
    (
      xdi_handle const&              requestId,
      xdi::ReadBlobResponse const&   resp,
      xdi_error const&               e
    );

    void performWriteSame
    (
      xdi_handle const&              requestId,
      xdi::ReadBlobResponse const&   resp,
      xdi_error const&               e
    );

    void performUnmap
    (
      xdi_handle const&              requestId,
      xdi::ReadBlobResponse const&   resp,
      xdi_error const&               e
    );

    void queuePartialWrite
    (
      xdi_handle const& requestId,
      xdi::ReadBlobResponse const& resp,
      WriteTask* task,
      uint32_t& seqId,
      read_map& rmap,
      write_map& wmap,
      std::shared_ptr<std::string>& buf,
      uint32_t const& blockOffset,
      uint32_t const& writeOffset,
      bool const isNewBlob
    );

    // api we've built
    string_ptr              volumeName;
    uint64_t                volumeId;
    string_ptr              empty_buffer;
    uint32_t                maxObjectSizeInBytes {0};

    bool shutting_down {false};

    // for all reads/writes to AM
    string_ptr             blobName;
    string_ptr             domainName;
    std::shared_ptr<int32_t>    blobMode;
    std::shared_ptr< std::map<std::string, std::string> > emptyMeta;

    std::shared_ptr<xdi::ApiInterface>      api;
    std::shared_ptr<WriteContext>           ctx;

    std::mutex readObjectsLock;
    std::unordered_map<uint64_t, std::shared_ptr<read_objects>>      readObjects;

    // keep current handles for which we are waiting responses
    std::mutex respLock;
    response_map_type responses;

    // Make sure only one thread is draining the update chain at a time
    std::mutex        drainChainMutex;
};

}  // namespace block
}  // namespace fds

#endif  // BLOCKOPERATIONS_H_
