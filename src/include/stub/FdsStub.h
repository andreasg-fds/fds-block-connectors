/*
 * FdsStub.h
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
#ifndef FDS_STUB_H_
#define FDS_STUB_H_

// System includes
#include <mutex>
#include <unordered_map>

// FDS includes
#include "xdi/ApiTypes.h"

namespace xdi {

class FdsStub {
public:
    FdsStub();
    ~FdsStub();

    ObjectId writeObject(WriteObjectRequest const& req);
    ApiErrorCode readObject(ReadObjectRequest const& req, std::shared_ptr<std::string>& buf);
    ApiErrorCode writeBlob(WriteBlobRequest const& req);
    ApiErrorCode readBlob(ReadBlobRequest const& req, ReadBlobResponse& resp);
    ApiErrorCode upsertBlobObjectCas(UpsertBlobObjectCasRequest const& req, bool& happened);
    ApiErrorCode deleteBlob(BlobPath const& path, bool& happened);
    ApiErrorCode getAllVolumes(ListAllVolumesResponse& resp);

    void addVolume(std::shared_ptr<VolumeDescriptorBase> vol);

    int getNumObjects();
    int getNumBlobs();
    int getNumVolumes();

private:

    uint64_t                                                    _nextId;
    std::unordered_map<ObjectId, std::shared_ptr<std::string>>  _objects;
    std::unordered_map<BlobPath, ObjectWriteMap, BlobPathHasher>     _blobs;

    std::vector<std::shared_ptr<VolumeDescriptorBase>>          _volumes;

    std::mutex                                                  _objectsLock;
    std::mutex                                                  _blobsLock;
};

} // namespace xdi

#endif //FDS_STUB_H_
