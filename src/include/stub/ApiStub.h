/*
 * ApiStub.h
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
#ifndef SRC_APISTUB_H
#define SRC_APISTUB_H

#include "xdi/ApiTypes.h"
#include "stub/FdsStub.h"

namespace xdi {

class ApiStub : public ApiInterface {

public:
    ApiStub(std::shared_ptr<FdsStub> stub, uint32_t delay);

    void list(Request const& requestId, ListBlobsRequest const& request) override;
    void readVolumeMeta(Request const& requestId, VolumeId const volumeId) override;
    void writeVolumeMeta(Request const& requestId, WriteMetadataRequest const& request) override;
    void readBlob(Request const& requestId, ReadBlobRequest const& request) override;
    void writeBlob(Request const& requestId, WriteBlobRequest const& request) override;
    void upsertBlobMetadataCas(Request const& requestId, UpsertBlobMetadataCasRequest const& request) override;
    void upsertBlobObjectCas(Request const& requestId, UpsertBlobObjectCasRequest const& request) override;
    void readObject(Request const& requestId, ReadObjectRequest const& request) override;
    void writeObject(Request const& requestId, WriteObjectRequest const& request) override;
    void deleteBlob(Request const& requestId, BlobPath const& target) override;
    void statVolume(Request const& requestId, VolumeId const volumeId) override;
    void listAllVolumes(Request const& requestId, ListAllVolumesRequest const& request) override;
private:
    std::shared_ptr<FdsStub>                  _stub;
    uint32_t                                  _delay;

    void delay();
};

// AsyncApiStub is a wrapper around ApiStub
// that spawns each call off in a new thread and
// then detaches from it.  This makes the response call
// to come back on a separate thread and allow the request
// call to immediately return.
class AsyncApiStub : public ApiStub {

public:
    AsyncApiStub(std::shared_ptr<FdsStub> stub, uint32_t delay);

    virtual void list(Request const& requestId, ListBlobsRequest const& request) override;
    virtual void readBlob(Request const& requestId, ReadBlobRequest const& request) override;
    virtual void writeBlob(Request const& requestId, WriteBlobRequest const& request) override;
    virtual void upsertBlobMetadataCas(Request const& requestId, UpsertBlobMetadataCasRequest const& request) override;
    virtual void upsertBlobObjectCas(Request const& requestId, UpsertBlobObjectCasRequest const& request) override;
    virtual void readObject(Request const& requestId, ReadObjectRequest const& request) override;
    virtual void writeObject(Request const& requestId, WriteObjectRequest const& request) override;
    virtual void deleteBlob(Request const& requestId, BlobPath const& target) override;
    virtual void statVolume(Request const& requestId, VolumeId const volumeId) override;
    virtual void listAllVolumes(Request const& requestId, ListAllVolumesRequest const& request) override;
};

} // namespace xdi

#endif //SRC_APISTUB_H
