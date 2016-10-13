/**
 * Copyright 2016 by Formation Data Systems, Inc.
 */
#include <thread>
#include <chrono>
#include <iostream>
#include "xdi/ApiTypes.h"
#include "stub/ApiStub.h"
#include "xdi/ApiResponseInterface.h"

namespace xdi {

ApiStub::ApiStub(std::shared_ptr<FdsStub> stub, uint32_t delay) :
        _stub(stub),
        _delay(delay)
{
}

void ApiStub::delay() {
    std::this_thread::sleep_for(std::chrono::milliseconds(_delay));
}

void ApiStub::list(Request const& requestId, ListBlobsRequest const& request) {
    delay();
    ListBlobsResponse resp;
    ApiErrorCode err = ApiErrorCode::XDI_OK;
    if (nullptr != requestId.resp) {
        requestId.resp->listResp(requestId.id, resp, err);
    }
}

void ApiStub::readVolumeMeta(Request const& requestId) {
    delay();
    VolumeMetadata resp;
    ApiErrorCode err = ApiErrorCode::XDI_OK;
    if (nullptr != requestId.resp) {
        requestId.resp->readVolumeMetaResp(requestId.id, resp, err);
    }
}

void ApiStub::writeVolumeMeta(Request const& requestId, VolumeMetadata const& metadata) {
    delay();
    ApiErrorCode err = ApiErrorCode::XDI_OK;
    if (nullptr != requestId.resp) {
        requestId.resp->writeVolumeMetaResp(requestId.id, err);
    }
}

void ApiStub::readBlob(Request const& requestId, ReadBlobRequest const& request) {
    delay();
    ReadBlobResponse resp;
    auto err = _stub->readBlob(request, resp);
    if (nullptr != requestId.resp) {
        requestId.resp->readBlobResp(requestId.id, resp, err);
    }
}

void ApiStub::writeBlob(Request const& requestId, WriteBlobRequest const& request) {
    delay();
    WriteBlobResponse resp;
    auto err = _stub->writeBlob(request);
    if (nullptr != requestId.resp) {
        requestId.resp->writeBlobResp(requestId.id, resp, err);
    }
}

void ApiStub::upsertBlobMetadataCas(Request const& requestId, UpsertBlobMetadataCasRequest const& request) {
    delay();
    bool resp {true};
    ApiErrorCode err = ApiErrorCode::XDI_OK;
    if (nullptr != requestId.resp) {
        requestId.resp->upsertBlobMetadataCasResp(requestId.id, resp, err);
    }
}

void ApiStub::upsertBlobObjectCas(Request const& requestId, UpsertBlobObjectCasRequest const& request) {
    delay();
    bool resp {true};
    auto err = _stub->upsertBlobObjectCas(request, resp);
    if (nullptr != requestId.resp) {
        requestId.resp->upsertBlobObjectCasResp(requestId.id, resp, err);
    }
}

void ApiStub::readObject(Request const& requestId, ReadObjectRequest const& request) {
    delay();
    auto resp = std::make_shared<std::string>();
    auto err = _stub->readObject(request, resp);
    requestId.resp->readObjectResp(requestId.id, resp, err);
}

void ApiStub::writeObject(Request const& requestId, WriteObjectRequest const& request) {
    delay();
    ApiErrorCode err = ApiErrorCode::XDI_OK;
    auto resp = _stub->writeObject(request);
    if (nullptr != requestId.resp) {
        requestId.resp->writeObjectResp(requestId.id, resp, err);
    }
}

void ApiStub::deleteBlob(Request const& requestId, BlobPath const& target) {
    delay();
    bool resp {true};
    auto err = _stub->deleteBlob(target, resp);
    if (nullptr != requestId.resp) {
        requestId.resp->deleteBlobResp(requestId.id, resp, err);
    }
}

void ApiStub::diffBlob(Request const& requestId, DiffBlobRequest const& request) {
    delay();
    DiffBlobResponse resp;
    ApiErrorCode err = ApiErrorCode::XDI_OK;
    if (nullptr != requestId.resp) {
        requestId.resp->diffBlobResp(requestId.id, resp, err);
    }
}

void ApiStub::diffAllBlobs(Request const& requestId, DiffAllBlobsRequest const& request) {
    delay();
    DiffAllBlobsResponse resp;
    ApiErrorCode err = ApiErrorCode::XDI_OK;
    if (nullptr != requestId.resp) {
        requestId.resp->diffAllBlobsResp(requestId.id, resp, err);
    }
}

void ApiStub::diffVolumes(Request const& requestId, DiffVolumesRequest const& request) {
    delay();
    DiffVolumesResponse resp;
    ApiErrorCode err = ApiErrorCode::XDI_OK;
    if (nullptr != requestId.resp) {
        requestId.resp->diffVolumesResp(requestId.id, resp, err);
    }
}

void ApiStub::statVolume(Request const& requestId, VolumeId const volumeId) {
    auto resp = std::make_shared<VolumeStatus>();
    ApiErrorCode err = ApiErrorCode::XDI_OK;
    if (nullptr != requestId.resp) {
        requestId.resp->statVolumeResp(requestId.id, resp, err);
    }
}

void ApiStub::listAllVolumes(Request const& requestId, ListAllVolumesRequest const& request) {
    std::cout << "ListAllVolumes" << std::endl;
    ListAllVolumesResponse resp;
    ApiErrorCode err = ApiErrorCode::XDI_OK;
    _stub->getAllVolumes(resp);
    std::cout << "Got volumes" << std::endl;
    if (nullptr != requestId.resp) {
        requestId.resp->listAllVolumesResp(requestId.id, resp, err);
    }
}

AsyncApiStub::AsyncApiStub(std::shared_ptr<FdsStub> stub, uint32_t delay) : ApiStub(stub, delay)
{
}

void AsyncApiStub::list(Request const& requestId, ListBlobsRequest const& request) {
    std::thread t([this, requestId, request]() {
        ApiStub::list(requestId, request);
    });
    t.detach();
}

void AsyncApiStub::readBlob(Request const& requestId, ReadBlobRequest const& request) {
    std::thread t([this, requestId, request]() {
        ApiStub::readBlob(requestId, request);
    });
    t.detach();
}

void AsyncApiStub::writeBlob(Request const& requestId, WriteBlobRequest const& request) {
    std::thread t([this, requestId, request]() {
        ApiStub::writeBlob(requestId, request);
    });
    t.detach();
}

void AsyncApiStub::upsertBlobMetadataCas(Request const& requestId, UpsertBlobMetadataCasRequest const& request) {
    std::thread t([this, requestId, request]() {
        ApiStub::upsertBlobMetadataCas(requestId, request);
    });
    t.detach();
}

void AsyncApiStub::upsertBlobObjectCas(Request const& requestId, UpsertBlobObjectCasRequest const& request) {
    std::thread t([this, requestId, request]() {
        ApiStub::upsertBlobObjectCas(requestId, request);
    });
    t.detach();
}

void AsyncApiStub::readObject(Request const& requestId, ReadObjectRequest const& request) {
    std::thread t([this, requestId, request]() {
        ApiStub::readObject(requestId, request);
    });
    t.detach();
}

void AsyncApiStub::writeObject(Request const& requestId, WriteObjectRequest const& request) {
    std::thread t([this, requestId, request]() {
        ApiStub::writeObject(requestId, request);
    });
    t.detach();
}

void AsyncApiStub::deleteBlob(Request const& requestId, BlobPath const& target) {
    std::thread t([this, requestId, target]() {
        ApiStub::deleteBlob(requestId, target);
    });
    t.detach();
}

void AsyncApiStub::diffBlob(Request const& requestId, DiffBlobRequest const& request) {
    std::thread t([this, requestId, request]() {
        ApiStub::diffBlob(requestId, request);
    });
    t.detach();
}

void AsyncApiStub::diffAllBlobs(Request const& requestId, DiffAllBlobsRequest const& request) {
    std::thread t([this, requestId, request]() {
        ApiStub::diffAllBlobs(requestId, request);
    });
    t.detach();
}

void AsyncApiStub::diffVolumes(Request const& requestId, DiffVolumesRequest const& request) {
    std::thread t([this, requestId, request]() {
        ApiStub::diffVolumes(requestId, request);
    });
    t.detach();
}

void AsyncApiStub::statVolume(Request const& requestId, VolumeId const volumeId) {
    std::thread t([this, requestId, volumeId]() {
        ApiStub::statVolume(requestId, volumeId);
    });
    t.detach();
}

void AsyncApiStub::listAllVolumes(Request const& requestId, ListAllVolumesRequest const& request) {
    std::thread t([this, requestId, request]() {
        ApiStub::listAllVolumes(requestId, request);
    });
    t.detach();
}

} // namespace xdi
