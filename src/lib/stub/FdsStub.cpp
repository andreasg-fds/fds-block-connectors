/**
 * Copyright 2016 by Formation Data Systems, Inc.
 */
//System includes
#include <string>

//FDS includes
#include "stub/FdsStub.h"

namespace xdi {

FdsStub::FdsStub() : _nextId(0) {

}

FdsStub::~FdsStub() {

}

ObjectId FdsStub::writeObject(WriteObjectRequest const& req) {
    std::lock_guard<std::mutex> lg(_objectsLock);
    _objects.emplace(std::make_pair(std::to_string(++_nextId), req.buffer));
    return std::to_string(_nextId);
}

ApiErrorCode FdsStub::readObject(ReadObjectRequest const& req, std::shared_ptr<std::string>& buf) {
    std::lock_guard<std::mutex> lg(_objectsLock);
    if (req.id == "0000000000000000000000000000000000000000") {
        return ApiErrorCode::XDI_OK;
    }
    auto itr = _objects.find(req.id);
    if (_objects.end() != itr) {
        buf = itr->second;
        return ApiErrorCode::XDI_OK;
    }
    return ApiErrorCode::XDI_MISSING_OBJECT;
}

ApiErrorCode FdsStub::writeBlob(WriteBlobRequest const& req) {
    ApiErrorCode ret = ApiErrorCode::XDI_OK;
    std::lock_guard<std::mutex> lg(_blobsLock);
    auto itr = _blobs.find(req.blob.blobInfo.path);
    if (_blobs.end() != itr) { // Blob exists, this is an update
        for (auto off : req.blob.objects) {
            // We don't write a null object
            if (off.second.objectId == "0000000000000000000000000000000000000000") {
                continue;
            }

            auto existingOffset = itr->second.find(off.first);
            if (itr->second.end() == existingOffset) { // Blob does not contain this offset
                itr->second.emplace(off.first, off.second);
            } else { // Blob has an entry for this offset
                existingOffset->second = off.second;
            }
        }

        if (true == req.blob.shouldTruncate) {
            ObjectOffsetVal maxOffset = 0;
            if (0 < req.blob.objects.size()) {
                maxOffset = (*req.blob.objects.crbegin()).first + 1;
            }
            auto o_itr = itr->second.begin();
            while (itr->second.end() != o_itr) {
                if (o_itr->first >= maxOffset) {
                    itr->second.erase(o_itr, itr->second.end());
                    break;
                }
                ++o_itr;
            }
        }
    } else { // This is a new Blob
        ObjectWriteMap newMap;
        for (auto off : req.blob.objects) {
            if (off.second.objectId == "0000000000000000000000000000000000000000") {
                continue;
            } else {
                newMap.emplace(off.first, off.second);
            }
        }
        auto inserted = _blobs.emplace(std::make_pair(req.blob.blobInfo.path, std::move(newMap)));
        if (false == inserted.second) {
            ret = ApiErrorCode::XDI_INTERNAL_SERVER_ERROR;
        }
    }
    return ret;
}

ApiErrorCode FdsStub::readBlob(ReadBlobRequest const& req, ReadBlobResponse& resp) {
    std::lock_guard<std::mutex> lg(_blobsLock);
    resp.blob.stat.size = 0;
    resp.blob.stat.blobInfo.path = req.path;
    ApiErrorCode ret = ApiErrorCode::XDI_OK;
    auto itr = _blobs.find(req.path);
    if (_blobs.end() != itr) {
        for (auto i = req.range.startObjectOffset; i <= req.range.endObjectOffset; ++i) {
            auto offsetItr = itr->second.find(i);
            if (itr->second.end() != offsetItr) {
                resp.blob.objects.emplace(i, offsetItr->second.objectId);
            }
        }
    } else {
        ret = ApiErrorCode::XDI_MISSING_BLOB;
    }
    return ret;
}

ApiErrorCode FdsStub::upsertBlobObjectCas(UpsertBlobObjectCasRequest const& req, bool& happened) {
    std::lock_guard<std::mutex> lg(_blobsLock);
    ApiErrorCode ret = ApiErrorCode::XDI_OK;
    happened = false;
    auto b_itr = _blobs.find(req.path);
    if (_blobs.end() != b_itr) {
        auto o_itr = b_itr->second.find(req.preconditionOffset);
        if (b_itr->second.end() !=  o_itr) {
            if (o_itr->second.objectId == req.preconditionRequiredObjectId) {
                b_itr->second.erase(o_itr);
                b_itr->second.emplace(req.preconditionOffset, req.objectId);
                happened = true;
            }
        }
    } else {
        ret = ApiErrorCode::XDI_MISSING_BLOB;
    }
    return ret;
}

ApiErrorCode FdsStub::deleteBlob(BlobPath const& path, bool& happened) {
    std::lock_guard<std::mutex> lg(_blobsLock);
    ApiErrorCode ret = ApiErrorCode::XDI_OK;
    auto b_itr = _blobs.find(path);
    if (_blobs.end() != b_itr) {
        _blobs.erase(b_itr);
        happened = true;
    } else {
        happened = false;
    }
    return ret;
}

ApiErrorCode FdsStub::getAllVolumes(ListAllVolumesResponse& resp) {
    ApiErrorCode ret = ApiErrorCode::XDI_OK;
    resp.volumes = _volumes;
    return ret;
}

void FdsStub::addVolume(std::shared_ptr<VolumeDescriptorBase> vol) {
    _volumes.push_back(vol);
}

int FdsStub::getNumObjects() {
    std::lock_guard<std::mutex> lg(_objectsLock);
    return _objects.size();
}

int FdsStub::getNumBlobs() {
    std::lock_guard<std::mutex> lg(_blobsLock);
    return _blobs.size();
}

int FdsStub::getNumVolumes() {
    return _volumes.size();
}

} // namespace xdi
