#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "stub/ApiStub.h"
#include "xdi/ApiResponseInterface.h"

using ::testing::_;
using ::testing::Pointee;

class MockResponseInterface : public xdi::ApiResponseInterface {
public:
    MOCK_METHOD3(statVolumeResp, void(xdi::RequestHandle const& requestId, xdi::VolumeStatusPtr const& resp, xdi::ApiErrorCode const& e));
    MOCK_METHOD3(listAllVolumesResp, void(xdi::RequestHandle const& requestId, xdi::ListAllVolumesResponse const& resp, xdi::ApiErrorCode const& e));
    MOCK_METHOD3(listResp, void(xdi::RequestHandle const& requestId, xdi::ListBlobsResponse const& resp, xdi::ApiErrorCode const& e));
    MOCK_METHOD3(readBlobResp, void(xdi::RequestHandle const& requestId, xdi::ReadBlobResponse const& resp, xdi::ApiErrorCode const& e));
    MOCK_METHOD3(writeBlobResp, void(xdi::RequestHandle const& requestId, xdi::WriteBlobResponse const& resp, xdi::ApiErrorCode const& e));
    MOCK_METHOD3(upsertBlobMetadataCasResp, void(xdi::RequestHandle const& requestId, bool const& resp, xdi::ApiErrorCode const& e));
    MOCK_METHOD3(upsertBlobObjectCasResp, void(xdi::RequestHandle const& requestId, bool const& resp, xdi::ApiErrorCode const& e));
    MOCK_METHOD3(readObjectResp, void(xdi::RequestHandle const& requestId, std::shared_ptr<std::string> const& resp, xdi::ApiErrorCode const& e));
    MOCK_METHOD3(writeObjectResp, void(xdi::RequestHandle const& requestId, xdi::ObjectId const& resp, xdi::ApiErrorCode const& e));
    MOCK_METHOD3(deleteBlobResp, void(xdi::RequestHandle const& requestId, bool const& resp, xdi::ApiErrorCode const& e));
    MOCK_METHOD3(diffBlobResp, void(xdi::RequestHandle const& requestId, xdi::DiffBlobResponse const& resp, xdi::ApiErrorCode const& e));
    MOCK_METHOD3(diffAllBlobsResp, void(xdi::RequestHandle const& requestId, xdi::DiffAllBlobsResponse const& resp, xdi::ApiErrorCode const& e));
    MOCK_METHOD3(diffVolumesResp, void(xdi::RequestHandle const& requestId, xdi::DiffVolumesResponse const& resp, xdi::ApiErrorCode const& e));
};

class ApiStubFixture : public ::testing::Test {
protected:
    std::shared_ptr<MockResponseInterface> mockInterface;
    std::shared_ptr<xdi::FdsStub> stub;
    std::shared_ptr<xdi::ApiInterface> interface;

    std::vector<std::string> objects;

    void SetUp() {
        objects.push_back("blahblahblah");
        objects.push_back("anotherobjecttoinsert");
        objects.push_back("addinganotherobject");
        objects.push_back("thisisanotheroject");
        objects.push_back("thisobjectwillbeusedforCAS");
        mockInterface = std::make_shared<MockResponseInterface>();
        stub = std::make_shared<xdi::FdsStub>();
        for (auto const& o : objects) {
            xdi::WriteObjectRequest req;
            req.buffer = std::make_shared<std::string>(o);
            req.volId = 0;
            stub->writeObject(req);
        }
        interface = std::make_shared<xdi::ApiStub>(stub, 0);
    };
    void TearDown() {

    };
};


std::string path("TestBlob");
xdi::BlobPath p(1, path);

std::string path2("TestBlob2");
xdi::BlobPath p2(1, path2);

uint64_t handleId = 0;

// Write all the objects required for the tests
TEST_F(ApiStubFixture, WriteObjects) {
    xdi::RequestHandle handle {++handleId, 0};
    xdi::Request r {handle, xdi::RequestType::WRITE_OBJECT_TYPE, mockInterface.get()};
    xdi::WriteObjectRequest req1;
    req1.buffer = std::make_shared<std::string>("writeobjecttest");
    EXPECT_CALL(*mockInterface, writeObjectResp(handle, "6", xdi::ApiErrorCode::XDI_OK));
    interface->writeObject(r, req1);
}

// Test a simple read of an ObjectId
TEST_F(ApiStubFixture, ReadObject) {
    xdi::RequestHandle readHandle {++handleId, 0};
    xdi::Request r {readHandle, xdi::RequestType::READ_OBJECT_TYPE, mockInterface.get()};
    xdi::ReadObjectRequest readReq;
    readReq.id = "1";
    EXPECT_CALL(*mockInterface, readObjectResp(readHandle, Pointee(objects[0]), xdi::ApiErrorCode::XDI_OK));
    interface->readObject(r, readReq);
}

// Test that reading a missing object returns XDI_MISSING_OBJECT
TEST_F(ApiStubFixture, ReadObjectMissing) {
    xdi::RequestHandle handle {++handleId, 0};
    xdi::Request r {handle, xdi::RequestType::READ_OBJECT_TYPE, mockInterface.get()};
    xdi::ReadObjectRequest readReq;
    readReq.id = "42";
    EXPECT_CALL(*mockInterface, readObjectResp(handle, _, xdi::ApiErrorCode::XDI_MISSING_OBJECT));
    interface->readObject(r, readReq);
}

// Test writing a Blob
TEST_F(ApiStubFixture, WriteBlob) {
    xdi::RequestHandle writeBlobHandle {++handleId, 0};
    xdi::Request r {writeBlobHandle, xdi::RequestType::WRITE_BLOB_TYPE, mockInterface.get()};
    xdi::WriteBlobRequest req;
    req.blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2;
    od1.objectId = "1";
    od1.length = objects[0].size();
    od2.objectId = "2";
    od2.length = objects[1].size();
    req.blob.objects.emplace(0, od1);
    req.blob.objects.emplace(1, od2);
    xdi::WriteBlobResponse resp;
    EXPECT_CALL(*mockInterface, writeBlobResp(writeBlobHandle, resp, xdi::ApiErrorCode::XDI_OK));
    interface->writeBlob(r, req);

    xdi::RequestHandle updateBlobHandle {++handleId, 0};
    xdi::Request r2 {updateBlobHandle, xdi::RequestType::WRITE_BLOB_TYPE, mockInterface.get()};
    xdi::WriteBlobRequest req2;
    req2.blob.blobInfo.path = p;
    xdi::ObjectDescriptor od3, od4;
    od3.objectId = "3";
    od3.length = objects[2].size();
    od4.objectId = "4";
    od4.length = objects[3].size();
    // This overwrites an offset
    req2.blob.objects.emplace(1, od3);
    // This one adds another offset
    req2.blob.objects.emplace(2, od4);
    xdi::WriteBlobResponse resp2;
    EXPECT_CALL(*mockInterface, writeBlobResp(updateBlobHandle, resp2, xdi::ApiErrorCode::XDI_OK));
    interface->writeBlob(r2, req2);

    xdi::RequestHandle readBlobHandle {++handleId, 0};
    xdi::Request r3 {readBlobHandle, xdi::RequestType::READ_BLOB_TYPE, mockInterface.get()};
    xdi::ReadBlobRequest req3;
    req3.path = p;
    req3.range.startObjectOffset = 0;
    req3.range.endObjectOffset = 2;
    xdi::ReadBlobResponse resp3;
    resp3.blob.stat.blobInfo.path = p;
    resp3.blob.stat.size = 0;
    resp3.blob.objects.emplace(0, "1");
    resp3.blob.objects.emplace(1, "3");
    resp3.blob.objects.emplace(2, "4");
    EXPECT_CALL(*mockInterface, readBlobResp(readBlobHandle, resp3, xdi::ApiErrorCode::XDI_OK));
    interface->readBlob(r3, req3);

    xdi::RequestHandle readBlobRangeHandle {++handleId, 0};
    xdi::Request r4 {readBlobRangeHandle, xdi::RequestType::READ_BLOB_TYPE, mockInterface.get()};
    xdi::ReadBlobRequest req4;
    req4.path = p;
    req4.range.startObjectOffset = 1;
    req4.range.endObjectOffset = 2;
    xdi::ReadBlobResponse resp4;
    resp4.blob.stat.blobInfo.path = p;
    resp4.blob.stat.size = 0;
    resp4.blob.objects.emplace(1, "3");
    resp4.blob.objects.emplace(2, "4");
    EXPECT_CALL(*mockInterface, readBlobResp(readBlobRangeHandle, resp4, xdi::ApiErrorCode::XDI_OK));
    interface->readBlob(r4, req4);
}

// Test that reading a missing Blob returns XDI_MISSING_BLOB
TEST_F(ApiStubFixture, ReadBlobMissing) {
    xdi::RequestHandle handle {++handleId, 0};
    xdi::Request r {handle, xdi::RequestType::READ_BLOB_TYPE, mockInterface.get()};
    xdi::ReadBlobRequest req;
    req.path = p2;
    req.range.startObjectOffset = 0;
    req.range.endObjectOffset = 2;
    xdi::ReadBlobResponse resp;
    EXPECT_CALL(*mockInterface, readBlobResp(handle, _, xdi::ApiErrorCode::XDI_MISSING_BLOB));
    interface->readBlob(r, req);
}

// Test a CAS operation and verify it with a read of the Blob
// Precondition: offset 0 has ID "1"
// Postcondition: offset 0 has ID "5"
TEST_F(ApiStubFixture, UpsertBlobObjectCas) {
    xdi::WriteBlobRequest writeBlobReq;
    writeBlobReq.blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2;
    od1.objectId = "1";
    od1.length = objects[0].size();
    od2.objectId = "2";
    od2.length = objects[1].size();
    writeBlobReq.blob.objects.emplace(0, od1);
    writeBlobReq.blob.objects.emplace(1, od2);
    stub->writeBlob(writeBlobReq);

    xdi::RequestHandle handle {++handleId, 0};
    xdi::Request r {handle, xdi::RequestType::UPSERT_BLOB_OBJECT_CAS_TYPE, mockInterface.get()};
    xdi::UpsertBlobObjectCasRequest casReq;
    casReq.path = p;
    casReq.preconditionOffset = 0;
    casReq.preconditionRequiredObjectId = "1";
    casReq.objectId.objectId = "5";
    casReq.objectId.length = objects[4].size();
    EXPECT_CALL(*mockInterface, upsertBlobObjectCasResp(handle, true, xdi::ApiErrorCode::XDI_OK));
    interface->upsertBlobObjectCas(r, casReq);

    xdi::RequestHandle handle2 {++handleId, 0};
    xdi::Request r2 {handle2, xdi::RequestType::READ_BLOB_TYPE, mockInterface.get()};
    xdi::ReadBlobRequest readReq;
    readReq.path = p;
    readReq.range.startObjectOffset = 0;
    readReq.range.endObjectOffset = 0;
    xdi::ReadBlobResponse resp;
    resp.blob.stat.blobInfo.path = p;
    resp.blob.stat.size = 0;
    resp.blob.objects.emplace(0, "5");
    EXPECT_CALL(*mockInterface, readBlobResp(handle2, resp, xdi::ApiErrorCode::XDI_OK));
    interface->readBlob(r2, readReq);
}

// Test that a CAS operation fails if the precondition isn't correct
// Precondition: offset 0 has ID "1"
TEST_F(ApiStubFixture, UpsertBlobObjectCasNegative) {
    xdi::WriteBlobRequest writeBlobReq;
    writeBlobReq.blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2;
    od1.objectId = "5";
    od1.length = objects[4].size();
    od2.objectId = "2";
    od2.length = objects[1].size();
    writeBlobReq.blob.objects.emplace(0, od1);
    writeBlobReq.blob.objects.emplace(1, od2);
    stub->writeBlob(writeBlobReq);

    xdi::RequestHandle handle {++handleId, 0};
    xdi::Request r {handle, xdi::RequestType::UPSERT_BLOB_OBJECT_CAS_TYPE, mockInterface.get()};
    xdi::UpsertBlobObjectCasRequest casReq;
    casReq.path = p;
    casReq.preconditionOffset = 0;
    casReq.preconditionRequiredObjectId = "1";
    casReq.objectId.objectId = "3";
    casReq.objectId.length = objects[2].size();
    EXPECT_CALL(*mockInterface, upsertBlobObjectCasResp(handle, false, xdi::ApiErrorCode::XDI_OK));
    interface->upsertBlobObjectCas(r, casReq);

    xdi::RequestHandle handle2 {++handleId, 0};
    xdi::Request r2 {handle2, xdi::RequestType::READ_BLOB_TYPE, mockInterface.get()};
    xdi::ReadBlobRequest readReq;
    readReq.path = p;
    readReq.range.startObjectOffset = 0;
    readReq.range.endObjectOffset = 0;
    xdi::ReadBlobResponse resp;
    resp.blob.stat.blobInfo.path = p;
    resp.blob.stat.size = 0;
    resp.blob.objects.emplace(0, "5");
    EXPECT_CALL(*mockInterface, readBlobResp(handle2, resp, xdi::ApiErrorCode::XDI_OK));
    interface->readBlob(r2, readReq);
}

// Test deleting Blob
TEST_F(ApiStubFixture, DeleteBlob) {
    xdi::WriteBlobRequest writeBlobReq;
    writeBlobReq.blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2;
    od1.objectId = "1";
    od1.length = objects[0].size();
    od2.objectId = "2";
    od2.length = objects[1].size();
    writeBlobReq.blob.objects.emplace(0, od1);
    writeBlobReq.blob.objects.emplace(1, od2);
    stub->writeBlob(writeBlobReq);

    xdi::RequestHandle handle {++handleId, 0};
    xdi::Request r {handle, xdi::RequestType::DELETE_BLOB_TYPE, mockInterface.get()};
    EXPECT_CALL(*mockInterface, deleteBlobResp(handle, true, xdi::ApiErrorCode::XDI_OK));
    interface->deleteBlob(r, p);
}

// Test deleting Blob
TEST_F(ApiStubFixture, DeleteBlobMissing) {
    xdi::RequestHandle handle {++handleId, 0};
    xdi::Request r {handle, xdi::RequestType::DELETE_BLOB_TYPE, mockInterface.get()};
    EXPECT_CALL(*mockInterface, deleteBlobResp(handle, false, xdi::ApiErrorCode::XDI_OK));
    interface->deleteBlob(r, p);
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
