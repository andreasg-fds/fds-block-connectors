#include <gtest/gtest.h>

#include "stub/FdsStub.h"

xdi::FdsStub s;

std::string path("BlockBlob");
xdi::BlobPath p(1, path);

std::string obj1 = "blahblahblah";
std::string obj2 = "anotherobjecttoinsert";

TEST(StubTest, AddVolume) {
    auto i_vol = std::make_shared<xdi::IscsiVolumeDescriptor>();
    i_vol->volumeId = 1;
    i_vol->volumeName = "IscsiVol";
    i_vol->maxObjectSize = 2048;
    i_vol->capacity = 1024 * 1024;
    s.addVolume(std::static_pointer_cast<xdi::VolumeDescriptorBase>(i_vol));
    auto o_vol = std::make_shared<xdi::ObjectVolumeDescriptor>();
    o_vol->volumeId = 2;
    o_vol->volumeName = "ObjectVol";
    o_vol->maxObjectSize = 1024;
    s.addVolume(std::static_pointer_cast<xdi::VolumeDescriptorBase>(o_vol));
    ASSERT_EQ(2, s.getNumVolumes());
    xdi::ListAllVolumesResponse vols;
    s.getAllVolumes(vols);
    xdi::VolumeDescriptorVisitor v;
    ASSERT_EQ(2, vols.volumes.size());
    EXPECT_EQ(xdi::VolumeType::ISCSI_VOLUME_TYPE, vols.volumes[0]->match(&v));
    EXPECT_EQ(xdi::VolumeType::OBJECT_VOLUME_TYPE, vols.volumes[1]->match(&v));
}

// Test inserting objects
TEST(StubTest, WriteObjects) {
    xdi::WriteObjectRequest req1;
    req1.buffer = std::make_shared<std::string>(obj1);
    auto ret = s.writeObject(req1);
    EXPECT_EQ("1", ret);
    xdi::WriteObjectRequest req2;
    req2.buffer = std::make_shared<std::string>(obj2);
    auto ret2 = s.writeObject(req2);
    EXPECT_EQ("2", ret2);
}

// Test reading back an existing object
TEST(StubTest, ReadObjects) {
    auto buf = std::make_shared<std::string>();
    xdi::ReadObjectRequest readReq;
    readReq.id = "2";
    EXPECT_EQ(xdi::ApiErrorCode::XDI_OK, s.readObject(readReq, buf));
    EXPECT_EQ("anotherobjecttoinsert", *buf);
}

// Test reading back a non-existing object
TEST(StubTest, ReadObjectNegative) {
    auto buf = std::make_shared<std::string>();
    xdi::ReadObjectRequest readReq;
    readReq.id = "42";
    EXPECT_EQ(xdi::ApiErrorCode::XDI_MISSING_OBJECT, s.readObject(readReq, buf));
}

// Test adding a new Blob that uses
// an Object that already exists.
TEST(StubTest, WriteBlob) {
    xdi::WriteBlobRequest writeReq;
    writeReq.blob.blobInfo.path = p;
    xdi::ObjectDescriptor od;
    od.objectId = "1";
    od.length = obj1.length();
    writeReq.blob.objects.emplace(0, od);
    EXPECT_EQ(xdi::ApiErrorCode::XDI_OK, s.writeBlob(writeReq));
}

// Test updating an existing Blob
// with another ObjectId at a new offset.
TEST(StubTest, AddToExistingBlob) {
    xdi::WriteBlobRequest writeReq;
    writeReq.blob.blobInfo.path = p;
    xdi::ObjectDescriptor od2;
    od2.objectId = "2";
    od2.length = obj2.length();
    writeReq.blob.objects.emplace(1, od2);
    EXPECT_EQ(xdi::ApiErrorCode::XDI_OK, s.writeBlob(writeReq));
}

// Make sure we can read a Blob back
// and that we get the expected number
// of Objects.
TEST(StubTest, ReadBlob) {
    xdi::ReadBlobRequest req;
    xdi::ReadBlobResponse resp;
    req.path = p;
    req.range.startObjectOffset = 0;
    req.range.endObjectOffset = 1;
    EXPECT_EQ(xdi::ApiErrorCode::XDI_OK, s.readBlob(req, resp));
    EXPECT_EQ(2, resp.blob.objects.size());
}

// Make sure the ObjectRange on reading
// a Blob works.
TEST(StubTest, ReadBlobObjectRange) {
    xdi::ReadBlobRequest req;
    xdi::ReadBlobResponse resp;
    req.path = p;
    req.range.startObjectOffset = 1;
    req.range.endObjectOffset = 1;
    EXPECT_EQ(xdi::ApiErrorCode::XDI_OK, s.readBlob(req, resp));
    EXPECT_EQ(1, resp.blob.objects.size());
}

// This tests truncating a blob in
// a WriteBlobRequest.
// The blob used to have 2 objects, but
// gets truncated to having just 1.
TEST(StubTest, TruncateBlob) {
    xdi::WriteBlobRequest writeReq;
    writeReq.blob.blobInfo.path = p;
    writeReq.blob.shouldTruncate = true;
    xdi::ObjectDescriptor od;
    od.objectId = "1";
    od.length = obj1.length();
    writeReq.blob.objects.emplace(0, od);
    EXPECT_EQ(xdi::ApiErrorCode::XDI_OK, s.writeBlob(writeReq));

    xdi::ReadBlobRequest req;
    xdi::ReadBlobResponse resp;
    req.path = p;
    req.range.startObjectOffset = 0;
    req.range.endObjectOffset = 10;
    EXPECT_EQ(xdi::ApiErrorCode::XDI_OK, s.readBlob(req, resp));
    // Make sure the truncation worked
    ASSERT_EQ(1, resp.blob.objects.size());
    EXPECT_EQ("1", resp.blob.objects[0]);
}

// KEEP THIS LAST
// Test deleting Blobs
TEST(StubTest, DeleteBlob) {
    bool happened;
    EXPECT_EQ(xdi::ApiErrorCode::XDI_OK, s.deleteBlob(p, happened));
    ASSERT_TRUE(happened);
    ASSERT_EQ(0, s.getNumBlobs());
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
#if 0
    po::options_description opts("Allowed options");
    opts.add_options()
        ("help", "produce help message")
        ("puts-cnt", po::value<int>()->default_value(1), "puts count");
    AmCacheTest::init(argc, argv, opts);
#endif
    return RUN_ALL_TESTS();
}