#include <gtest/gtest.h>

#include "Log.h"
#include "connector/block/WriteContext.h"

std::string path("TestBlob");
xdi::BlobPath p(1, path);

std::string obj1 = "blahblahblah";
std::string obj2 = "anotherobjecttoinsert";

fds::block::WriteContext ctx(1, path, 1024);

// Test inserting blob write
TEST(WriteContextTest, InsertBlob) {
    xdi::BlobUpdate blob;
    blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2, od3;
    od1.objectId = "1";
    od1.length = 1024;
    od2.objectId = "2";
    od2.length = 1024;
    od3.objectId = "3";
    od3.length = 1024;
    blob.objects.emplace(0, od1);
    blob.objects.emplace(1, od2);
    blob.objects.emplace(2, od3);
    EXPECT_TRUE(ctx.addReadBlob(0, 2, nullptr));
    EXPECT_TRUE(ctx.addPendingWrite(0, 2, nullptr));

    ctx.updateOffset(0, "4");
    ctx.updateOffset(1, "5");
    ctx.triggerWrite(0);
    ctx.updateOffset(2, "6");
    ctx.updateOffset(0, "7");

    xdi::WriteBlobRequest req;
    fds::block::WriteContext::PendingTasks q;
    EXPECT_TRUE(ctx.getWriteBlobRequest(0, req, q));
    ASSERT_EQ(3, req.blob.objects.size());
    EXPECT_EQ(0, ctx.getNumPendingBlobs());
    EXPECT_EQ(1, q.size());
    EXPECT_EQ("7", req.blob.objects[0].objectId);
    EXPECT_EQ("5", req.blob.objects[1].objectId);
    EXPECT_EQ("6", req.blob.objects[2].objectId);
    fds::block::WriteContext::PendingTasks awaitingQ;
    ctx.completeBlobWrite(0, awaitingQ);
    EXPECT_EQ(0, awaitingQ.size());
}

// Test updating blob write
// Second write is for offsets following first write.
TEST(WriteContextTest, AddOverlappingRangeAfter) {
    xdi::BlobUpdate blob;
    blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2, od3;
    od1.objectId = "1";
    od1.length = 1024;
    od2.objectId = "2";
    od2.length = 1024;
    od3.objectId = "3";
    od3.length = 1024;
    blob.objects.emplace(0, od1);
    blob.objects.emplace(1, od2);
    blob.objects.emplace(2, od3);
    EXPECT_TRUE(ctx.addReadBlob(0, 2, nullptr));
    EXPECT_TRUE(ctx.addPendingWrite(0, 2, nullptr));

    xdi::BlobUpdate blob2;
    blob2.blobInfo.path = p;
    xdi::ObjectDescriptor od4, od5, od6;
    od4.objectId = "3";
    od4.length = 1024;
    od5.objectId = "4";
    od5.length = 1024;
    od6.objectId = "5";
    od6.length = 1024;
    blob2.objects.emplace(2, od4);
    blob2.objects.emplace(3, od5);
    blob2.objects.emplace(4, od6);
    EXPECT_TRUE(ctx.addReadBlob(2, 4, nullptr));
    EXPECT_TRUE(ctx.addPendingWrite(2, 4, nullptr));

    ctx.updateOffset(0, "6");
    ctx.updateOffset(1, "7");
    ctx.updateOffset(2, "8");
    ctx.updateOffset(3, "9");
    ctx.updateOffset(4, "10");

    fds::block::WriteContext::PendingTasks q;
    xdi::WriteBlobRequest req;
    EXPECT_TRUE(ctx.getWriteBlobRequest(0, req, q));
    ASSERT_EQ(5, req.blob.objects.size());
    EXPECT_EQ(0, ctx.getNumPendingBlobs());
    EXPECT_EQ(2, q.size());
    EXPECT_EQ("6", req.blob.objects[0].objectId);
    EXPECT_EQ("7", req.blob.objects[1].objectId);
    EXPECT_EQ("8", req.blob.objects[2].objectId);
    EXPECT_EQ("9", req.blob.objects[3].objectId);
    EXPECT_EQ("10", req.blob.objects[4].objectId);
    fds::block::WriteContext::PendingTasks awaitingQ;
    ctx.completeBlobWrite(0, awaitingQ);
    EXPECT_EQ(0, awaitingQ.size());
}

// Test updating blob write
// Second write is for offsets less than original write.
TEST(WriteContextTest, AddOverlappingRangeBefore) {
    xdi::BlobUpdate blob;
    blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2, od3;
    od1.objectId = "1";
    od1.length = 1024;
    od2.objectId = "2";
    od2.length = 1024;
    od3.objectId = "3";
    od3.length = 1024;
    blob.objects.emplace(3, od1);
    blob.objects.emplace(4, od2);
    blob.objects.emplace(5, od3);
    EXPECT_TRUE(ctx.addReadBlob(3, 5, nullptr));
    EXPECT_TRUE(ctx.addPendingWrite(3, 5, nullptr));

    xdi::BlobUpdate blob2;
    blob2.blobInfo.path = p;
    xdi::ObjectDescriptor od4, od5, od6;
    od4.objectId = "3";
    od4.length = 1024;
    od5.objectId = "4";
    od5.length = 1024;
    od6.objectId = "1";
    od6.length = 1024;
    blob2.objects.emplace(1, od4);
    blob2.objects.emplace(2, od5);
    blob2.objects.emplace(3, od6);
    EXPECT_TRUE(ctx.addReadBlob(1, 3, nullptr));
    EXPECT_TRUE(ctx.addPendingWrite(1, 3, nullptr));

    ctx.updateOffset(1, "6");
    ctx.updateOffset(2, "7");
    ctx.updateOffset(3, "8");
    ctx.updateOffset(4, "9");
    ctx.updateOffset(5, "10");

    fds::block::WriteContext::PendingTasks q;
    xdi::WriteBlobRequest req;
    EXPECT_TRUE(ctx.getWriteBlobRequest(1, req, q));
    ASSERT_EQ(5, req.blob.objects.size());
    EXPECT_EQ(0, ctx.getNumPendingBlobs());
    EXPECT_EQ(2, q.size());
    EXPECT_EQ("6", req.blob.objects[1].objectId);
    EXPECT_EQ("7", req.blob.objects[2].objectId);
    EXPECT_EQ("8", req.blob.objects[3].objectId);
    EXPECT_EQ("9", req.blob.objects[4].objectId);
    EXPECT_EQ("10", req.blob.objects[5].objectId);
    fds::block::WriteContext::PendingTasks awaitingQ;
    ctx.completeBlobWrite(1, awaitingQ);
    EXPECT_EQ(0, awaitingQ.size());
}

// Test BlockOperations type scenario where writes
// complete in order and then the Blob is complete
// when the last WriteObject comes back.
TEST(WriteContextTest, TestBlockOperationsScenario) {
    xdi::BlobUpdate blob;
    blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2, od3;
    od1.objectId = "1";
    od1.length = 1024;
    od2.objectId = "2";
    od2.length = 1024;
    od3.objectId = "3";
    od3.length = 1024;
    blob.objects.emplace(0, od1);
    blob.objects.emplace(1, od2);
    blob.objects.emplace(2, od3);
    EXPECT_TRUE(ctx.addReadBlob(0, 2, nullptr));
    EXPECT_TRUE(ctx.addPendingWrite(0, 2, nullptr));

    xdi::RequestHandle h1 {1, 0};
    xdi::RequestHandle h2 {2, 0};
    ctx.queue_update(0, h1);
    ctx.queue_update(1, h1);
    ctx.queue_update(2, h1);
    // another write came in for offset 0
    ctx.queue_update(0, h2);
    auto first_pop = ctx.pop(0);
    EXPECT_TRUE(std::get<0>(first_pop));
    auto second_pop = ctx.pop(0);
    EXPECT_FALSE(std::get<0>(second_pop));
    xdi::WriteBlobRequest req;
    fds::block::WriteContext::PendingTasks q;
    ctx.updateOffset(0, "4");
    EXPECT_FALSE(ctx.getWriteBlobRequest(0, req, q));
    ctx.updateOffset(1, "5");
    EXPECT_FALSE(ctx.getWriteBlobRequest(1, req, q));
    ctx.updateOffset(2, "6");
    EXPECT_TRUE(ctx.getWriteBlobRequest(2, req, q));
    ASSERT_EQ(3, req.blob.objects.size());
    EXPECT_EQ(0, ctx.getNumPendingBlobs());
    EXPECT_EQ(1, q.size());
    EXPECT_EQ("4", req.blob.objects[0].objectId);
    EXPECT_EQ("5", req.blob.objects[1].objectId);
    EXPECT_EQ("6", req.blob.objects[2].objectId);
    // Another write comes in before the BlobWrite is complete
    xdi::BlobUpdate blob2;
    blob2.blobInfo.path = p;
    xdi::ObjectDescriptor od4, od5, od6;
    od4.objectId = "4";
    od4.length = 1024;
    od5.objectId = "5";
    od5.length = 1024;
    od6.objectId = "6";
    od6.length = 1024;
    blob2.objects.emplace(2, od4);
    blob2.objects.emplace(3, od5);
    blob2.objects.emplace(4, od6);
    EXPECT_FALSE(ctx.addReadBlob(2, 4, nullptr));
    //EXPECT_FALSE(ctx.addPendingWrite(blob2, nullptr));

    fds::block::WriteContext::PendingTasks awaitingQ;
    ctx.completeBlobWrite(0, awaitingQ);
    EXPECT_EQ(1, awaitingQ.size());
}

// Test to make sure getWriteBlobRequest fails if not all
// offsets have been updated with new ObjectIds.
// Offset 1 does not get an update.
// Then verify that after we do update it that the call succeeds.
TEST(WriteContextTest, GetWriteBlobRequestNegative) {
    xdi::BlobUpdate blob;
    blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2, od3;
    od1.objectId = "1";
    od1.length = 1024;
    od2.objectId = "2";
    od2.length = 1024;
    od3.objectId = "3";
    od3.length = 1024;
    blob.objects.emplace(0, od1);
    blob.objects.emplace(1, od2);
    blob.objects.emplace(2, od3);
    EXPECT_TRUE(ctx.addReadBlob(0, 2, nullptr));
    EXPECT_TRUE(ctx.addPendingWrite(0, 2, nullptr));

    ctx.updateOffset(0, "4");
    ctx.updateOffset(2, "6");

    xdi::WriteBlobRequest reqFailure;
    fds::block::WriteContext::PendingTasks q;
    EXPECT_FALSE(ctx.getWriteBlobRequest(0, reqFailure, q));

    ctx.updateOffset(1, "5");

    xdi::WriteBlobRequest reqSuccess;
    EXPECT_TRUE(ctx.getWriteBlobRequest(0, reqSuccess, q));
    ASSERT_EQ(3, reqSuccess.blob.objects.size());
    EXPECT_EQ(0, ctx.getNumPendingBlobs());
    EXPECT_EQ(1, q.size());
    EXPECT_EQ("4", reqSuccess.blob.objects[0].objectId);
    EXPECT_EQ("5", reqSuccess.blob.objects[1].objectId);
    EXPECT_EQ("6", reqSuccess.blob.objects[2].objectId);
    fds::block::WriteContext::PendingTasks awaitingQ;
    ctx.completeBlobWrite(0, awaitingQ);
    EXPECT_EQ(0, awaitingQ.size());
}

// Test failing a Blob
TEST(WriteContextTest, FailBlob) {
    xdi::BlobUpdate blob;
    blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2, od3;
    od1.objectId = "1";
    od1.length = 1024;
    od2.objectId = "2";
    od2.length = 1024;
    od3.objectId = "3";
    od3.length = 1024;
    blob.objects.emplace(0, od1);
    blob.objects.emplace(1, od2);
    blob.objects.emplace(2, od3);
    EXPECT_TRUE(ctx.addReadBlob(0, 2, nullptr));
    EXPECT_TRUE(ctx.addPendingWrite(0, 2, nullptr));

    ctx.updateOffset(0, "4");
    ctx.updateOffset(1, "5");

    xdi::WriteBlobRequest req;
    fds::block::WriteContext::PendingTasks q;
    EXPECT_TRUE(ctx.failWriteBlobRequest(0, q));
    EXPECT_EQ(0, ctx.getNumPendingBlobs());
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
