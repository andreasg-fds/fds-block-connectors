/*
 * gtestWriteContext.cpp
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

#include <gtest/gtest.h>

#include "log/test_log.h"
#include "connector/block/WriteContext.h"

std::string path("TestBlob");
xdi::BlobPath p(1, path);

std::string obj1 = "blahblahblah";
std::string obj2 = "anotherobjecttoinsert";

static const uint32_t OBJECTSIZE = 1024;

class TestWriteContextFixture : public ::testing::Test {
protected:
   std::shared_ptr<fds::block::WriteContext> ctx;
void SetUp() {
   ctx = std::make_shared<fds::block::WriteContext>(1, path, OBJECTSIZE);
};
void TearDown() {

};
};

// Test inserting blob write
TEST_F(TestWriteContextFixture, InsertBlob) {
    xdi::BlobUpdate blob;
    blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2, od3;
    od1.objectId = "1";
    od1.length = OBJECTSIZE;
    od2.objectId = "2";
    od2.length = OBJECTSIZE;
    od3.objectId = "3";
    od3.length = OBJECTSIZE;
    blob.objects.emplace(0, od1);
    blob.objects.emplace(1, od2);
    blob.objects.emplace(2, od3);
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::OK, ctx->addReadBlob(0, 2, nullptr, false));
    EXPECT_TRUE(ctx->addPendingWrite(0, 2, nullptr));

    ctx->updateOffset(0, "4");
    ctx->updateOffset(1, "5");
    ctx->triggerWrite(0);
    ctx->updateOffset(2, "6");
    ctx->updateOffset(0, "7");

    xdi::WriteBlobRequest req;
    fds::block::WriteContext::PendingTasks q;
    EXPECT_TRUE(ctx->getWriteBlobRequest(0, req, q));
    ASSERT_EQ(3, req.blob.objects.size());
    EXPECT_EQ(0, ctx->getNumPendingBlobs());
    EXPECT_EQ(1, q.size());
    EXPECT_EQ("7", req.blob.objects[0].objectId);
    EXPECT_EQ("5", req.blob.objects[1].objectId);
    EXPECT_EQ("6", req.blob.objects[2].objectId);
    fds::block::WriteContext::PendingTasks awaitingQ;
    ctx->completeBlobWrite(0, awaitingQ);
    EXPECT_EQ(0, awaitingQ.size());
}

// Test updating blob write
// Second write is for offsets following first write.
TEST_F(TestWriteContextFixture, AddOverlappingRangeAfter) {
    xdi::BlobUpdate blob;
    blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2, od3;
    od1.objectId = "1";
    od1.length = OBJECTSIZE;
    od2.objectId = "2";
    od2.length = OBJECTSIZE;
    od3.objectId = "3";
    od3.length = OBJECTSIZE;
    blob.objects.emplace(0, od1);
    blob.objects.emplace(1, od2);
    blob.objects.emplace(2, od3);
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::OK, ctx->addReadBlob(0, 2, nullptr, false));
    EXPECT_TRUE(ctx->addPendingWrite(0, 2, nullptr));

    xdi::BlobUpdate blob2;
    blob2.blobInfo.path = p;
    xdi::ObjectDescriptor od4, od5, od6;
    od4.objectId = "3";
    od4.length = OBJECTSIZE;
    od5.objectId = "4";
    od5.length = OBJECTSIZE;
    od6.objectId = "5";
    od6.length = OBJECTSIZE;
    blob2.objects.emplace(2, od4);
    blob2.objects.emplace(3, od5);
    blob2.objects.emplace(4, od6);
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::OK, ctx->addReadBlob(2, 4, nullptr, false));
    EXPECT_TRUE(ctx->addPendingWrite(2, 4, nullptr));

    ctx->updateOffset(0, "6");
    ctx->updateOffset(1, "7");
    ctx->updateOffset(2, "8");
    ctx->updateOffset(3, "9");
    ctx->updateOffset(4, "10");

    fds::block::WriteContext::PendingTasks q;
    xdi::WriteBlobRequest req;
    EXPECT_TRUE(ctx->getWriteBlobRequest(0, req, q));
    ASSERT_EQ(5, req.blob.objects.size());
    EXPECT_EQ(0, ctx->getNumPendingBlobs());
    EXPECT_EQ(2, q.size());
    EXPECT_EQ("6", req.blob.objects[0].objectId);
    EXPECT_EQ("7", req.blob.objects[1].objectId);
    EXPECT_EQ("8", req.blob.objects[2].objectId);
    EXPECT_EQ("9", req.blob.objects[3].objectId);
    EXPECT_EQ("10", req.blob.objects[4].objectId);
    fds::block::WriteContext::PendingTasks awaitingQ;
    ctx->completeBlobWrite(0, awaitingQ);
    EXPECT_EQ(0, awaitingQ.size());
}

// Test updating blob write
// Second write is for offsets less than original write.
TEST_F(TestWriteContextFixture, AddOverlappingRangeBefore) {
    xdi::BlobUpdate blob;
    blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2, od3;
    od1.objectId = "1";
    od1.length = OBJECTSIZE;
    od2.objectId = "2";
    od2.length = OBJECTSIZE;
    od3.objectId = "3";
    od3.length = OBJECTSIZE;
    blob.objects.emplace(3, od1);
    blob.objects.emplace(4, od2);
    blob.objects.emplace(5, od3);
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::OK, ctx->addReadBlob(3, 5, nullptr, false));
    EXPECT_TRUE(ctx->addPendingWrite(3, 5, nullptr));

    xdi::BlobUpdate blob2;
    blob2.blobInfo.path = p;
    xdi::ObjectDescriptor od4, od5, od6;
    od4.objectId = "3";
    od4.length = OBJECTSIZE;
    od5.objectId = "4";
    od5.length = OBJECTSIZE;
    od6.objectId = "1";
    od6.length = OBJECTSIZE;
    blob2.objects.emplace(1, od4);
    blob2.objects.emplace(2, od5);
    blob2.objects.emplace(3, od6);
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::OK, ctx->addReadBlob(1, 3, nullptr, false));
    EXPECT_TRUE(ctx->addPendingWrite(1, 3, nullptr));

    ctx->updateOffset(1, "6");
    ctx->updateOffset(2, "7");
    ctx->updateOffset(3, "8");
    ctx->updateOffset(4, "9");
    ctx->updateOffset(5, "10");

    fds::block::WriteContext::PendingTasks q;
    xdi::WriteBlobRequest req;
    EXPECT_TRUE(ctx->getWriteBlobRequest(1, req, q));
    ASSERT_EQ(5, req.blob.objects.size());
    EXPECT_EQ(0, ctx->getNumPendingBlobs());
    EXPECT_EQ(2, q.size());
    EXPECT_EQ("6", req.blob.objects[1].objectId);
    EXPECT_EQ("7", req.blob.objects[2].objectId);
    EXPECT_EQ("8", req.blob.objects[3].objectId);
    EXPECT_EQ("9", req.blob.objects[4].objectId);
    EXPECT_EQ("10", req.blob.objects[5].objectId);
    fds::block::WriteContext::PendingTasks awaitingQ;
    ctx->completeBlobWrite(1, awaitingQ);
    EXPECT_EQ(0, awaitingQ.size());
}

// Test BlockOperations type scenario where writes
// complete in order and then the Blob is complete
// when the last WriteObject comes back.
TEST_F(TestWriteContextFixture, TestBlockOperationsScenario) {
    xdi::BlobUpdate blob;
    blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2, od3;
    od1.objectId = "1";
    od1.length = OBJECTSIZE;
    od2.objectId = "2";
    od2.length = OBJECTSIZE;
    od3.objectId = "3";
    od3.length = OBJECTSIZE;
    blob.objects.emplace(0, od1);
    blob.objects.emplace(1, od2);
    blob.objects.emplace(2, od3);
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::OK, ctx->addReadBlob(0, 2, nullptr, false));
    EXPECT_TRUE(ctx->addPendingWrite(0, 2, nullptr));

    xdi::RequestHandle h1 {1, 0};
    xdi::RequestHandle h2 {2, 0};
    ctx->queue_update(0, h1);
    ctx->queue_update(1, h1);
    ctx->queue_update(2, h1);
    // another write came in for offset 0
    ctx->queue_update(0, h2);
    auto first_pop = ctx->pop(0);
    EXPECT_TRUE(std::get<0>(first_pop));
    auto second_pop = ctx->pop(0);
    EXPECT_FALSE(std::get<0>(second_pop));
    xdi::WriteBlobRequest req;
    fds::block::WriteContext::PendingTasks q;
    ctx->updateOffset(0, "4");
    EXPECT_FALSE(ctx->getWriteBlobRequest(0, req, q));
    ctx->updateOffset(1, "5");
    EXPECT_FALSE(ctx->getWriteBlobRequest(1, req, q));
    ctx->updateOffset(2, "6");
    EXPECT_TRUE(ctx->getWriteBlobRequest(2, req, q));
    ASSERT_EQ(3, req.blob.objects.size());
    EXPECT_EQ(0, ctx->getNumPendingBlobs());
    EXPECT_EQ(1, q.size());
    EXPECT_EQ("4", req.blob.objects[0].objectId);
    EXPECT_EQ("5", req.blob.objects[1].objectId);
    EXPECT_EQ("6", req.blob.objects[2].objectId);
    // Another write comes in before the BlobWrite is complete
    xdi::BlobUpdate blob2;
    blob2.blobInfo.path = p;
    xdi::ObjectDescriptor od4, od5, od6;
    od4.objectId = "4";
    od4.length = OBJECTSIZE;
    od5.objectId = "5";
    od5.length = OBJECTSIZE;
    od6.objectId = "6";
    od6.length = OBJECTSIZE;
    blob2.objects.emplace(2, od4);
    blob2.objects.emplace(3, od5);
    blob2.objects.emplace(4, od6);
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::PENDING, ctx->addReadBlob(2, 4, nullptr, false));
    //EXPECT_FALSE(ctx->addPendingWrite(blob2, nullptr));

    fds::block::WriteContext::PendingTasks awaitingQ;
    ctx->completeBlobWrite(0, awaitingQ);
    EXPECT_EQ(1, awaitingQ.size());
}

// Test to make sure getWriteBlobRequest fails if not all
// offsets have been updated with new ObjectIds.
// Offset 1 does not get an update.
// Then verify that after we do update it that the call succeeds.
TEST_F(TestWriteContextFixture, GetWriteBlobRequestNegative) {
    xdi::BlobUpdate blob;
    blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2, od3;
    od1.objectId = "1";
    od1.length = OBJECTSIZE;
    od2.objectId = "2";
    od2.length = OBJECTSIZE;
    od3.objectId = "3";
    od3.length = OBJECTSIZE;
    blob.objects.emplace(0, od1);
    blob.objects.emplace(1, od2);
    blob.objects.emplace(2, od3);
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::OK, ctx->addReadBlob(0, 2, nullptr, false));
    EXPECT_TRUE(ctx->addPendingWrite(0, 2, nullptr));

    ctx->updateOffset(0, "4");
    ctx->updateOffset(2, "6");

    xdi::WriteBlobRequest reqFailure;
    fds::block::WriteContext::PendingTasks q;
    EXPECT_FALSE(ctx->getWriteBlobRequest(0, reqFailure, q));

    ctx->updateOffset(1, "5");

    xdi::WriteBlobRequest reqSuccess;
    EXPECT_TRUE(ctx->getWriteBlobRequest(0, reqSuccess, q));
    ASSERT_EQ(3, reqSuccess.blob.objects.size());
    EXPECT_EQ(0, ctx->getNumPendingBlobs());
    EXPECT_EQ(1, q.size());
    EXPECT_EQ("4", reqSuccess.blob.objects[0].objectId);
    EXPECT_EQ("5", reqSuccess.blob.objects[1].objectId);
    EXPECT_EQ("6", reqSuccess.blob.objects[2].objectId);
    fds::block::WriteContext::PendingTasks awaitingQ;
    ctx->completeBlobWrite(0, awaitingQ);
    EXPECT_EQ(0, awaitingQ.size());
}

// Test failing a Blob
TEST_F(TestWriteContextFixture, FailBlob) {
    xdi::BlobUpdate blob;
    blob.blobInfo.path = p;
    xdi::ObjectDescriptor od1, od2, od3;
    od1.objectId = "1";
    od1.length = OBJECTSIZE;
    od2.objectId = "2";
    od2.length = OBJECTSIZE;
    od3.objectId = "3";
    od3.length = OBJECTSIZE;
    blob.objects.emplace(0, od1);
    blob.objects.emplace(1, od2);
    blob.objects.emplace(2, od3);
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::OK, ctx->addReadBlob(0, 2, nullptr, false));
    EXPECT_TRUE(ctx->addPendingWrite(0, 2, nullptr));

    ctx->updateOffset(0, "4");
    ctx->updateOffset(1, "5");

    xdi::WriteBlobRequest req;
    fds::block::WriteContext::PendingTasks q;
    EXPECT_TRUE(ctx->failWriteBlobRequest(0, q));
    EXPECT_EQ(0, ctx->getNumPendingBlobs());
}

// Test for UNAVAILABLE if a request wants exclusive access to range
TEST_F(TestWriteContextFixture, TestExclusiveAccess) {
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::OK, ctx->addReadBlob(2, 4, nullptr, false));
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::OK, ctx->addReadBlob(8, 10, nullptr, false));
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::OK, ctx->addReadBlob(15, 18, nullptr, false));
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::OK, ctx->addReadBlob(20, 25, nullptr, false));

    // These should overlap
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::UNAVAILABLE, ctx->addReadBlob(9, 9, nullptr, true));
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::UNAVAILABLE, ctx->addReadBlob(18, 20, nullptr, true));
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::UNAVAILABLE, ctx->addReadBlob(10, 13, nullptr, true));

    // This should be available
    EXPECT_EQ(fds::block::WriteContext::ReadBlobResult::OK, ctx->addReadBlob(30, 35, nullptr, true));
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    xdi::SetTestLogger(xdi::createLogger("gtestWriteContext"));
    return RUN_ALL_TESTS();
}
