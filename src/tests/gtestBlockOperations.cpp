/*
 * gtestBlockOperations.cpp
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
#include <condition_variable>

#include "connector/block/BlockOperations.h"
#include "stub/FdsStub.h"
#include "stub/ApiStub.h"
#include "connector/block/Tasks.h"
#include "log/test_log.h"

static const uint32_t OBJECTSIZE = 131072;
static const uint32_t LBASIZE = 512;
static const uint32_t LBA_PER_OBJECT = OBJECTSIZE/LBASIZE;

class TestTask : public fds::block::ProtoTask {
public:
    TestTask(uint64_t const hdl) : fds::block::ProtoTask(hdl) {}
};

std::mutex mutex;
std::condition_variable cond_var;
int count {0};
int expected {0};

class TestConnector : public fds::block::BlockOperations {
public:
    TestConnector(std::shared_ptr<xdi::ApiInterface> interface, bool multi) : fds::block::BlockOperations(interface), isMultithreaded(multi) {}
    ~TestConnector() {}

    bool verifyBuffers(std::vector<std::shared_ptr<std::string>>& bufs) {
        for (auto& b : bufs) {
            if (true == verifyBuffer(b)) return true;
        }
        return false;
    }

    bool verifyBuffer(std::shared_ptr<std::string>& buf) {
        if ((buf->size() != readBuffer->size()) || (*buf != *readBuffer)) return false;
        return true;
    }

    void respondTask(fds::block::BlockTask* response) override {
        fds::block::TaskVisitor v;
        if (fds::block::TaskType::READ == response->match(&v)) {
            auto btask = static_cast<fds::block::ReadTask *>(response);
            readBuffer.reset(new std::string());
            uint32_t i = 0, context = 0;
            std::shared_ptr<std::string> buf = btask->getNextReadBuffer(context);
            while (buf != NULL) {
                *readBuffer += *buf;
                i += buf->length();
                buf = btask->getNextReadBuffer(context);
            }
        }
        if (true == isMultithreaded) {
            delete response->getProtoTask();
            std::lock_guard<std::mutex> lg(mutex);
            ++count;
            if (count == expected) {
                cond_var.notify_one();
            }
        }
    }
private:
    std::shared_ptr<std::string> readBuffer;
    bool                         isMultithreaded;
};

std::shared_ptr<std::string> randomStrGen(int length) {
    auto result = std::make_shared<std::string>();
    result->resize(length);

    for (int i = 0; i < length; i++)
        (*result)[i] = std::rand();
    
    return result;
}

class TestConnectorFixture : public ::testing::Test {
protected:
    std::shared_ptr<xdi::FdsStub> stubPtr;
    std::shared_ptr<xdi::ApiInterface> interfacePtr;
    std::shared_ptr<TestConnector> connectorPtr;
    void SetUp() {
        stubPtr = std::make_shared<xdi::FdsStub>();
        interfacePtr = std::make_shared<xdi::ApiStub>(stubPtr, 0);
        connectorPtr = std::make_shared<TestConnector>(interfacePtr, false);
        connectorPtr->init("testVol", 0, OBJECTSIZE);
    };
    void TearDown() {

    };
};

class AsyncTestConnectorFixture : public ::testing::Test {
protected:
    std::shared_ptr<xdi::FdsStub> stubPtr;
    std::shared_ptr<xdi::ApiInterface> interfacePtr;
    std::shared_ptr<TestConnector> connectorPtr;
    void SetUp() {
        stubPtr = std::make_shared<xdi::FdsStub>();
        interfacePtr = std::make_shared<xdi::AsyncApiStub>(stubPtr, 0);
        connectorPtr = std::make_shared<TestConnector>(interfacePtr, true);
        connectorPtr->init("testVol", 0, OBJECTSIZE);
    };
    void TearDown() {

    };
};

/******************************
** Basic Read/Write tests
******************************/
// Test reading nonexisting blob/object
// Read full object at offset 0
TEST_F(TestConnectorFixture, ReadNonExisting) {
    uint64_t seqId = 0;
    TestTask testTask(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask);
    auto fullBuf = std::make_shared<std::string>(OBJECTSIZE, '\0');
    readTask->set(0, OBJECTSIZE);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(fullBuf));
}

// Test reading nonexisting blob/object
// Read offset 1024 for length 65536
TEST_F(TestConnectorFixture, ReadNonExisting2) {
    uint64_t seqId = 0;
    uint64_t offset = 1024;
    uint32_t length = 65536;
    TestTask testTask(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask);
    auto fullBuf = std::make_shared<std::string>(length, '\0');
    readTask->set(offset, length);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(fullBuf));
}

// Test reading nonexisting object but existing blob
// Write offset 512 for length 1024
// Read offset 131072+1024 for length 65536
TEST_F(TestConnectorFixture, ReadNonExisting3) {
    uint64_t readOffset = 1024 + OBJECTSIZE;
    uint32_t readLength = 65536;
    uint64_t writeOffset = 512;
    uint32_t writeLength = 1024;
    uint64_t seqId = 0;
    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto write_buffer = randomStrGen(writeLength);
    writeTask->setWriteBuffer(write_buffer);
    writeTask->set(writeOffset, write_buffer->size());
    connectorPtr->executeTask(writeTask);

    TestTask testTask2(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask2);
    auto fullBuf = std::make_shared<std::string>(readLength, '\0');
    readTask->set(readOffset, readLength);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(fullBuf));
}

TEST_F(TestConnectorFixture, WriteTest) {
    TestTask testTask(0);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto write_buffer = randomStrGen(64);
    writeTask->setWriteBuffer(write_buffer);
    writeTask->set(0, write_buffer->size());
    connectorPtr->executeTask(writeTask);

    TestTask testTask2(1);
    auto readTask = new fds::block::ReadTask(&testTask2);
    readTask->set(0, write_buffer->size());
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(write_buffer));
}

TEST_F(TestConnectorFixture, WriteTestMultipleAligned) {
    TestTask testTask(0);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto write_buffer = randomStrGen(OBJECTSIZE*2);
    writeTask->setWriteBuffer(write_buffer);
    writeTask->set(0, write_buffer->size());
    connectorPtr->executeTask(writeTask);

    TestTask testTask2(1);
    auto readTask = new fds::block::ReadTask(&testTask2);
    readTask->set(0, write_buffer->size());
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(write_buffer));
}

TEST_F(TestConnectorFixture, WriteTestMultipleUnaligned) {
    uint64_t numWrites = 5;
    auto fullBuf = std::make_shared<std::string>();
    for (uint64_t i = 0; i < numWrites; ++i) {
        TestTask testTask(i);
        auto writeTask = new fds::block::WriteTask(&testTask);
        auto write_buffer = randomStrGen(1000);
        *fullBuf += *write_buffer;
        writeTask->setWriteBuffer(write_buffer);
        writeTask->set(i * 1000, write_buffer->size());
        connectorPtr->executeTask(writeTask);
    }

    TestTask testTask2(numWrites);
    auto readTask = new fds::block::ReadTask(&testTask2);
    readTask->set(0, numWrites * 1000);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(fullBuf));
}

TEST_F(TestConnectorFixture, WriteTestMultipleUnalignedWithOffset) {
    uint64_t numWrites = 5;
    uint64_t offset = 100;
    auto fullBuf = std::make_shared<std::string>();
    for (uint64_t i = 0; i < numWrites; ++i) {
        TestTask testTask(i);
        auto writeTask = new fds::block::WriteTask(&testTask);
        auto write_buffer = randomStrGen(1000);
        *fullBuf += *write_buffer;
        writeTask->setWriteBuffer(write_buffer);
        writeTask->set(i * 1000 + offset, write_buffer->size());
        connectorPtr->executeTask(writeTask);
    }

    TestTask testTask2(numWrites);
    auto readTask = new fds::block::ReadTask(&testTask2);
    readTask->set(offset, numWrites * 1000);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(fullBuf));
}

// Write 100000 bytes at offset 512
// Read back offset 0 for full object size
TEST_F(TestConnectorFixture, WriteTest2) {
    uint64_t offset = 512;
    uint32_t length = 100000;
    uint64_t seqId = 0;
    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto write_buffer = randomStrGen(length);
    writeTask->setWriteBuffer(write_buffer);
    writeTask->set(offset, write_buffer->size());
    connectorPtr->executeTask(writeTask);

    TestTask testTask2(seqId++);
    // We expected a buffer of zeros with our written data in the middle
    auto fullBuf = std::make_shared<std::string>(OBJECTSIZE, '\0');
    fullBuf->replace(offset, length, *write_buffer);
    auto readTask = new fds::block::ReadTask(&testTask2);
    readTask->set(0, OBJECTSIZE);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(fullBuf));
}

// Write full object of random data
// Write 100000 bytes at offset 512
// Read back offset 0 for full object size
TEST_F(TestConnectorFixture, WriteTest3) {
    uint64_t offset = 512;
    uint32_t length = 100000;
    uint64_t seqId = 0;

    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto write_buffer = randomStrGen(OBJECTSIZE);
    writeTask->setWriteBuffer(write_buffer);
    writeTask->set(0, OBJECTSIZE);
    connectorPtr->executeTask(writeTask);

    TestTask testTask2(seqId++);
    auto writeTask2 = new fds::block::WriteTask(&testTask2);
    auto write_buffer2 = randomStrGen(length);
    writeTask2->setWriteBuffer(write_buffer2);
    writeTask2->set(offset, write_buffer2->size());
    connectorPtr->executeTask(writeTask2);

    // overwrite the newly written section with second generated buffer
    write_buffer->replace(offset, length, *write_buffer2);

    TestTask testTask3(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask3);
    readTask->set(0, OBJECTSIZE);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(write_buffer));
}

// Write at offset 1024 for 327680 bytes (slightly over 2 objects)
// Read offset 0 for 3 full objects
TEST_F(TestConnectorFixture, WriteTestSpanning) {
    uint64_t offset = 1024;
    uint32_t length = 327680;
    uint32_t readLength = 3 * OBJECTSIZE;
    uint64_t seqId = 0;
    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto write_buffer = randomStrGen(length);
    writeTask->setWriteBuffer(write_buffer);
    writeTask->set(offset, write_buffer->size());
    connectorPtr->executeTask(writeTask);

    TestTask testTask2(seqId++);
    // We expected a buffer of zeros with our written data in the middle
    auto fullBuf = std::make_shared<std::string>(readLength, '\0');
    fullBuf->replace(offset, length, *write_buffer);
    auto readTask = new fds::block::ReadTask(&testTask2);
    readTask->set(0, readLength);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(fullBuf));
}

// Do several writes spanning the first 3 objects in random combinations
// Read back 4 objects
// Write1   |-s-------------|-e-------------|---------------|
// Write2   |---------------|-------------s-|---e-----------|
// Write2   |---------------|----se---------|---------------|
// Write2   |-------------s-|-------e-------|---------------|
TEST_F(TestConnectorFixture, WriteTestRepeatingRMW) {
    uint64_t seqId = 0;
    uint32_t readLength = 4 * OBJECTSIZE;
    std::vector<uint64_t> offset;
    std::vector<uint32_t> length;
    // Write1
    offset.push_back(2048);
    length.push_back(OBJECTSIZE);
    // Write2
    offset.push_back(261120);
    length.push_back(65536);
    // Write3
    offset.push_back(135168);
    length.push_back(512);
    // Write4
    offset.push_back(126976);
    length.push_back(65536);

    ASSERT_EQ(offset.size(), length.size());

    auto fullBuf = std::make_shared<std::string>(readLength, '\0');

    for (unsigned int i = 0; i < offset.size(); ++i) {
        TestTask testTask(seqId++);
        auto writeTask = new fds::block::WriteTask(&testTask);
        auto write_buffer = randomStrGen(length[i]);
        fullBuf->replace(offset[i], length[i], *write_buffer);
        writeTask->setWriteBuffer(write_buffer);
        writeTask->set(offset[i], write_buffer->size());
        connectorPtr->executeTask(writeTask);
    }

    TestTask testTask2(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask2);
    readTask->set(0, readLength);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(fullBuf));
}

/******************************
** WriteSame Tests
******************************/

// Write 2 objects worth of random data
// Then overwrite them using 512 byte writeSame command
TEST_F(TestConnectorFixture, WriteSameTestAligned) {
    uint64_t seqId = 0;
    uint64_t offset = 0;
    uint32_t length = 2 * OBJECTSIZE;
    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto writeBuffer = randomStrGen(length);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask->set(offset, writeBuffer->size());
    connectorPtr->executeTask(writeTask);

    TestTask testTask2(seqId++);
    auto writeSameTask = new fds::block::WriteSameTask(&testTask2);
    auto writeSameBuffer = std::make_shared<std::string>(LBASIZE, 'x');
    auto expectBuffer = std::make_shared<std::string>();
    for (unsigned int i = 0; i < 2 * LBA_PER_OBJECT; ++i) {
        *expectBuffer += *writeSameBuffer;
    }
    writeSameTask->set(0, length);
    writeSameTask->setWriteBuffer(writeSameBuffer);
    connectorPtr->executeTask(writeSameTask);

    TestTask testTask3(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask3);
    readTask->set(offset, length);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(expectBuffer));
}

// Write 2 objects worth of random data
// Then use writeSame to overwrite end aligned
TEST_F(TestConnectorFixture, WriteSameTestEndAligned) {
    uint64_t seqId = 0;
    uint64_t offset = 0;
    uint32_t length = 2 * OBJECTSIZE;
    uint32_t num_lbas = 5;
    uint64_t writeSameOffset = (LBA_PER_OBJECT - num_lbas) * LBASIZE;
    uint32_t writeSameLength = num_lbas * LBASIZE;
    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto writeBuffer = randomStrGen(length);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask->set(offset, writeBuffer->size());
    connectorPtr->executeTask(writeTask);

    TestTask testTask2(seqId++);
    auto writeSameTask = new fds::block::WriteSameTask(&testTask2);
    auto writeSameBuffer = std::make_shared<std::string>(LBASIZE, 'x');
    for (unsigned int i = 0; i < num_lbas; ++i) {
        writeBuffer->replace(writeSameOffset + (i * LBASIZE), LBASIZE, *writeSameBuffer);
    }
    writeSameTask->set(writeSameOffset, writeSameLength);
    writeSameTask->setWriteBuffer(writeSameBuffer);
    connectorPtr->executeTask(writeSameTask);

    TestTask testTask3(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask3);
    readTask->set(offset, length);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(writeBuffer));
}

// Write 2 objects worth of random data
// Then use writeSame to overwrite start aligned
TEST_F(TestConnectorFixture, WriteSameTestStartAligned) {
    uint64_t seqId = 0;
    uint64_t offset = 0;
    uint32_t length = 2 * OBJECTSIZE;
    uint32_t num_lbas = 5;
    uint64_t writeSameOffset = OBJECTSIZE;
    uint32_t writeSameLength = num_lbas * LBASIZE;
    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto writeBuffer = randomStrGen(length);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask->set(offset, writeBuffer->size());
    connectorPtr->executeTask(writeTask);

    TestTask testTask2(seqId++);
    auto writeSameTask = new fds::block::WriteSameTask(&testTask2);
    auto writeSameBuffer = std::make_shared<std::string>(LBASIZE, 'x');
    for (unsigned int i = 0; i < num_lbas; ++i) {
        writeBuffer->replace(writeSameOffset + (i * LBASIZE), LBASIZE, *writeSameBuffer);
    }
    writeSameTask->set(writeSameOffset, writeSameLength);
    writeSameTask->setWriteBuffer(writeSameBuffer);
    connectorPtr->executeTask(writeSameTask);

    TestTask testTask3(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask3);
    readTask->set(offset, length);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(writeBuffer));
}

// Write 3 objects worth of random data
// Then use writeSame to overwrite unaligned
TEST_F(TestConnectorFixture, WriteSameTestUnaligned) {
    uint64_t seqId = 0;
    uint64_t offset = 0;
    uint32_t length = 3 * OBJECTSIZE;
    uint32_t num_lbas = 337;
    uint64_t writeSameOffset = 200 * LBASIZE;
    uint32_t writeSameLength = num_lbas * LBASIZE;
    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto writeBuffer = randomStrGen(length);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask->set(offset, writeBuffer->size());
    connectorPtr->executeTask(writeTask);

    TestTask testTask2(seqId++);
    auto writeSameTask = new fds::block::WriteSameTask(&testTask2);
    auto writeSameBuffer = std::make_shared<std::string>(LBASIZE, 'x');
    for (unsigned int i = 0; i < num_lbas; ++i) {
        writeBuffer->replace(writeSameOffset + (i * LBASIZE), LBASIZE, *writeSameBuffer);
    }
    writeSameTask->set(writeSameOffset, writeSameLength);
    writeSameTask->setWriteBuffer(writeSameBuffer);
    connectorPtr->executeTask(writeSameTask);

    TestTask testTask3(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask3);
    readTask->set(offset, length);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(writeBuffer));
}

// Use writeSame to write a few LBAs in the middle of an object
TEST_F(TestConnectorFixture, WriteSameTestMiddle) {
    uint64_t seqId = 0;
    uint64_t readOffset = 0;
    uint32_t readLength = OBJECTSIZE;
    uint32_t num_lbas = 5;
    uint32_t offset_lbas = 10;
    uint64_t writeSameOffset = offset_lbas * LBASIZE;
    uint32_t writeSameLength = num_lbas * LBASIZE;

    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto writeBuffer = randomStrGen(readLength);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask->set(readOffset, writeBuffer->size());
    connectorPtr->executeTask(writeTask);

    TestTask testTask2(seqId++);
    auto writeSameTask = new fds::block::WriteSameTask(&testTask2);
    auto writeSameBuffer = std::make_shared<std::string>(LBASIZE, 'x');
    for (unsigned int i = 0; i < num_lbas; ++i) {
        writeBuffer->replace(writeSameOffset + (i * LBASIZE), LBASIZE, *writeSameBuffer);
    }
    writeSameTask->set(writeSameOffset, writeSameLength);
    writeSameTask->setWriteBuffer(writeSameBuffer);
    connectorPtr->executeTask(writeSameTask);

    TestTask testTask3(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask3);
    readTask->set(readOffset, readLength);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(writeBuffer));
}

// Use writeSame to write a few LBAs in the middle of a non existing object
TEST_F(TestConnectorFixture, WriteSameTestNonExistingObject) {
    uint64_t seqId = 0;
    uint64_t readOffset = 0;
    uint32_t readLength = OBJECTSIZE;
    uint32_t num_lbas = 5;
    uint32_t offset_lbas = 10;
    uint64_t writeSameOffset = offset_lbas * LBASIZE;
    uint32_t writeSameLength = num_lbas * LBASIZE;

    auto fullBuffer = std::make_shared<std::string>(readLength, '\0');

    TestTask testTask(seqId++);
    auto writeSameTask = new fds::block::WriteSameTask(&testTask);
    auto writeSameBuffer = randomStrGen(LBASIZE);
    for (unsigned int i = 0; i < num_lbas; ++i) {
        fullBuffer->replace(writeSameOffset + (i * LBASIZE), LBASIZE, *writeSameBuffer);
    }
    writeSameTask->set(writeSameOffset, writeSameLength);
    writeSameTask->setWriteBuffer(writeSameBuffer);
    connectorPtr->executeTask(writeSameTask);

    TestTask testTask2(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask2);
    readTask->set(readOffset, readLength);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(fullBuffer));
}

/******************************
** Unmap Tests
******************************/

// Write 2 objects worth of random data
// Then overwrite them using single unmap command
TEST_F(TestConnectorFixture, UnmapTestAligned) {
    uint64_t seqId = 0;
    uint64_t offset = 0;
    uint32_t length = 2 * OBJECTSIZE;
    uint32_t num_lbas = 2 * LBA_PER_OBJECT;
    uint32_t offset_lba = 0;

    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto writeBuffer = randomStrGen(length);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask->set(offset, writeBuffer->size());
    connectorPtr->executeTask(writeTask);

    fds::block::UnmapTask::unmap_vec_ptr write_vec(new fds::block::UnmapTask::unmap_vec);
    fds::block::UnmapTask::UnmapRange range;
    range.offset = offset_lba * LBASIZE;
    range.length = num_lbas * LBASIZE;
    write_vec->push_back(range);
    TestTask testTask2(seqId++);
    auto unmapTask = new fds::block::UnmapTask(&testTask2, std::move(write_vec));
    connectorPtr->executeTask(unmapTask);

    auto expectBuffer = std::make_shared<std::string>(length, '\0');

    TestTask testTask3(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask3);
    readTask->set(offset, length);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(expectBuffer));
}

// Write 2 objects worth of random data
// Then overwrite them using two unmap commands
TEST_F(TestConnectorFixture, UnmapTestAligned2) {
    uint64_t seqId = 0;
    uint64_t offset = 0;
    uint32_t length = 2 * OBJECTSIZE;
    uint32_t num_lbas = LBA_PER_OBJECT;
    uint32_t offset_lba = 0;

    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto writeBuffer = randomStrGen(length);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask->set(offset, writeBuffer->size());
    connectorPtr->executeTask(writeTask);

    fds::block::UnmapTask::unmap_vec_ptr write_vec(new fds::block::UnmapTask::unmap_vec);
    fds::block::UnmapTask::UnmapRange range;
    range.offset = offset_lba * LBASIZE;
    range.length = num_lbas * LBASIZE;
    write_vec->push_back(range);
    fds::block::UnmapTask::UnmapRange range2;
    range2.offset = (LBA_PER_OBJECT + offset_lba) * LBASIZE;
    range2.length = num_lbas * LBASIZE;
    write_vec->push_back(range2);
    TestTask testTask2(seqId++);
    auto unmapTask = new fds::block::UnmapTask(&testTask2, std::move(write_vec));
    connectorPtr->executeTask(unmapTask);

    auto expectBuffer = std::make_shared<std::string>(length, '\0');

    TestTask testTask3(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask3);
    readTask->set(offset, length);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(expectBuffer));
}

// Write 2 objects worth of random data
// Then unmap a spanning range
// |-------------uuu|
// |uu--------------|
TEST_F(TestConnectorFixture, UnmapTestSpanning) {
    uint64_t seqId = 0;
    uint64_t offset = 0;
    uint32_t length = 2 * OBJECTSIZE;
    uint32_t num_lbas = 50;
    uint32_t offset_lba = LBA_PER_OBJECT - 30;
    uint64_t unmapOffset = offset_lba * LBASIZE;

    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto writeBuffer = randomStrGen(length);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask->set(offset, writeBuffer->size());
    connectorPtr->executeTask(writeTask);

    fds::block::UnmapTask::unmap_vec_ptr write_vec(new fds::block::UnmapTask::unmap_vec);
    fds::block::UnmapTask::UnmapRange range;
    range.offset = offset_lba * LBASIZE;
    range.length = num_lbas * LBASIZE;
    write_vec->push_back(range);
    TestTask testTask2(seqId++);
    auto unmapTask = new fds::block::UnmapTask(&testTask2, std::move(write_vec));
    connectorPtr->executeTask(unmapTask);

    auto unmapBuffer = std::make_shared<std::string>(LBASIZE, '\0');
    for (unsigned int i = 0; i < num_lbas; ++i) {
        writeBuffer->replace(unmapOffset + (i * LBASIZE), LBASIZE, *unmapBuffer);
    }

    TestTask testTask3(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask3);
    readTask->set(offset, length);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(writeBuffer));
}

// Write 4 objects worth of random data
// Then unmap two spanning ranges
// |-------------uuu|
// |uu--------------|
// |------------uuuu|
// |uuuuu-----------|
TEST_F(TestConnectorFixture, UnmapTestDualSpanning) {
    uint64_t seqId = 0;
    uint64_t offset = 0;
    uint32_t length = 4 * OBJECTSIZE;
    uint32_t num_lbas = 50;
    uint32_t offset_lba = LBA_PER_OBJECT - 30;
    uint64_t unmapOffset = offset_lba * LBASIZE;
    uint32_t num_lbas2 = 90;
    uint32_t offset_lba2 = (2 * LBA_PER_OBJECT) + (LBA_PER_OBJECT - 40);
    uint64_t unmapOffset2 = offset_lba2 * LBASIZE;

    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto writeBuffer = randomStrGen(length);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask->set(offset, writeBuffer->size());
    connectorPtr->executeTask(writeTask);

    fds::block::UnmapTask::unmap_vec_ptr write_vec(new fds::block::UnmapTask::unmap_vec);
    fds::block::UnmapTask::UnmapRange range;
    range.offset = offset_lba * LBASIZE;
    range.length = num_lbas * LBASIZE;
    write_vec->push_back(range);
    fds::block::UnmapTask::UnmapRange range2;
    range2.offset = (offset_lba2) * LBASIZE;
    range2.length = num_lbas2 * LBASIZE;
    write_vec->push_back(range2);
    TestTask testTask2(seqId++);
    auto unmapTask = new fds::block::UnmapTask(&testTask2, std::move(write_vec));
    connectorPtr->executeTask(unmapTask);

    auto unmapBuffer = std::make_shared<std::string>(LBASIZE, '\0');
    for (unsigned int i = 0; i < num_lbas; ++i) {
        writeBuffer->replace(unmapOffset + (i * LBASIZE), LBASIZE, *unmapBuffer);
    }
    for (unsigned int i = 0; i < num_lbas2; ++i) {
        writeBuffer->replace(unmapOffset2 + (i * LBASIZE), LBASIZE, *unmapBuffer);
    }

    TestTask testTask3(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask3);
    readTask->set(offset, length);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(writeBuffer));
}

// Write 1 object worth of random data
// Then unmap start aligned
// |uu-----------------------|
TEST_F(TestConnectorFixture, UnmapTestStartAligned) {
    uint64_t seqId = 0;
    uint64_t offset = 0;
    uint32_t length = OBJECTSIZE;
    uint32_t num_lbas = 20;
    uint32_t offset_lba = 0;
    uint64_t unmapOffset = offset_lba * LBASIZE;

    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto writeBuffer = randomStrGen(length);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask->set(offset, writeBuffer->size());
    connectorPtr->executeTask(writeTask);

    fds::block::UnmapTask::unmap_vec_ptr write_vec(new fds::block::UnmapTask::unmap_vec);
    fds::block::UnmapTask::UnmapRange range;
    range.offset = offset_lba * LBASIZE;
    range.length = num_lbas * LBASIZE;
    write_vec->push_back(range);
    TestTask testTask2(seqId++);
    auto unmapTask = new fds::block::UnmapTask(&testTask2, std::move(write_vec));
    connectorPtr->executeTask(unmapTask);

    auto unmapBuffer = std::make_shared<std::string>(LBASIZE, '\0');
    for (unsigned int i = 0; i < num_lbas; ++i) {
        writeBuffer->replace(unmapOffset + (i * LBASIZE), LBASIZE, *unmapBuffer);
    }

    TestTask testTask3(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask3);
    readTask->set(offset, length);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(writeBuffer));
}

// Write 1 object worth of random data
// Then unmap end aligned
// |-------------------------uu|
TEST_F(TestConnectorFixture, UnmapTestEndAligned) {
    uint64_t seqId = 0;
    uint64_t offset = 0;
    uint32_t length = OBJECTSIZE;
    uint32_t num_lbas = 20;
    uint32_t offset_lba = LBA_PER_OBJECT - num_lbas;
    uint64_t unmapOffset = offset_lba * LBASIZE;

    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto writeBuffer = randomStrGen(length);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask->set(offset, writeBuffer->size());
    connectorPtr->executeTask(writeTask);

    fds::block::UnmapTask::unmap_vec_ptr write_vec(new fds::block::UnmapTask::unmap_vec);
    fds::block::UnmapTask::UnmapRange range;
    range.offset = offset_lba * LBASIZE;
    range.length = num_lbas * LBASIZE;
    write_vec->push_back(range);
    TestTask testTask2(seqId++);
    auto unmapTask = new fds::block::UnmapTask(&testTask2, std::move(write_vec));
    connectorPtr->executeTask(unmapTask);

    auto unmapBuffer = std::make_shared<std::string>(LBASIZE, '\0');
    for (unsigned int i = 0; i < num_lbas; ++i) {
        writeBuffer->replace(unmapOffset + (i * LBASIZE), LBASIZE, *unmapBuffer);
    }

    TestTask testTask3(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask3);
    readTask->set(offset, length);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(writeBuffer));
}

// Write 1 object worth of random data
// Then unmap in the middle
// |----------uu----------|
TEST_F(TestConnectorFixture, UnmapTestMiddle) {
    uint64_t seqId = 0;
    uint64_t offset = 0;
    uint32_t length = OBJECTSIZE;
    uint32_t num_lbas = 20;
    uint32_t offset_lba = 50;
    uint64_t unmapOffset = offset_lba * LBASIZE;

    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto writeBuffer = randomStrGen(length);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask->set(offset, writeBuffer->size());
    connectorPtr->executeTask(writeTask);

    fds::block::UnmapTask::unmap_vec_ptr write_vec(new fds::block::UnmapTask::unmap_vec);
    fds::block::UnmapTask::UnmapRange range;
    range.offset = offset_lba * LBASIZE;
    range.length = num_lbas * LBASIZE;
    write_vec->push_back(range);
    TestTask testTask2(seqId++);
    auto unmapTask = new fds::block::UnmapTask(&testTask2, std::move(write_vec));
    connectorPtr->executeTask(unmapTask);

    auto unmapBuffer = std::make_shared<std::string>(LBASIZE, '\0');
    for (unsigned int i = 0; i < num_lbas; ++i) {
        writeBuffer->replace(unmapOffset + (i * LBASIZE), LBASIZE, *unmapBuffer);
    }

    TestTask testTask3(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask3);
    readTask->set(offset, length);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(writeBuffer));
}

// Write 1 object worth of random data
// Then unmap both start and end aligned
// |uuu-----------uuuu|
TEST_F(TestConnectorFixture, UnmapTestBothEnds) {
    uint64_t seqId = 0;
    uint64_t offset = 0;
    uint32_t length = OBJECTSIZE;
    uint32_t num_lbas = 30;
    uint32_t offset_lba = 0;
    uint64_t unmapOffset = offset_lba * LBASIZE;
    uint32_t num_lbas2 = 40;
    uint32_t offset_lba2 = LBA_PER_OBJECT - num_lbas2;
    uint64_t unmapOffset2 = offset_lba2 * LBASIZE;

    TestTask testTask(seqId++);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto writeBuffer = randomStrGen(length);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask->set(offset, writeBuffer->size());
    connectorPtr->executeTask(writeTask);

    fds::block::UnmapTask::unmap_vec_ptr write_vec(new fds::block::UnmapTask::unmap_vec);
    fds::block::UnmapTask::UnmapRange range;
    range.offset = offset_lba * LBASIZE;
    range.length = num_lbas * LBASIZE;
    write_vec->push_back(range);
    fds::block::UnmapTask::UnmapRange range2;
    range2.offset = (offset_lba2) * LBASIZE;
    range2.length = num_lbas2 * LBASIZE;
    write_vec->push_back(range2);
    TestTask testTask2(seqId++);
    auto unmapTask = new fds::block::UnmapTask(&testTask2, std::move(write_vec));
    connectorPtr->executeTask(unmapTask);

    auto unmapBuffer = std::make_shared<std::string>(LBASIZE, '\0');
    for (unsigned int i = 0; i < num_lbas; ++i) {
        writeBuffer->replace(unmapOffset + (i * LBASIZE), LBASIZE, *unmapBuffer);
    }
    for (unsigned int i = 0; i < num_lbas2; ++i) {
        writeBuffer->replace(unmapOffset2 + (i * LBASIZE), LBASIZE, *unmapBuffer);
    }

    TestTask testTask3(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask3);
    readTask->set(offset, length);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffer(writeBuffer));
}

// Run basic single write test multithreaded
TEST_F(AsyncTestConnectorFixture, AsyncWriteTestSimple) {
    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = 1;
    }

    auto testTask = new TestTask(0);
    auto writeTask = new fds::block::WriteTask(testTask);
    auto write_buffer = randomStrGen(64);
    writeTask->setWriteBuffer(write_buffer);
    writeTask->set(0, write_buffer->size());
    connectorPtr->executeTask(writeTask);

    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }

    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = 1;
    }

    auto testTask2 = new TestTask(1);
    auto readTask = new fds::block::ReadTask(testTask2);
    readTask->set(0, write_buffer->size());
    connectorPtr->executeTask(readTask);
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }
    EXPECT_TRUE(connectorPtr->verifyBuffer(write_buffer));
}

// Do 64 4k writes
TEST_F(AsyncTestConnectorFixture, AsyncWriteTest_qd64_4k) {
    uint32_t queueDepth = 64;
    uint32_t writeSize = 4096;
    uint64_t seqId = 0;
    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = queueDepth;
    }

    auto expectBuffer = std::make_shared<std::string>();
    std::vector<std::shared_ptr<std::string>> writes;
    for (uint32_t i = 0; i < queueDepth; ++i) {
        auto writeBuffer = randomStrGen(writeSize);
        *expectBuffer += *writeBuffer;
        writes.push_back(writeBuffer);
    }

    uint64_t offset = 0;
    for (auto& w : writes) {
        auto testTask = new TestTask(seqId++);
        auto writeTask = new fds::block::WriteTask(testTask);
        writeTask->setWriteBuffer(w);
        writeTask->set(offset, w->size());
        offset += w->size();
        connectorPtr->executeTask(writeTask);
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }

    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = 1;
    }

    auto testTask2 = new TestTask(seqId++);
    auto readTask = new fds::block::ReadTask(testTask2);
    readTask->set(0, expectBuffer->size());
    connectorPtr->executeTask(readTask);
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }
    EXPECT_TRUE(connectorPtr->verifyBuffer(expectBuffer));
}

// Do 4 128k writes
// Aligned writes
TEST_F(AsyncTestConnectorFixture, AsyncWriteTest_qd4_128k) {
    uint32_t queueDepth = 4;
    uint32_t writeSize = OBJECTSIZE;
    uint64_t seqId = 0;
    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = queueDepth;
    }

    auto expectBuffer = std::make_shared<std::string>();
    std::vector<std::shared_ptr<std::string>> writes;
    for (uint32_t i = 0; i < queueDepth; ++i) {
        auto writeBuffer = randomStrGen(writeSize);
        *expectBuffer += *writeBuffer;
        writes.push_back(writeBuffer);
    }

    uint64_t offset = 0;
    for (auto& w : writes) {
        auto testTask = new TestTask(seqId++);
        auto writeTask = new fds::block::WriteTask(testTask);
        writeTask->setWriteBuffer(w);
        writeTask->set(offset, w->size());
        offset += w->size();
        connectorPtr->executeTask(writeTask);
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }

    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = 1;
    }

    auto testTask2 = new TestTask(seqId++);
    auto readTask = new fds::block::ReadTask(testTask2);
    readTask->set(0, expectBuffer->size());
    connectorPtr->executeTask(readTask);
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }
    EXPECT_TRUE(connectorPtr->verifyBuffer(expectBuffer));
}

// Do 8 163840 byte writes
// Larger than object size writes
TEST_F(AsyncTestConnectorFixture, AsyncWriteTest_qd8_163840) {
    uint32_t queueDepth = 8;
    uint32_t writeSize = 163840;
    uint64_t seqId = 0;
    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = queueDepth;
    }

    auto expectBuffer = std::make_shared<std::string>();
    std::vector<std::shared_ptr<std::string>> writes;
    for (uint32_t i = 0; i < queueDepth; ++i) {
        auto writeBuffer = randomStrGen(writeSize);
        *expectBuffer += *writeBuffer;
        writes.push_back(writeBuffer);
    }

    uint64_t offset = 0;
    for (auto& w : writes) {
        auto testTask = new TestTask(seqId++);
        auto writeTask = new fds::block::WriteTask(testTask);
        writeTask->setWriteBuffer(w);
        writeTask->set(offset, w->size());
        offset += w->size();
        connectorPtr->executeTask(writeTask);
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }

    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = 1;
    }

    auto testTask2 = new TestTask(seqId++);
    auto readTask = new fds::block::ReadTask(testTask2);
    readTask->set(0, expectBuffer->size());
    connectorPtr->executeTask(readTask);
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }
    EXPECT_TRUE(connectorPtr->verifyBuffer(expectBuffer));
}

// Do 2 8k writes at same offset
TEST_F(AsyncTestConnectorFixture, AsyncWriteTest_qd2_8k_overwrite) {
    uint32_t queueDepth = 2;
    uint32_t writeSize = 8192;
    uint64_t writeOffset = 4096;
    uint64_t seqId = 0;
    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = queueDepth;
    }

    std::vector<std::shared_ptr<std::string>> writes;
    for (uint32_t i = 0; i < queueDepth; ++i) {
        auto writeBuffer = randomStrGen(writeSize);
        writes.push_back(writeBuffer);
    }

    for (auto& w : writes) {
        auto testTask = new TestTask(seqId++);
        auto writeTask = new fds::block::WriteTask(testTask);
        writeTask->setWriteBuffer(w);
        writeTask->set(writeOffset, w->size());
        connectorPtr->executeTask(writeTask);
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }

    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = 1;
    }

    auto testTask2 = new TestTask(seqId++);
    auto readTask = new fds::block::ReadTask(testTask2);
    readTask->set(writeOffset, writeSize);
    connectorPtr->executeTask(readTask);
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }
    EXPECT_TRUE(connectorPtr->verifyBuffers(writes));
}

// Do 2 overlapping
// |xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx----------|
// |----------OOOOOOOOOOOOOOOOOOOOOOOOOOOOOO|
// expect
// |xxxxxxxxxxOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO|
// or
// |xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxOOOOOOOOOO|
TEST_F(AsyncTestConnectorFixture, AsyncWriteTest_qd2_overlapping) {
    uint32_t queueDepth = 2;
    uint32_t writeSize = 98304;
    uint64_t seqId = 0;
    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = queueDepth;
    }

    auto expectBuffer = std::make_shared<std::string>(OBJECTSIZE, '\0');
    auto expectBuffer2 = std::make_shared<std::string>(OBJECTSIZE, '\0');

    std::vector<std::shared_ptr<std::string>> bufs;
    auto writeBuffer = randomStrGen(writeSize);
    expectBuffer->replace(0, writeSize, *writeBuffer);
    auto writeBuffer2 = randomStrGen(writeSize);
    expectBuffer->replace(32768, writeSize, *writeBuffer2);
    expectBuffer2->replace(32768, writeSize, *writeBuffer2);
    expectBuffer2->replace(0, writeSize, *writeBuffer);
    bufs.push_back(expectBuffer);
    bufs.push_back(expectBuffer2);

    auto testTask = new TestTask(seqId++);
    auto testTask2 = new TestTask(seqId++);
    auto writeTask = new fds::block::WriteTask(testTask);
    auto writeTask2 = new fds::block::WriteTask(testTask2);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask2->setWriteBuffer(writeBuffer2);
    writeTask->set(0, writeBuffer->size());
    writeTask2->set(32768, writeBuffer->size());
    connectorPtr->executeTask(writeTask);
    connectorPtr->executeTask(writeTask2);

    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }

    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = 1;
    }

    auto testTask3 = new TestTask(seqId++);
    auto readTask = new fds::block::ReadTask(testTask3);
    readTask->set(0, OBJECTSIZE);
    connectorPtr->executeTask(readTask);
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }
    EXPECT_TRUE(connectorPtr->verifyBuffers(bufs));
}

// Write 128k block, partially overwritten by a smaller 8k block
// |xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx|
// |----------000000------------------------|
// expect
// |xxxxxxxxxx000000xxxxxxxxxxxxxxxxxxxxxxxx|
// or
// |xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx|
TEST_F(AsyncTestConnectorFixture, AsyncWriteTest_qd2_overlapping2) {
    uint32_t queueDepth = 2;
    uint32_t writeSize = 8192;
    uint64_t writeOffset = 32768;
    uint64_t seqId = 0;
    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = queueDepth;
    }

    auto expectBuffer = std::make_shared<std::string>(OBJECTSIZE, '\0');

    std::vector<std::shared_ptr<std::string>> bufs;
    auto writeBuffer = randomStrGen(OBJECTSIZE);
    bufs.push_back(writeBuffer);
    expectBuffer->replace(0, OBJECTSIZE, *writeBuffer);
    auto writeBuffer2 = randomStrGen(writeSize);
    expectBuffer->replace(writeOffset, writeSize, *writeBuffer2);
    bufs.push_back(expectBuffer);

    auto testTask = new TestTask(seqId++);
    auto testTask2 = new TestTask(seqId++);
    auto writeTask = new fds::block::WriteTask(testTask);
    auto writeTask2 = new fds::block::WriteTask(testTask2);
    writeTask->setWriteBuffer(writeBuffer);
    writeTask2->setWriteBuffer(writeBuffer2);
    writeTask->set(0, writeBuffer->size());
    writeTask2->set(writeOffset, writeBuffer2->size());
    connectorPtr->executeTask(writeTask);
    connectorPtr->executeTask(writeTask2);

    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }

    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = 1;
    }

    auto testTask3 = new TestTask(seqId++);
    auto readTask = new fds::block::ReadTask(testTask3);
    readTask->set(0, OBJECTSIZE);
    connectorPtr->executeTask(readTask);
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }
    EXPECT_TRUE(connectorPtr->verifyBuffers(bufs));
}

// Single write that spans 3 objects
// WriteSame that overwrites object in the middle
TEST_F(AsyncTestConnectorFixture, AsyncWriteTest_qd2_write_overlapping_writeSame) {
    uint32_t queueDepth = 2;
    uint32_t writeSize = 3 * OBJECTSIZE;
    uint64_t writeOffset = 0;
    uint64_t seqId = 0;
    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = queueDepth;
    }

    auto lbaBuffer = randomStrGen(LBASIZE);

    auto writeBuffer = randomStrGen(writeSize);
    auto testTask = new TestTask(seqId++);
    auto testTask2 = new TestTask(seqId++);
    auto writeTask = new fds::block::WriteTask(testTask);
    auto writeSameTask = new fds::block::WriteSameTask(testTask2);
    writeTask->setWriteBuffer(writeBuffer);
    writeSameTask->setWriteBuffer(lbaBuffer);
    writeTask->set(writeOffset, writeBuffer->size());
    writeSameTask->set(OBJECTSIZE, OBJECTSIZE);
    connectorPtr->executeTask(writeTask);
    connectorPtr->executeTask(writeSameTask);

    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }

    {
        std::lock_guard<std::mutex> lg(mutex);
        count = 0;
        expected = 1;
    }

    auto testTask3 = new TestTask(seqId++);
    auto readTask = new fds::block::ReadTask(testTask3);
    readTask->set(0, 3 * OBJECTSIZE);
    connectorPtr->executeTask(readTask);
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cond_var.wait_for(lock, std::chrono::seconds(5), [&] { return count == expected; }));
    }
    EXPECT_TRUE(connectorPtr->verifyBuffer(writeBuffer));
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    xdi::SetTestLogger(xdi::createLogger("gtestBlockOperations"));
    auto seed = time(NULL);

    std::cout << "Seed value: " << seed << std::endl;

    std::srand(seed);
    return RUN_ALL_TESTS();
}
