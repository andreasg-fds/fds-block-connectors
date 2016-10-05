#include <gtest/gtest.h>
#include "connector/block/BlockOperations.h"
#include "stub/FdsStub.h"
#include "stub/ApiStub.h"
#include "connector/block/Tasks.h"

static const uint32_t OBJECTSIZE = 131072;

class TestTask : public fds::block::ProtoTask {
public:
    TestTask(uint64_t const hdl) : fds::block::ProtoTask(hdl) {}
};

class TestConnector : public fds::block::BlockOperations {
public:
    TestConnector(std::shared_ptr<xdi::ApiInterface> interface) : fds::block::BlockOperations(interface) {}
    ~TestConnector() {}

    bool verifyBuffers(std::shared_ptr<std::string>& buf) {
        if ((buf->size() != readBuffer->size()) || (*buf != *readBuffer)) return false;
        return true;
    }

    void respondTask(fds::block::BlockTask* response) override {
        auto task = static_cast<TestTask*>(response->getProtoTask());
        fds::block::TaskVisitor v;
        if (fds::block::TaskType::READ == response->match(&v)) {
            auto btask = static_cast<fds::block::ReadTask*>(response);
            readBuffer.reset(new std::string());
            uint32_t i = 0, context = 0;
            std::shared_ptr<std::string> buf = btask->getNextReadBuffer(context);
            while (buf != NULL) {
                *readBuffer += *buf;
                i += buf->length();
                buf = btask->getNextReadBuffer(context);
            }
        }
    }
private:
    std::shared_ptr<std::string> readBuffer;
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
        connectorPtr = std::make_shared<TestConnector>(interfacePtr);
        connectorPtr->init("testVol", 0, OBJECTSIZE);
    };
    void TearDown() {

    };
};

// Test reading nonexisting blob/object
// Read full object at offset 0
TEST_F(TestConnectorFixture, ReadNonExisting) {
    uint64_t seqId = 0;
    TestTask testTask(seqId++);
    auto readTask = new fds::block::ReadTask(&testTask);
    auto fullBuf = std::make_shared<std::string>(OBJECTSIZE, '\0');
    readTask->set(0, OBJECTSIZE);
    connectorPtr->executeTask(readTask);
    EXPECT_TRUE(connectorPtr->verifyBuffers(fullBuf));
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
    EXPECT_TRUE(connectorPtr->verifyBuffers(fullBuf));
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
    EXPECT_TRUE(connectorPtr->verifyBuffers(fullBuf));
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
    EXPECT_TRUE(connectorPtr->verifyBuffers(write_buffer));
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
    EXPECT_TRUE(connectorPtr->verifyBuffers(write_buffer));
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
    EXPECT_TRUE(connectorPtr->verifyBuffers(fullBuf));
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
    EXPECT_TRUE(connectorPtr->verifyBuffers(fullBuf));
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
    EXPECT_TRUE(connectorPtr->verifyBuffers(fullBuf));
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
    EXPECT_TRUE(connectorPtr->verifyBuffers(write_buffer));
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
    EXPECT_TRUE(connectorPtr->verifyBuffers(fullBuf));
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
    EXPECT_TRUE(connectorPtr->verifyBuffers(fullBuf));
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    uint32_t seed = 1;

    std::cout << "Seed value: " << seed << std::endl;

    std::srand(seed);
    return RUN_ALL_TESTS();
}