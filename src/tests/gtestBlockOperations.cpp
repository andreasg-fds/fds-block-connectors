#include <gtest/gtest.h>
#include "connector/block/BlockOperations.h"
#include "stub/FdsStub.h"
#include "stub/ApiStub.h"
#include "connector/block/Tasks.h"

static const uint32_t OBJECTSIZE = 131072;

class TestConnector : public fds::block::BlockOperations {
public:
    TestConnector(std::shared_ptr<xdi::ApiInterface> interface) : fds::block::BlockOperations(interface) {}
    ~TestConnector() {}

    void respondTask(fds::block::BlockTask* response) override {

    }
};

class TestTask : public fds::block::ProtoTask {
public:
    TestTask(uint64_t const hdl) : fds::block::ProtoTask(hdl) {}
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
    std::shared_ptr<fds::block::BlockOperations> connectorPtr;
    void SetUp() {
        stubPtr = std::make_shared<xdi::FdsStub>();
        interfacePtr = std::make_shared<xdi::ApiStub>(stubPtr, 0);
        connectorPtr = std::make_shared<TestConnector>(interfacePtr);
        connectorPtr->init("testVol", 0, OBJECTSIZE);
    };
    void TearDown() {

    };
};

TEST_F(TestConnectorFixture, WriteTest) {
    TestTask testTask(0);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto write_buffer = randomStrGen(64);
    writeTask->setWriteBuffer(write_buffer);
    writeTask->set(0, write_buffer->size());
    connectorPtr->executeTask(writeTask);

    EXPECT_EQ(1, stubPtr->getNumObjects());
    EXPECT_EQ(1, stubPtr->getNumBlobs());
}

TEST_F(TestConnectorFixture, WriteTestMultiple) {
    TestTask testTask(0);
    auto writeTask = new fds::block::WriteTask(&testTask);
    auto write_buffer = randomStrGen(OBJECTSIZE*2);
    writeTask->setWriteBuffer(write_buffer);
    writeTask->set(0, write_buffer->size());
    connectorPtr->executeTask(writeTask);

    EXPECT_EQ(2, stubPtr->getNumObjects());
    EXPECT_EQ(1, stubPtr->getNumBlobs());
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    uint32_t seed = 1;

    std::cout << "Seed value: " << seed << std::endl;

    std::srand(seed);
    return RUN_ALL_TESTS();
}