#include <gtest/gtest.h>
#include "connector/block/BlockOperations.h"
#include "stub/FdsStub.h"

class TestConnector : public fds::block::BlockOperations {
    TestConnector(std::shared_ptr<xdi::ApiInterface> interface) : fds::block::BlockOperations(interface) {}
    ~TestConnector() {}
};

class TestConnectorFixture : public ::testing::Test {
protected:
    std::shared_ptr<xdi::FdsStub> stubPtr;
    void SetUp() {

    };
    void TearDown() {

    };
};

    int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    //fds::g_fdslog = new fds::fds_log("amdataprovidertest", "", fds::fds_log::debug);
#if 0
    po::options_description opts("Allowed options");
    opts.add_options()
        ("help", "produce help message")
        ("puts-cnt", po::value<int>()->default_value(1), "puts count");
    AmCacheTest::init(argc, argv, opts);
#endif
    return RUN_ALL_TESTS();
}