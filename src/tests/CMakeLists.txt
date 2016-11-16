include (gtest.cmake)

enable_testing()

add_executable(gtestWriteContext gtestWriteContext.cpp)
target_link_libraries(gtestWriteContext libgtest block)

add_executable(gtestBlockOperations gtestBlockOperations.cpp)
target_link_libraries(gtestBlockOperations libgtest block stub)

add_executable(gtestStub gtestStub.cpp)
target_link_libraries(gtestStub libgtest stub)

add_executable(gtestApiStub gtestApiStub.cpp)
target_link_libraries(gtestApiStub libgtest libgmock stub)

add_executable(gtestBlockTools gtestBlockTools.cpp)
target_link_libraries(gtestBlockTools libgtest block)

add_test(stubTest gtestStub)
add_test(apiStubTest gtestApiStub)
add_test(writeContextTest gtestWriteContext)
add_test(blockToolsTest gtestBlockTools)
add_test(blockOperationsTest gtestBlockOperations)
