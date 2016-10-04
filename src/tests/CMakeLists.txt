include (gtest.cmake)

enable_testing()

add_executable(gtestWriteContext gtestWriteContext.cpp)
target_link_libraries(gtestWriteContext libgtest block)

add_executable(gtestBlockOperations gtestBlockOperations.cpp)
target_link_libraries(gtestBlockOperations libgtest block)

add_executable(gtestStub gtestStub.cpp)
target_link_libraries(gtestStub libgtest stub)

add_executable(gtestApiStub gtestApiStub.cpp)
target_link_libraries(gtestApiStub libgtest libgmock stub)

add_test(writeContextTest gtestWriteContext)
add_test(blockOperationsTest gtestBlockOperations)
add_test(stubTest gtestStub)
add_test(apiStubTest gtestApiStub)
