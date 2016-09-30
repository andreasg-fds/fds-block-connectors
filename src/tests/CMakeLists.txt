include (gtest.cmake)

enable_testing()

add_executable(gtestWriteContext gtestWriteContext.cpp)
target_link_libraries(gtestWriteContext libgtest libgmock block)

add_test(writeContextTest gtestWriteContext)
