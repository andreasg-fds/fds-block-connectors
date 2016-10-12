set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
add_library (block
		BlockOperations.cpp
		BlockTools.cpp
		Tasks.cpp
		WriteContext.cpp)

target_link_libraries (block log)
