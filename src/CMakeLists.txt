set (GCC_CPP_14_FLAGS "-std=c++1y")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GCC_CPP_14_FLAGS}")

set (FDS_INCLUDES "${FDS_INSTALL}/include")

include_directories (BEFORE include
			    ${FDS_INCLUDES})

add_subdirectory (lib)
add_subdirectory (nbd)
