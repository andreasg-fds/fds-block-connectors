set (GCC_CPP_14_FLAGS "-std=c++1y")
set (GCC_CPP_WARNINGS "-Wall -Wextra -Werror")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GCC_CPP_14_FLAGS} ${GCC_CPP_WARNINGS}")

set (FDS_INCLUDES "${FDS_INSTALL}/include")

find_library (LIB_EV ev PATHS "${EXTERN_FDS_LIBS}")

include_directories (BEFORE include)
include_directories (BEFORE SYSTEM ${FDS_INCLUDES})

add_subdirectory (lib)
add_subdirectory (nbd)
add_subdirectory (scst)
add_subdirectory (tests)
