cmake_minimum_required (VERSION 2.8)
project (fds-block-connectors)

set (CPACK_PACKAGE_VERSION_MAJOR	"1")
set (CPACK_PACKAGE_VERSION_MINOR	"0")
set (CPACK_PACKAGE_VERSION_PATCH	"4")
set (BUILD_VERSION			"9")
set (CPACK_PACKAGE_VERSION		"${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}-${BUILD_VERSION}")

set (FDS_INSTALL "/opt/fds/formation_one")
set (EXTERN_FDS_LIBS "${FDS_INSTALL}/lib")
set (CMAKE_INSTALL_PREFIX "${FDS_INSTALL}/packages/${PROJECT_NAME}-${CPACK_PACKAGE_VERSION}")

if (DEFINED ENV{build_type})
    message ("build_type: $ENV{build_type}")
    if ($ENV{build_type} STREQUAL "debug")
	set (CMAKE_BUILD_TYPE Debug)
    elseif ($ENV{build_type} STREQUAL "release")
	set (CMAKE_BUILD_TYPE RelWithDebInfo)
    else ()
	message(FATAL_ERROR "Invalid build_type: $ENV{build_type}")
    endif()
else ()
    message (STATUS "Using default build_type: Debug")
    set (CMAKE_BUILD_TYPE Debug)
endif ()
if (${CMAKE_BUILD_TYPE} STREQUAL Debug)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEBUG")
endif()

find_package (Threads)

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

# Locate all Formation dependencies
find_library (LIB_XDI xdi PATHS "${EXTERN_FDS_LIBS}")

# Locate all 3rd party dependencies
find_library (LIB_CONFIGPP config++ PATHS "${EXTERN_FDS_LIBS}")

set (ARTIFACT_DIR "${CMAKE_CURRENT_BINARY_DIR}/artifacts")
set (ARTIFACT_DIR_SCST "${ARTIFACT_DIR}-scst")
set (ARTIFACT_DIR_NBD  "${ARTIFACT_DIR}-nbd")
file (MAKE_DIRECTORY ${ARTIFACT_DIR_SCST})
file (MAKE_DIRECTORY "${ARTIFACT_DIR_SCST}/lib")
file (MAKE_DIRECTORY "${ARTIFACT_DIR_SCST}/lib/connector")
file (MAKE_DIRECTORY ${ARTIFACT_DIR_NBD})
file (MAKE_DIRECTORY "${ARTIFACT_DIR_NBD}/lib")
file (MAKE_DIRECTORY "${ARTIFACT_DIR_NBD}/lib/connector")

add_subdirectory (src)
