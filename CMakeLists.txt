cmake_minimum_required (VERSION 2.8)
project (fds-block-connectors)

set (FDS_INSTALL "/opt/fds/formation_one")
set (EXTERN_FDS_LIBS "${FDS_INSTALL}/lib")
set (CMAKE_INSTALL_PREFIX "${FDS_INSTALL}/packages/${PROJECT_NAME}")

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


add_subdirectory (src)

set (CPACK_PACKAGE_NAME			"${PROJECT_NAME}")
set (CPACK_PACKAGE_VERSION_MAJOR	"1")
set (CPACK_PACKAGE_VERSION_MINOR	"0")
set (CPACK_PACKAGE_VERSION_PATCH	"4")
set (CPACK_PACKAGE_VERSION		"${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}")
set (CPACK_DEBIAN_PACKAGE_ARCHITECTURE	"amd64")
set (CPACK_DEBIAN_PACKAGE_DESCRIPTION	"Block connectors for Formation One")
set (CPACK_DEBIAN_PACKAGE_MAINTAINER	"Brian Szmyd <szmyd@formationds.com>")
set (CPACK_DEBIAN_PACKAGE_NAME		"fds-block-connectors")
set (CPACK_DEBIAN_PACKAGE_SECTION	"Formation Data Systems")
set (CPACK_PACKAGING_INSTALL_PREFIX	"${CMAKE_INSTALL_PREFIX}")
set (CPACK_SYSTEM_NAME			"amd64")
set (CPACK_PACKAGE_FILE_NAME	 	"${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}_${CPACK_PACKAGE_VERSION}_${CPACK_SYSTEM_NAME}")
include (CPack)
