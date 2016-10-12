cmake_minimum_required (VERSION 2.8)
project (fds-block-connectors)

set (FDS_INSTALL "/opt/fds/formation_one")
set (EXTERN_FDS_LIBS "${FDS_INSTALL}/lib")

find_package (Threads)

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

# Locate all Formation dependencies
find_library (LIB_XDI xdi PATHS "${EXTERN_FDS_LIBS}")

# Locate all 3rd party dependencies
find_library (LIB_BOOSTSYSTEM boost_system PATHS "${EXTERN_FDS_LIBS}")
find_library (LIB_BOOSTTHREAD boost_thread PATHS "${EXTERN_FDS_LIBS}")
find_library (LIB_BOOSTLOG boost_log PATHS "${EXTERN_FDS_LIBS}")


add_subdirectory (src)
