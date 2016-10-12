set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
add_library (log
        Log.cpp)

target_link_libraries (log boost_system boost_thread boost_log)