add_library (fds-am-connector-nbd SHARED
		NbdConnection.cpp
		NbdConnector.cpp
		NbdTask.cpp)
target_link_libraries (fds-am-connector-nbd log block ev)
