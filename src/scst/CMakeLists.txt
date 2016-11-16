add_library (fds-am-connector-scst SHARED
		ScstAdmin.cpp
		ScstConnector.cpp
		ScstDevice.cpp
		ScstDisk.cpp
		ScstInquiry.cpp
		ScstMode.cpp
		ScstTarget.cpp
		ScstTask.cpp)
target_link_libraries (fds-am-connector-scst block ev config++)
install (TARGETS fds-am-connector-scst
		LIBRARY DESTINATION lib/connector)
