# Enable ExternalProject CMake module
include(ExternalProject)

# Download and install Spdlog
ExternalProject_Add(
    spdlog
    URL https://github.com/gabime/spdlog/archive/v0.11.0.zip
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/spdlog
    # Disable install step
    CONFIGURE_COMMAND true
    BUILD_COMMAND true
    INSTALL_COMMAND true
)

ExternalProject_Get_Property(spdlog source_dir)

include_directories("${source_dir}/include")
