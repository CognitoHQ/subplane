# Aeron C client + C media driver, the agent's first real dependency (roadmap item 9).
#
# Pinned tag, fetched at configure time; no Java, no archive, no tests, no samples.
# Offline or vendored builds point FETCHCONTENT_SOURCE_DIR_AERON at a checkout.
# Aeron's own CMake needs >= 3.30 (Ubuntu 26.04 ships 4.2). SUBPLANE_WITH_AERON=OFF builds
# exactly the pre-Aeron agent.
include(FetchContent)

set(SUBPLANE_AERON_TAG "1.53.3" CACHE STRING "Aeron git tag to build against")

if(CMAKE_VERSION VERSION_LESS 3.30)
    message(FATAL_ERROR "Aeron ${SUBPLANE_AERON_TAG} needs CMake >= 3.30 (have ${CMAKE_VERSION}). "
                        "Install a newer CMake or configure with -DSUBPLANE_WITH_AERON=OFF.")
endif()

# Aeron's options default from STANDALONE_BUILD, which is off when it is a sub-project,
# but be explicit so an Aeron upgrade cannot drag Java/gradle (archive codecs) back in.
set(BUILD_AERON_DRIVER ON CACHE BOOL "" FORCE)
set(BUILD_AERON_ARCHIVE_API OFF CACHE BOOL "" FORCE)
set(AERON_TESTS OFF CACHE BOOL "" FORCE)
set(AERON_UNIT_TESTS OFF CACHE BOOL "" FORCE)
set(AERON_SYSTEM_TESTS OFF CACHE BOOL "" FORCE)
set(AERON_BUILD_SAMPLES OFF CACHE BOOL "" FORCE)
set(AERON_BUILD_DOCUMENTATION OFF CACHE BOOL "" FORCE)
set(AERON_INSTALL_TARGETS OFF CACHE BOOL "" FORCE)

FetchContent_Declare(aeron
    GIT_REPOSITORY https://github.com/aeron-io/aeron.git
    GIT_TAG        ${SUBPLANE_AERON_TAG}
    GIT_SHALLOW    TRUE
    GIT_PROGRESS   FALSE
    EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(aeron)

# INTENT: aeron_driver_static already contains the whole C client (Aeron builds the
# driver archive from client + driver sources), so the agent links that one archive.
# Linking aeron_static as well would duplicate every client symbol.
add_library(subplane_aeron INTERFACE)
target_link_libraries(subplane_aeron INTERFACE aeron_driver_static)
# Aeron headers are third-party: keep -Wpedantic/-Wconversion (and -Werror) off them.
target_include_directories(subplane_aeron SYSTEM INTERFACE
    ${aeron_SOURCE_DIR}/aeron-client/src/main/c
    ${aeron_SOURCE_DIR}/aeron-driver/src/main/c)
target_compile_definitions(subplane_aeron INTERFACE SUBPLANE_WITH_AERON=1)
