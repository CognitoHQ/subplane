# libvalkey, the C client subplane-lib's ValkeyStore speaks through (BSD-3-Clause).
#
# Pinned tag, fetched at configure time, built static with no TLS, RDMA, tests or examples.
# Offline or vendored builds point FETCHCONTENT_SOURCE_DIR_LIBVALKEY at a checkout.
# SUBPLANE_WITH_VALKEY=OFF builds subplane-lib without ValkeyStore.
include(FetchContent)

set(SUBPLANE_LIBVALKEY_TAG "0.5.0" CACHE STRING "libvalkey git tag to build against")

set(DISABLE_TESTS ON CACHE BOOL "" FORCE)
set(ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
set(ENABLE_TLS OFF CACHE BOOL "" FORCE)
set(ENABLE_RDMA OFF CACHE BOOL "" FORCE)

# INTENT: libvalkey reads BUILD_SHARED_LIBS (default ON) for its own library type.
# Set it OFF only while libvalkey configures, so the static archive links into
# subplane_lib and nothing else in the tree sees the change.
set(_subplane_shared ${BUILD_SHARED_LIBS})
set(BUILD_SHARED_LIBS OFF)
FetchContent_Declare(libvalkey
    GIT_REPOSITORY https://github.com/valkey-io/libvalkey.git
    GIT_TAG        ${SUBPLANE_LIBVALKEY_TAG}
    GIT_SHALLOW    TRUE
    GIT_PROGRESS   FALSE
    EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(libvalkey)
set(BUILD_SHARED_LIBS ${_subplane_shared})
set_target_properties(valkey PROPERTIES POSITION_INDEPENDENT_CODE ON)

# INTENT: libvalkey's build tree exposes include/valkey (so <valkey.h>), its install
# tree include/ (so <valkey/valkey.h>). subplane_valkey gives the installed spelling
# in both, as SYSTEM so the third-party headers stay outside -Wpedantic/-Werror.
add_library(subplane_valkey INTERFACE)
target_link_libraries(subplane_valkey INTERFACE valkey::valkey)
target_include_directories(subplane_valkey SYSTEM INTERFACE ${libvalkey_SOURCE_DIR}/include)
