# SPDX-License-Identifier: Apache-2.0
# Example apps. They depend only on the client headers (plus core/clock.h), not on subplane_core.
# INTENT: apps compile the client headers and generated views with their own toolchains, so the
# examples build as C++20 while the agent is C++26 (INV-007). Raising this lets C++23/26 leak into
# the app SDK unnoticed.
set(CMAKE_CXX_STANDARD 20)
add_executable(shm_demo_app shm_demo_app.cpp)
target_include_directories(shm_demo_app PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(shm_demo_app PRIVATE subplane_warnings Threads::Threads)

add_executable(rule_runner rule_runner.cpp)
target_include_directories(rule_runner PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(rule_runner PRIVATE subplane_warnings Threads::Threads)

# Canonical views consumer: the generated header (schemas/CMakeLists.txt) plus the client headers.
if(TARGET views_subplane_canonical)
    add_executable(views_app views_app.cpp ${SP_GENERATED_DIR}/subplane_canonical_views.h)
    target_include_directories(views_app PRIVATE ${CMAKE_SOURCE_DIR}/include ${SP_GENERATED_DIR})
    target_link_libraries(views_app PRIVATE subplane_warnings Threads::Threads)
    add_dependencies(views_app views_subplane_canonical)
endif()

# The smallest publisher the agent can ingest: generated canonical structs + a socket. The file a gateway author copies.
if(TARGET views_subplane_canonical)
    add_executable(canonical_pub canonical_pub.cpp ${SP_GENERATED_DIR}/subplane_canonical_views.h)
    target_include_directories(canonical_pub PRIVATE ${SP_GENERATED_DIR})
    target_link_libraries(canonical_pub PRIVATE subplane_warnings Threads::Threads)
    add_dependencies(canonical_pub views_subplane_canonical)
endif()
