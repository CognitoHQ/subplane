# Example apps. They depend only on the client headers (plus core/clock.h), not on flashtick_core.
add_executable(shm_demo_app shm_demo_app.cpp)
target_include_directories(shm_demo_app PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(shm_demo_app PRIVATE flashtick_warnings Threads::Threads)

add_executable(rule_runner rule_runner.cpp)
target_include_directories(rule_runner PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(rule_runner PRIVATE flashtick_warnings Threads::Threads)

# Canonical views consumer: the generated header (schemas/CMakeLists.txt) plus the client headers.
if(TARGET views_flashtick_canonical)
    add_executable(views_app views_app.cpp ${FT_GENERATED_DIR}/flashtick_canonical_views.h)
    target_include_directories(views_app PRIVATE ${CMAKE_SOURCE_DIR}/include ${FT_GENERATED_DIR})
    target_link_libraries(views_app PRIVATE flashtick_warnings Threads::Threads)
    add_dependencies(views_app views_flashtick_canonical)
endif()
