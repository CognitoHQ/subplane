# Example apps. They depend only on the client headers (plus core/clock.h), not on flashtick_core.
add_executable(shm_demo_app shm_demo_app.cpp)
target_include_directories(shm_demo_app PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(shm_demo_app PRIVATE flashtick_warnings Threads::Threads)

add_executable(rule_runner rule_runner.cpp)
target_include_directories(rule_runner PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(rule_runner PRIVATE flashtick_warnings Threads::Threads)
