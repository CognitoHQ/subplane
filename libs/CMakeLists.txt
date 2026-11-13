# subplane-lib: rate limits and primary/secondary election shared across processes.
# Standalone: nothing in the agent links it yet. C++20 like the app SDK, so other
# repos (subplane-feeds) can link it.

add_library(subplane_lib STATIC
    src/elect.cpp
    src/fake_store.cpp
    src/rate.cpp
    src/runner.cpp
)
set_target_properties(subplane_lib PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON POSITION_INDEPENDENT_CODE ON)
target_include_directories(subplane_lib PUBLIC ${CMAKE_SOURCE_DIR}/include PRIVATE src)
target_link_libraries(subplane_lib PUBLIC Threads::Threads PRIVATE subplane_warnings)
if(SUBPLANE_WITH_VALKEY)
    target_sources(subplane_lib PRIVATE src/valkey_store.cpp)
    target_link_libraries(subplane_lib PRIVATE subplane_valkey)
    target_compile_definitions(subplane_lib PUBLIC SUBPLANE_WITH_VALKEY=1)
endif()

if(SUBPLANE_BUILD_TESTS)
    add_executable(test_lib ${CMAKE_SOURCE_DIR}/tests/unit/test_main.cpp tests/test_rate.cpp tests/test_elect.cpp)
    target_link_libraries(test_lib PRIVATE subplane_lib doctest subplane_warnings)
    target_include_directories(test_lib PRIVATE src)
    add_test(NAME test_lib COMMAND test_lib)

    if(SUBPLANE_WITH_VALKEY)
        # Against a real valkey-server, started by the script; skipped (77) without one.
        add_executable(lib_client tests/lib_client.cpp)
        target_link_libraries(lib_client PRIVATE subplane_lib subplane_warnings)
        add_executable(test_lib_valkey ${CMAKE_SOURCE_DIR}/tests/unit/test_main.cpp tests/test_valkey_store.cpp)
        target_link_libraries(test_lib_valkey PRIVATE subplane_lib doctest subplane_warnings)
        add_test(NAME lib_valkey
                 COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/valkey_e2e.sh $<TARGET_FILE:test_lib_valkey> $<TARGET_FILE:lib_client>)
        set_tests_properties(lib_valkey PROPERTIES SKIP_RETURN_CODE 77)
    endif()
endif()
