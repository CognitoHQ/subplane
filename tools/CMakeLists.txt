# Operator tools (cold path).
set(FT_DEFAULT_SCHEMA "${CMAKE_SOURCE_DIR}/tests/data/sample_md.xml")

foreach(tool ft_sbe_sender ft_probe)
    if(tool STREQUAL "ft_sbe_sender")
        set(src sbe_sender.cpp)
    else()
        set(src ft_probe.cpp)
    endif()
    add_executable(${tool} ${src})
    target_link_libraries(${tool} PRIVATE flashtick_core flashtick_warnings)
    target_include_directories(${tool} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_compile_definitions(${tool} PRIVATE FT_DEFAULT_SCHEMA="${FT_DEFAULT_SCHEMA}")
endforeach()

add_executable(ft_dbn_sim dbn_sim.cpp)
target_link_libraries(ft_dbn_sim PRIVATE flashtick_core flashtick_warnings)
target_include_directories(ft_dbn_sim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(ft_tail ft_tail.cpp)
target_link_libraries(ft_tail PRIVATE flashtick_core flashtick_warnings)

# M8: pcap replay + in-process pipeline benchmark, shared-memory ring chaos test.
add_executable(ft_replay ft_replay.cpp)
target_link_libraries(ft_replay PRIVATE flashtick_core flashtick_warnings)
target_include_directories(ft_replay PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_definitions(ft_replay PRIVATE FT_DEFAULT_SCHEMA="${FT_DEFAULT_SCHEMA}")

add_executable(ring_soak ring_soak.cpp)
target_link_libraries(ring_soak PRIVATE flashtick_core flashtick_warnings)
target_include_directories(ring_soak PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Root enables testing via FLASHTICK_BUILD_TESTS (CTest module not included, so BUILD_TESTING is unset).
if(BUILD_TESTING OR FLASHTICK_BUILD_TESTS)
    add_test(NAME sender_probe
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_sender_probe.sh
                $<TARGET_FILE:ft_sbe_sender> $<TARGET_FILE:ft_probe>
                ${CMAKE_SOURCE_DIR}/tests/data/sample_md.xml)

    # ring_soak substitutes <pid> in --path itself.
    add_test(NAME ring_soak_short
        COMMAND $<TARGET_FILE:ring_soak> --role harness --readers 2 --seconds 2
                --path /dev/shm/flashtick.soak.<pid>.ring)
    add_test(NAME replay_bench
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_replay_bench.sh
                $<TARGET_FILE:ft_replay> ${CMAKE_SOURCE_DIR}/config.yaml
                ${CMAKE_SOURCE_DIR}/tests/data/sample_md.xml)
    # Real CME MDP 3.0 packets; skipped unless scripts/fetch-cme-sample.sh has run.
    add_test(NAME cme_probe
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_cme_probe.sh
                $<TARGET_FILE:ft_replay> $<TARGET_FILE:ft_probe> ${CMAKE_BINARY_DIR}/cme-sample)
    set_tests_properties(cme_probe PROPERTIES SKIP_RETURN_CODE 77)
    # DBN over TCP: simulator -> agent -> ring, synthetic records, no data files needed.
    add_test(NAME dbn_sim
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_dbn_sim.sh
                $<TARGET_FILE:ft_dbn_sim> $<TARGET_FILE:flashtickd> $<TARGET_FILE:ft_tail>)
    # FlatBuffers over UDP: pcap of real flatc buffers (1..4 per datagram) -> agent -> ring.
    add_test(NAME flatbuffers_replay
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_flatbuffers_replay.sh
                $<TARGET_FILE:ft_replay> $<TARGET_FILE:flashtickd> $<TARGET_FILE:ft_tail>
                ${CMAKE_SOURCE_DIR}/tests/fixtures/flatbuffers)
endif()
