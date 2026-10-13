# Operator tools (cold path).
set(SP_DEFAULT_SCHEMA "${CMAKE_SOURCE_DIR}/tests/data/sample_md.xml")

foreach(tool sp_sbe_sender sp_probe)
    if(tool STREQUAL "sp_sbe_sender")
        set(src sbe_sender.cpp)
    else()
        set(src sp_probe.cpp)
    endif()
    add_executable(${tool} ${src})
    target_link_libraries(${tool} PRIVATE subplane_core subplane_warnings)
    target_include_directories(${tool} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_compile_definitions(${tool} PRIVATE SP_DEFAULT_SCHEMA="${SP_DEFAULT_SCHEMA}")
endforeach()

add_executable(sp_dbn_sim dbn_sim.cpp)
target_link_libraries(sp_dbn_sim PRIVATE subplane_core subplane_warnings)
target_include_directories(sp_dbn_sim PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(sp_schema sp_schema.cpp)
target_link_libraries(sp_schema PRIVATE subplane_core subplane_warnings)
target_include_directories(sp_schema PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Aeron publisher of canonical messages: test peer and proof for the `aeron` transport.
if(SUBPLANE_WITH_AERON AND TARGET views_subplane_canonical)
    add_executable(sp_aeron_pub aeron_pub.cpp ${SP_GENERATED_DIR}/subplane_canonical_views.h)
    target_include_directories(sp_aeron_pub PRIVATE ${SP_GENERATED_DIR})
    target_link_libraries(sp_aeron_pub PRIVATE subplane_aeron subplane_warnings Threads::Threads)
    add_dependencies(sp_aeron_pub views_subplane_canonical)
endif()

add_executable(sp_tail sp_tail.cpp)
target_link_libraries(sp_tail PRIVATE subplane_core subplane_warnings)

# M8: pcap replay + in-process pipeline benchmark, shared-memory ring chaos test.
add_executable(sp_replay sp_replay.cpp)
target_link_libraries(sp_replay PRIVATE subplane_core subplane_warnings)
target_include_directories(sp_replay PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_definitions(sp_replay PRIVATE SP_DEFAULT_SCHEMA="${SP_DEFAULT_SCHEMA}")

add_executable(ring_soak ring_soak.cpp)
target_link_libraries(ring_soak PRIVATE subplane_core subplane_warnings)
target_include_directories(ring_soak PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Root enables testing via SUBPLANE_BUILD_TESTS (CTest module not included, so BUILD_TESTING is unset).
if(BUILD_TESTING OR SUBPLANE_BUILD_TESTS)
    add_test(NAME sender_probe
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_sender_probe.sh
                $<TARGET_FILE:sp_sbe_sender> $<TARGET_FILE:sp_probe>
                ${CMAKE_SOURCE_DIR}/tests/data/sample_md.xml)

    # ring_soak substitutes <pid> in --path itself.
    add_test(NAME ring_soak_short
        COMMAND $<TARGET_FILE:ring_soak> --role harness --readers 2 --seconds 2
                --path /dev/shm/subplane.soak.<pid>.ring)
    add_test(NAME replay_bench
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_replay_bench.sh
                $<TARGET_FILE:sp_replay> ${CMAKE_SOURCE_DIR}/config.yaml
                ${CMAKE_SOURCE_DIR}/tests/data/sample_md.xml)
    # Real CME MDP 3.0 packets; skipped unless scripts/fetch-cme-sample.sh has run.
    add_test(NAME cme_probe
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_cme_probe.sh
                $<TARGET_FILE:sp_replay> $<TARGET_FILE:sp_probe> ${CMAKE_BINARY_DIR}/cme-sample)
    set_tests_properties(cme_probe PROPERTIES SKIP_RETURN_CODE 77)
    # DBN over TCP: simulator -> agent -> ring, synthetic records, no data files needed.
    add_test(NAME dbn_sim
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_dbn_sim.sh
                $<TARGET_FILE:sp_dbn_sim> $<TARGET_FILE:subplaned> $<TARGET_FILE:sp_tail>)
    # Typed views: sp_schema's NumPy output loaded by python3 (skipped where numpy is absent).
    add_test(NAME schema_views_numpy
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_schema_views_numpy.sh
                $<TARGET_FILE:sp_schema> ${CMAKE_SOURCE_DIR}/tests/data/sample_md.xml ${CMAKE_SOURCE_DIR}/schemas/subplane_canonical.xml)
    set_tests_properties(schema_views_numpy PROPERTIES SKIP_RETURN_CODE 77)
    # Canonical schema end to end: sender (canonical profile) -> agent -> ring -> views_app (generated struct views).
    add_test(NAME canonical_views_app
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_canonical_views_app.sh
                $<TARGET_FILE:sp_sbe_sender> $<TARGET_FILE:subplaned> $<TARGET_FILE:views_app>
                ${CMAKE_SOURCE_DIR}/schemas/subplane_canonical.xml)
    # The sample publisher -> agent -> ring -> views_app.
    add_test(NAME canonical_pub
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_canonical_pub.sh
                $<TARGET_FILE:canonical_pub> $<TARGET_FILE:subplaned> $<TARGET_FILE:views_app>
                ${CMAKE_SOURCE_DIR}/schemas/subplane_canonical.xml)
    # Aeron ingest: sp_aeron_pub -> media driver -> agent (aeron transport) -> ring -> views_app.
    if(TARGET sp_aeron_pub)
        foreach(mode ipc udp)
            add_test(NAME aeron_${mode}
                COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_aeron_ingest.sh ${mode}
                        $<TARGET_FILE:sp_aeron_pub> $<TARGET_FILE:subplaned> $<TARGET_FILE:views_app>
                        ${CMAKE_SOURCE_DIR}/schemas/subplane_canonical.xml)
        endforeach()
    endif()
    # FlatBuffers over UDP: pcap of real flatc buffers (1..4 per datagram) -> agent -> ring.
    add_test(NAME flatbuffers_replay
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_flatbuffers_replay.sh
                $<TARGET_FILE:sp_replay> $<TARGET_FILE:subplaned> $<TARGET_FILE:sp_tail>
                ${CMAKE_SOURCE_DIR}/tests/fixtures/flatbuffers)
endif()
