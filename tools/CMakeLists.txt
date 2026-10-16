# The operator CLI: one binary, one subcommand per tool (cold path).
set(SP_DEFAULT_SCHEMA "${CMAKE_SOURCE_DIR}/tests/data/sample_md.xml")
set(SP_CANONICAL_SCHEMA "${CMAKE_SOURCE_DIR}/schemas/subplane_canonical.xml")

add_executable(subplane subplane.cpp probe.cpp tail.cpp schema.cpp send.cpp replay.cpp dbn_sim.cpp soak.cpp)
if(SUBPLANE_WITH_AERON)
    target_sources(subplane PRIVATE aeron_pub.cpp)
endif()
target_link_libraries(subplane PRIVATE subplane_core subplane_warnings)
target_include_directories(subplane PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_definitions(subplane PRIVATE
    SP_DEFAULT_SCHEMA="${SP_DEFAULT_SCHEMA}" SP_CANONICAL_SCHEMA="${SP_CANONICAL_SCHEMA}")
install(TARGETS subplane DESTINATION bin)

# Root enables testing via SUBPLANE_BUILD_TESTS (CTest module not included, so BUILD_TESTING is unset).
if(BUILD_TESTING OR SUBPLANE_BUILD_TESTS)
    set(SP $<TARGET_FILE:subplane>)
    add_test(NAME sender_probe
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_sender_probe.sh ${SP} ${SP_DEFAULT_SCHEMA})
    # soak substitutes <pid> in --path itself.
    add_test(NAME ring_soak_short
        COMMAND ${SP} soak --role harness --readers 2 --seconds 2 --path /dev/shm/subplane.soak.<pid>.ring)
    add_test(NAME replay_bench
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_replay_bench.sh ${SP} ${CMAKE_SOURCE_DIR}/config.yaml ${SP_DEFAULT_SCHEMA})
    # Real CME MDP 3.0 packets; skipped unless scripts/fetch-cme-sample.sh has run.
    add_test(NAME cme_probe
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_cme_probe.sh ${SP} ${CMAKE_BINARY_DIR}/cme-sample)
    set_tests_properties(cme_probe PROPERTIES SKIP_RETURN_CODE 77)
    # DBN over TCP: simulator -> agent -> ring, synthetic records, no data files needed.
    add_test(NAME dbn_sim
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_dbn_sim.sh ${SP} $<TARGET_FILE:subplaned>)
    # Generated Python views: dataclass round trip with any python3, NumPy dtype part when NumPy imports.
    add_test(NAME schema_views_numpy
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_schema_views_numpy.sh ${SP} ${SP_DEFAULT_SCHEMA} ${SP_CANONICAL_SCHEMA})
    set_tests_properties(schema_views_numpy PROPERTIES SKIP_RETURN_CODE 77)
    # Canonical schema end to end: send -> agent -> ring -> views_app (generated struct views).
    add_test(NAME canonical_views_app
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_canonical_views_app.sh
                ${SP} $<TARGET_FILE:subplaned> $<TARGET_FILE:views_app> ${SP_CANONICAL_SCHEMA})
    # Reference deployments (deploy/*/agent.yaml) start an agent.
    add_test(NAME reference_configs
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_reference_configs.sh $<TARGET_FILE:subplaned> ${CMAKE_SOURCE_DIR})
    # The sample publisher -> agent -> ring -> views_app.
    add_test(NAME canonical_pub
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_canonical_pub.sh
                $<TARGET_FILE:canonical_pub> $<TARGET_FILE:subplaned> $<TARGET_FILE:views_app> ${SP_CANONICAL_SCHEMA})
    # Aeron ingest: aeron-pub -> media driver -> agent (aeron transport) -> ring -> views_app.
    if(SUBPLANE_WITH_AERON)
        foreach(mode ipc udp)
            add_test(NAME aeron_${mode}
                COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_aeron_ingest.sh ${mode}
                        ${SP} $<TARGET_FILE:subplaned> $<TARGET_FILE:views_app> ${SP_CANONICAL_SCHEMA})
        endforeach()
    endif()
    # FlatBuffers over UDP: pcap of real flatc buffers (1..4 per datagram) -> agent -> ring.
    add_test(NAME flatbuffers_replay
        COMMAND ${CMAKE_SOURCE_DIR}/tests/integration/test_flatbuffers_replay.sh
                ${SP} $<TARGET_FILE:subplaned> ${CMAKE_SOURCE_DIR}/tests/fixtures/flatbuffers)
endif()
