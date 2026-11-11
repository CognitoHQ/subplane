# Unit tests: one executable per area, all using the vendored doctest.
# tests/unit/test_main.cpp provides DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN.

function(subplane_unit_test name)
    add_executable(${name} unit/test_main.cpp ${ARGN})
    target_link_libraries(${name} PRIVATE subplane_core doctest subplane_warnings)
    add_test(NAME ${name} COMMAND ${name})
endfunction()

subplane_unit_test(test_core unit/test_message.cpp unit/test_api_server.cpp unit/test_prom_writer.cpp unit/test_endpoint_key.cpp unit/test_proc_scan.cpp)

# M1 core types
subplane_unit_test(test_core_types unit/test_filter_table.cpp unit/test_seqlock.cpp unit/test_histogram.cpp unit/test_symbol_table.cpp unit/test_lvc.cpp)
# M2 shared-memory ring + client
subplane_unit_test(test_ring unit/test_ring.cpp)
# Recordings: pcap/pcapng reader and writer (include/subplane/record)
subplane_unit_test(test_pcapng unit/test_pcapng.cpp)
subplane_unit_test(test_reader_slots unit/test_reader_slots.cpp)
# M3 SBE schema, length walker, routing decoder
subplane_unit_test(test_sbe unit/test_sbe_schema.cpp unit/test_sbe_length.cpp unit/test_sbe_decoder.cpp)
subplane_unit_test(test_spp_codec unit/test_spp_codec.cpp unit/test_sbe_lint.cpp)
# M4 transport + registry
subplane_unit_test(test_transport unit/test_transport.cpp)
subplane_unit_test(test_framing unit/test_framing.cpp)
# M5 pipeline
subplane_unit_test(test_pipeline unit/test_pipeline.cpp unit/test_tap.cpp)
subplane_unit_test(test_recap unit/test_recap_manager.cpp)
subplane_unit_test(test_recap_pipeline unit/test_recap_pipeline.cpp)
if(SUBPLANE_WITH_AERON)
    subplane_unit_test(test_recap_aeron unit/test_recap_aeron.cpp)
endif()
subplane_unit_test(test_session_manager unit/test_session_manager.cpp)
subplane_unit_test(test_topics unit/test_topics.cpp)
# M6 control channel
subplane_unit_test(test_control unit/test_control.cpp)
# DBN decoder, TCP stream transport, DBN through the pipeline
subplane_unit_test(test_dbn unit/test_dbn_decoder.cpp unit/test_tcp_stream.cpp unit/test_dbn_pipeline.cpp)
# Typed views emitter, and the headers generated for the sample and canonical schemas (schemas/CMakeLists.txt):
# subplane schema runs at build time so the static_asserts in the generated headers are checked on every build.
subplane_unit_test(test_sbe_views unit/test_sbe_views.cpp unit/test_sbe_views_generated.cpp ${SP_GENERATED_DIR}/sample_md_views.h)
target_include_directories(test_sbe_views PRIVATE ${SP_GENERATED_DIR} ${CMAKE_SOURCE_DIR}/tools)
add_dependencies(test_sbe_views views_sample_md)
subplane_unit_test(test_canonical unit/test_canonical_schema.cpp ${SP_GENERATED_DIR}/subplane_canonical_views.h)
target_include_directories(test_canonical PRIVATE ${SP_GENERATED_DIR} ${CMAKE_SOURCE_DIR}/tools)
add_dependencies(test_canonical views_subplane_canonical)
# FlatBuffers wire reader, routing decoder, pipeline (fixture: real flatc output in tests/fixtures/flatbuffers)
subplane_unit_test(test_flatbuffers unit/test_flatbuffers_wire.cpp unit/test_flatbuffers_decoder.cpp unit/test_flatbuffers_pipeline.cpp)
