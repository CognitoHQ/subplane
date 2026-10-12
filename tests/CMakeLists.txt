# Unit tests: one executable per area, all using the vendored doctest.
# tests/unit/test_main.cpp provides DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN.

function(flashtick_unit_test name)
    add_executable(${name} unit/test_main.cpp ${ARGN})
    target_link_libraries(${name} PRIVATE flashtick_core doctest flashtick_warnings)
    add_test(NAME ${name} COMMAND ${name})
endfunction()

flashtick_unit_test(test_core unit/test_message.cpp unit/test_api_server.cpp)

# M1 core types
flashtick_unit_test(test_core_types unit/test_filter_table.cpp unit/test_seqlock.cpp unit/test_histogram.cpp unit/test_symbol_table.cpp)
# M2 shared-memory ring + client
flashtick_unit_test(test_ring unit/test_ring.cpp)
# M3 SBE schema, length walker, routing decoder
flashtick_unit_test(test_sbe unit/test_sbe_schema.cpp unit/test_sbe_length.cpp unit/test_sbe_decoder.cpp)
# M4 transport + registry
flashtick_unit_test(test_transport unit/test_transport.cpp)
flashtick_unit_test(test_framing unit/test_framing.cpp)
# M5 pipeline
flashtick_unit_test(test_pipeline unit/test_pipeline.cpp)
flashtick_unit_test(test_session_manager unit/test_session_manager.cpp)
# M6 control channel
flashtick_unit_test(test_control unit/test_control.cpp)
# DBN decoder, TCP stream transport, DBN through the pipeline
flashtick_unit_test(test_dbn unit/test_dbn_decoder.cpp unit/test_tcp_stream.cpp unit/test_dbn_pipeline.cpp)
# FlatBuffers wire reader, routing decoder, pipeline (fixture: real flatc output in tests/fixtures/flatbuffers)
flashtick_unit_test(test_flatbuffers unit/test_flatbuffers_wire.cpp unit/test_flatbuffers_decoder.cpp unit/test_flatbuffers_pipeline.cpp)
