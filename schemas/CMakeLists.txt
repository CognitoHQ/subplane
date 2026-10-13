# Typed views generated from the shipped schemas at build time by `subplane schema`.
# sp_generate_views(<xml> <namespace>) defines the outputs under ${SP_GENERATED_DIR};
# consumers add ${SP_GENERATED_DIR} to their include path and list the header as a source.
set(SP_GENERATED_DIR ${CMAKE_BINARY_DIR}/generated CACHE INTERNAL "generated typed views")
function(sp_generate_views xml ns)
    get_filename_component(base ${xml} NAME_WE)
    set(outs ${SP_GENERATED_DIR}/${base}_views.h ${SP_GENERATED_DIR}/${base}_views.py ${SP_GENERATED_DIR}/${base}_views.q)
    add_custom_command(
        OUTPUT ${outs}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${SP_GENERATED_DIR}
        COMMAND subplane schema --schema ${xml} --views all --namespace ${ns} --out ${SP_GENERATED_DIR}
        DEPENDS subplane ${xml}
        COMMENT "subplane schema: typed views for ${xml}")
    add_custom_target(views_${base} DEPENDS ${outs})
endfunction()
sp_generate_views(${CMAKE_SOURCE_DIR}/tests/data/sample_md.xml sp_views)
sp_generate_views(${CMAKE_SOURCE_DIR}/schemas/subplane_canonical.xml sp_canonical)
