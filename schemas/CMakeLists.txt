# Typed views generated from the shipped schemas at build time by ft_schema.
# ft_generate_views(<xml> <namespace>) defines the outputs under ${FT_GENERATED_DIR};
# consumers add ${FT_GENERATED_DIR} to their include path and list the header as a source.
set(FT_GENERATED_DIR ${CMAKE_BINARY_DIR}/generated CACHE INTERNAL "generated typed views")
function(ft_generate_views xml ns)
    get_filename_component(base ${xml} NAME_WE)
    set(outs ${FT_GENERATED_DIR}/${base}_views.h ${FT_GENERATED_DIR}/${base}_views.py ${FT_GENERATED_DIR}/${base}_views.q)
    add_custom_command(
        OUTPUT ${outs}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${FT_GENERATED_DIR}
        COMMAND ft_schema --schema ${xml} --views all --namespace ${ns} --out ${FT_GENERATED_DIR}
        DEPENDS ft_schema ${xml}
        COMMENT "ft_schema: typed views for ${xml}")
    add_custom_target(views_${base} DEPENDS ${outs})
endfunction()
ft_generate_views(${CMAKE_SOURCE_DIR}/tests/data/sample_md.xml ft_views)
ft_generate_views(${CMAKE_SOURCE_DIR}/schemas/flashtick_canonical.xml ft_canonical)
