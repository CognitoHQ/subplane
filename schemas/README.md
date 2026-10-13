# Schemas

SBE XML schemas referenced by `partitions[].decoder.schema` in `config.yaml`.
Paths are resolved relative to the config file's directory.

`sample_md.xml` is the synthetic schema used by the tests and the `subplane send`
tool. Replace it with the publisher's schema for a real bus; the only hard
requirement on a publisher is that this file exists and is current.

`subplane_canonical.xml` is the canonical flat schema shipped with the agent (package
`subplane.canonical`, id 100): eight messages, no groups, no varData, every scalar
naturally aligned, a common 32-byte prefix, `int64` prices and quantities scaled by
10^-9. `CMakeLists.txt` here runs `subplane schema --views all` on both schemas at build time
into `build/generated/`; `test_canonical` and `examples/views_app` compile the result.
Change the canonical schema only with a version bump and in the same commit as the
docs table in `docs/context/architecture.md`.
