# Schemas

SBE XML schemas referenced by `partitions[].decoder.schema` in `config.yaml`.
Paths are resolved relative to the config file's directory.

`sample_md.xml` is the synthetic schema used by the tests and the `ft_sbe_sender`
tool. Replace it with the publisher's schema for a real bus; the only hard
requirement on a publisher is that this file exists and is current.
