"""Lakehouse loader: lands source data and loads it into Iceberg tables through Polaris."""
import resource

# No core dumps from any process that loads this package: a dump is a copy of process memory, which
# holds plaintext identifiers while a batch is encrypted. One crashed test run left a
# 2.4 GB dump in the working tree. Lowering the limit needs no privileges and works everywhere.
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
