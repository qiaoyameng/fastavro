fastavro.ocf
============

Merging Avro Object Container Files without re-compressing their blocks.

A record-by-record ``reader`` -> ``writer`` copy has to compress every record
again. The :func:`~fastavro.ocf.merge` function instead validates only the
container framing and copies each block's record count together with the
already compressed payload byte for byte. Record values inside the blocks are
never decoded or validated.

Only the ``null`` and ``deflate`` codecs are supported and every input
container must use the exact same complete writer schema. The schema
comparison uses fully parsed schemas rather than short fingerprints, so
different field defaults or logical types are rejected.

.. autofunction:: fastavro.ocf.merge

.. autofunction:: fastavro.ocf.iter_raw_blocks

.. autoexception:: fastavro.ocf.OCFMergeError
