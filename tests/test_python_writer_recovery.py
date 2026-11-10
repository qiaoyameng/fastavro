from datetime import date
from io import BytesIO

import pytest

import fastavro
from fastavro._write_py import LOGICAL_WRITERS, Writer


schema = {
    "type": "record",
    "name": "writer_recovery",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "number", "type": "int"},
        {
            "name": "date",
            "type": {"type": "int", "logicalType": "date"},
            "default": 0,
        },
    ],
}


first_record = {"name": "first", "number": 1, "date": date(1970, 1, 2)}
second_record = {"name": "second", "number": 2, "date": date(1970, 1, 3)}


def read_records(file_obj):
    file_obj.seek(0)
    return list(fastavro.reader(file_obj))


@pytest.mark.parametrize("codec", ["null", "deflate"])
@pytest.mark.parametrize("sync_interval", [0, 1000 * 16])
def test_bad_record_is_rolled_back(codec, sync_interval):
    file_obj = BytesIO()
    writer = Writer(file_obj, schema, codec=codec, sync_interval=sync_interval)

    writer.write(first_record)
    with pytest.raises(TypeError, match="on field number"):
        writer.write({"name": "bad", "number": "not-an-int", "date": 0})

    assert writer.block_count == (0 if sync_interval == 0 else 1)
    writer.write(second_record)
    writer.flush()

    assert read_records(file_obj) == [first_record, second_record]


@pytest.mark.parametrize("codec", ["null", "deflate"])
def test_logical_writer_error_is_rolled_back(codec, monkeypatch):
    original_logical_writer = LOGICAL_WRITERS["int-date"]

    def broken_logical_writer(datum, logical_schema):
        monkeypatch.setitem(LOGICAL_WRITERS, "int-date", original_logical_writer)
        raise RuntimeError("logical writer failed")

    file_obj = BytesIO()
    writer = Writer(file_obj, schema, codec=codec)
    writer.write(first_record)

    monkeypatch.setitem(LOGICAL_WRITERS, "int-date", broken_logical_writer)

    with pytest.raises(RuntimeError, match="logical writer failed"):
        writer.write({"name": "bad", "number": 0, "date": 0})

    assert writer.block_count == 1
    writer.write(second_record)
    writer.flush()

    assert read_records(file_obj) == [first_record, second_record]


@pytest.mark.parametrize("codec", ["null", "deflate"])
@pytest.mark.parametrize("strict_option", ["strict", "strict_allow_default"])
def test_strict_validation_error_is_rolled_back(codec, strict_option):
    file_obj = BytesIO()
    writer = Writer(file_obj, schema, codec=codec, options={strict_option: True})
    writer.write(first_record)

    with pytest.raises(ValueError, match="record contains more fields"):
        writer.write(
            {
                "name": "bad",
                "number": 0,
                "date": 0,
                "unexpected": True,
            }
        )

    assert writer.block_count == 1
    writer.write(second_record)
    writer.flush()

    assert read_records(file_obj) == [first_record, second_record]


@pytest.mark.parametrize("codec", ["null", "deflate"])
def test_bad_record_recovery_before_write_block(codec):
    source = BytesIO()
    fastavro.writer(source, schema, [second_record], codec=codec)
    source.seek(0)
    block = next(fastavro.block_reader(source, schema))

    file_obj = BytesIO()
    writer = Writer(file_obj, schema, codec=codec)
    writer.write(first_record)

    with pytest.raises(TypeError, match="on field number"):
        writer.write({"name": "bad", "number": "not-an-int", "date": 0})

    writer.write_block(block)
    writer.flush()

    assert read_records(file_obj) == [first_record, second_record]


class FailingString:
    def encode(self, encoding="utf-8"):
        raise OSError("memory encoding failed")


@pytest.mark.parametrize("codec", ["null", "deflate"])
def test_memory_encoding_os_error_is_rolled_back(codec):
    file_obj = BytesIO()
    writer = Writer(file_obj, schema, codec=codec)
    writer.write(first_record)

    with pytest.raises(OSError, match="memory encoding failed"):
        writer.write({"name": FailingString(), "number": 0, "date": 0})

    assert writer._failed is False
    assert writer.block_count == 1
    writer.write(second_record)
    writer.flush()

    assert read_records(file_obj) == [first_record, second_record]


class PartialWriteIO:
    def __init__(self):
        self.buffer = BytesIO()
        self.write_calls = 0
        self.header_write_calls = None

    def write(self, data):
        if self.header_write_calls is not None:
            if self.write_calls > self.header_write_calls:
                length = len(data)
                partial_length = max(1, length // 2)
                self.buffer.write(data[:partial_length])
                raise OSError("simulated partial write")
        result = self.buffer.write(data)
        self.write_calls += 1
        if len(data) == 16:
            self.header_write_calls = self.write_calls
        return result

    def flush(self):
        pass

    def seekable(self):
        return True

    def seek(self, *args):
        return self.buffer.seek(*args)

    def tell(self):
        return self.buffer.tell()

    def readable(self):
        return False


@pytest.mark.parametrize("codec", ["null", "deflate"])
def test_partial_io_write_marks_writer_failed(codec):
    file_obj = PartialWriteIO()
    writer = Writer(file_obj, schema, codec=codec, sync_interval=0)

    with pytest.raises(OSError, match="simulated partial write"):
        writer.write(first_record)

    assert writer._failed is True
    with pytest.raises(RuntimeError, match="failed state"):
        writer.write(second_record)
    with pytest.raises(RuntimeError, match="failed state"):
        writer.flush()


@pytest.mark.parametrize("codec", ["null", "deflate"])
def test_partial_io_flush_marks_writer_failed(codec):
    file_obj = PartialWriteIO()
    writer = Writer(file_obj, schema, codec=codec)
    writer.write(first_record)

    with pytest.raises(OSError, match="simulated partial write"):
        writer.flush()

    assert writer._failed is True
    with pytest.raises(RuntimeError, match="failed state"):
        writer.write(second_record)
    with pytest.raises(RuntimeError, match="failed state"):
        writer.flush()


@pytest.mark.parametrize("codec", ["null", "deflate"])
def test_partial_io_write_block_marks_writer_failed(codec):
    source = BytesIO()
    fastavro.writer(source, schema, [first_record], codec=codec)
    source.seek(0)
    block = next(fastavro.block_reader(source, schema))

    file_obj = PartialWriteIO()
    writer = Writer(file_obj, schema, codec=codec)

    with pytest.raises(OSError, match="simulated partial write"):
        writer.write_block(block)

    assert writer._failed is True
    with pytest.raises(RuntimeError, match="failed state"):
        writer.write(second_record)
    with pytest.raises(RuntimeError, match="failed state"):
        writer.flush()
