#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import jsonschema
import pytest
from source_faker import SourceFaker

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStream,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    StreamDescriptor,
    Type,
)
from airbyte_cdk.models.airbyte_protocol_serializers import AirbyteMessageSerializer


class MockLogger:
    def debug(a, b, **kwargs):
        return None

    def info(a, b, **kwargs):
        return None

    def exception(a, b, **kwargs):
        print(b)
        return None

    def isEnabledFor(a, b, **kwargs):
        return False


logger = MockLogger()
serializer = AirbyteMessageSerializer


def schemas_are_valid():
    config = {"count": 1, "parallelism": 1}
    source = _create_source(catalog=None, config=config, state=None)
    catalog = source.discover(None, config)
    catalog = serializer.dump(AirbyteMessage(type=Type.CATALOG, catalog=catalog))
    schemas = [stream["json_schema"] for stream in catalog["catalog"]["streams"]]

    for schema in schemas:
        jsonschema.Draft7Validator.check_schema(schema)


def test_source_streams():
    config = {"count": 1, "parallelism": 1}
    source = _create_source(catalog=None, config=config, state=None)
    catalog = source.discover(None, config)
    catalog = serializer.dump(AirbyteMessage(type=Type.CATALOG, catalog=catalog))
    schemas = [stream["json_schema"] for stream in catalog["catalog"]["streams"]]

    assert len(schemas) == 3
    assert schemas[1]["properties"] == {
        "id": {"type": "integer"},
        "created_at": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"},
        "updated_at": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"},
        "name": {"type": "string"},
        "title": {"type": "string"},
        "age": {"type": "integer"},
        "email": {"type": "string"},
        "telephone": {"type": "string"},
        "gender": {"type": "string"},
        "language": {"type": "string"},
        "academic_degree": {"type": "string"},
        "nationality": {"type": "string"},
        "occupation": {"type": "string"},
        "height": {"type": "string"},
        "blood_type": {"type": "string"},
        "weight": {"type": "integer"},
        "address": {
            "type": "object",
            "properties": {
                "city": {"type": "string"},
                "country_code": {"type": "string"},
                "postal_code": {"type": "string"},
                "province": {"type": "string"},
                "state": {"type": "string"},
                "street_name": {"type": "string"},
                "street_number": {"type": "string"},
            },
        },
    }


def test_read_small_random_data():
    config = {"count": 10, "parallelism": 1}
    source = _create_source(catalog=None, config=config, state=None)
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(name="users", json_schema={}, supported_sync_modes=["incremental"]),
                sync_mode="incremental",
                destination_sync_mode="overwrite",
            )
        ]
    )
    state = {}
    iterator = source.read(logger, config, catalog, state)

    estimate_row_count = 0
    record_rows_count = 0
    state_rows_count = 0
    for row in iterator:
        if row.type is Type.TRACE:
            estimate_row_count = estimate_row_count + 1
        if row.type is Type.RECORD:
            record_rows_count = record_rows_count + 1
        if row.type is Type.STATE:
            state_rows_count = state_rows_count + 1

    assert estimate_row_count == 4
    assert record_rows_count == 10
    assert state_rows_count == 1


def test_read_always_updated():
    config = {"count": 10, "parallelism": 1, "always_updated": False}
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(name="users", json_schema={}, supported_sync_modes=["incremental"]),
                sync_mode="incremental",
                destination_sync_mode="overwrite",
            )
        ]
    )
    source = _create_source(catalog=catalog, config=config, state=None)
    state = None
    iterator = source.read(logger, config, catalog, state)

    record_rows_count = 0
    for row in iterator:
        if row.type is Type.RECORD:
            record_rows_count = record_rows_count + 1

    assert record_rows_count == 10

    state = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="users"),
                stream_state=AirbyteStateBlob({"updated_at": "something"}),
            ),
        )
    ]
    source = _create_source(catalog=catalog, config=config, state=state)
    iterator = source.read(logger, config, catalog, state)

    record_rows_count = 0
    for row in iterator:
        if row.type is Type.RECORD:
            record_rows_count = record_rows_count + 1

    assert record_rows_count == 0


def test_read_products():
    config = {"count": 999, "parallelism": 1}
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(name="products", json_schema={}, supported_sync_modes=["full_refresh"]),
                sync_mode="incremental",
                destination_sync_mode="overwrite",
            )
        ]
    )
    state = {}
    source = _create_source(catalog=catalog, config=config, state=state)
    iterator = source.read(logger, config, catalog, state)

    estimate_row_count = 0
    record_rows_count = 0
    state_rows_count = 0
    for row in iterator:
        if row.type is Type.TRACE:
            estimate_row_count = estimate_row_count + 1
        if row.type is Type.RECORD:
            record_rows_count = record_rows_count + 1
        if row.type is Type.STATE:
            state_rows_count = state_rows_count + 1

    assert estimate_row_count == 4
    assert record_rows_count == 100  # only 100 products, no matter the count
    assert state_rows_count == 2


def test_read_big_random_data():
    config = {"count": 1000, "records_per_slice": 100, "parallelism": 1}
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(name="users", json_schema={}, supported_sync_modes=["incremental"]),
                sync_mode="incremental",
                destination_sync_mode="overwrite",
            ),
            ConfiguredAirbyteStream(
                stream=AirbyteStream(name="products", json_schema={}, supported_sync_modes=["full_refresh"]),
                sync_mode="incremental",
                destination_sync_mode="overwrite",
            ),
        ]
    )
    state = {}
    source = _create_source(catalog=catalog, config=config, state=state)
    iterator = source.read(logger, config, catalog, state)

    record_rows_count = 0
    state_rows_count = 0
    for row in iterator:
        if row.type is Type.RECORD:
            record_rows_count = record_rows_count + 1
        if row.type is Type.STATE:
            state_rows_count = state_rows_count + 1

    assert record_rows_count == 1000 + 100  # 1000 users, and 100 products
    assert state_rows_count == 10 + 1


def test_with_purchases():
    config = {"count": 1000, "parallelism": 1}
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(name="users", json_schema={}, supported_sync_modes=["incremental"]),
                sync_mode="incremental",
                destination_sync_mode="overwrite",
            ),
            ConfiguredAirbyteStream(
                stream=AirbyteStream(name="products", json_schema={}, supported_sync_modes=["full_refresh"]),
                sync_mode="incremental",
                destination_sync_mode="overwrite",
            ),
            ConfiguredAirbyteStream(
                stream=AirbyteStream(name="purchases", json_schema={}, supported_sync_modes=["incremental"]),
                sync_mode="incremental",
                destination_sync_mode="overwrite",
            ),
        ]
    )
    state = {}
    source = _create_source(catalog=catalog, config=config, state=state)
    iterator = source.read(logger, config, catalog, state)

    record_rows_count = 0
    state_rows_count = 0
    for row in iterator:
        if row.type is Type.RECORD:
            record_rows_count = record_rows_count + 1
        if row.type is Type.STATE:
            state_rows_count = state_rows_count + 1

    assert record_rows_count > 1000 + 100  # should be greater than 1000 users, and 100 products
    assert state_rows_count > 10 + 1  # should be greater than 1000/100, and one state for the products


def test_read_with_seed():
    """
    This test asserts that setting a seed always returns the same values
    """

    config = {"count": 1, "seed": 100, "parallelism": 1}
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(name="users", json_schema={}, supported_sync_modes=["incremental"]),
                sync_mode="incremental",
                destination_sync_mode="overwrite",
            )
        ]
    )
    state = {}
    source = _create_source(catalog=catalog, config=config, state=state)
    iterator = source.read(logger, config, catalog, state)

    records = [row for row in iterator if row.type is Type.RECORD]
    assert records[0].record.data["occupation"] == "Wheel Clamper"
    assert records[0].record.data["email"] == "see1889+1@yandex.com"


def _create_source(*, catalog, config, state):
    if state:
        print(f"creating source with state: {vars(state[0].stream.stream_state)}")
    source = SourceFaker(catalog=catalog, config=config, state=state)
    return source
