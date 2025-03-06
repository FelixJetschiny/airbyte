#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, List, Mapping, MutableMapping, Optional, Tuple

from airbyte_cdk import (
    AirbyteLogFormatter,
    ConcurrentSource,
    ConcurrentSourceAdapter,
    ConfiguredAirbyteCatalog,
    ConnectorStateManager,
    Cursor,
    FinalStateCursor,
    InMemoryMessageRepository,
    Level,
    MessageRepository,
    Record,
    StreamFacade,
)
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.concurrent.cursor import Cursor
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition

from .streams import Products, Purchases, Users


DEFAULT_COUNT = 1_000

logger = logging.getLogger("airbyte")


class SourceFaker(ConcurrentSourceAdapter):
    message_repository = InMemoryMessageRepository(Level(AirbyteLogFormatter.level_mapping[logger.level]))

    def __init__(self, catalog: Optional[ConfiguredAirbyteCatalog], config: Optional[Mapping[str, Any]], state: Optional[Any], **kwargs):
        concurrency_level = 1
        logger.info(f"Using concurrent cdk with concurrency level {concurrency_level}")
        concurrent_source = ConcurrentSource.create(concurrency_level, 1, logger, self._slice_logger, self.message_repository)
        super().__init__(concurrent_source)
        self.catalog = catalog

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        if type(config["count"]) == int or type(config["count"]) == float:
            return True, None
        else:
            return False, "Count option is missing"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        count: int = config["count"] if "count" in config else DEFAULT_COUNT
        seed: int | None = config["seed"] if "seed" in config else None
        records_per_slice: int = config["records_per_slice"] if "records_per_slice" in config else 100
        always_updated: bool = config["always_updated"] if "always_updated" in config else True
        parallelism: int = config["parallelism"] if "parallelism" in config else 4

        streams: List[Stream] = [
            self._wrap_for_concurrency(
                Products(self.message_repository, count, seed, parallelism, records_per_slice, always_updated), seed
            ),
            self._wrap_for_concurrency(Users(self.message_repository, count, seed, parallelism, records_per_slice, always_updated), seed),
            self._wrap_for_concurrency(
                Purchases(self.message_repository, count, seed, parallelism, records_per_slice, always_updated), seed
            ),
        ]

        return streams

    def _wrap_for_concurrency(self, stream: Stream, seed: int | None):
        message_repository = InMemoryMessageRepository(Level(AirbyteLogFormatter.level_mapping[logger.level]))
        cursor = FakerCursor(stream_name=stream.name, stream_namespace=stream.namespace, message_repository=message_repository, seed=seed)
        return StreamFacade.create_from_stream(stream, self, logger, {}, cursor)


class FakerCursor(Cursor):
    def __init__(self, stream_name: str, stream_namespace: Optional[str], message_repository: MessageRepository, seed: int | None) -> None:
        self._stream_name = stream_name
        self._stream_namespace = stream_namespace
        self._message_repository = message_repository
        # Normally the connector state manager operates at the source-level. However, we only need it to write the sentinel
        # state message rather than manage overall source state. This is also only temporary as we move to the resumable
        # full refresh world where every stream uses a FileBasedConcurrentCursor with incremental state.
        self._connector_state_manager = ConnectorStateManager()
        self._seed = seed
        self._at_least_one_state_emitted = False

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    def observe(self, record: Record) -> None:
        updated_at = record.data["updated_at"]
        loop_offset = record.data["loop_offset"]
        self._state = {"seed": self._seed, "updated_at": updated_at, "loop_offset": loop_offset}

    def close_partition(self, partition: Partition) -> None:
        self._at_least_one_state_emitted = True

    def ensure_at_least_one_state_emitted(self) -> None:
        if not self._at_least_one_state_emitted:
            self._connector_state_manager.update_state_for_stream(self._stream_name, self._stream_namespace, self.state)
            state_message = self._connector_state_manager.create_state_message(self._stream_name, self._stream_namespace)
            self._message_repository.emit_message(state_message)
