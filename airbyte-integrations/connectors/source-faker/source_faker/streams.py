#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import datetime
import logging
import os
from multiprocessing import Pool
from collections.abc import Iterable, Mapping, MutableMapping
from typing import Any

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStream,
    Type,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException, FailureType
from airbyte_cdk.sources.streams import IncrementalMixin, Stream
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer

from .airbyte_message_with_cached_json import AirbyteMessageWithCachedJSON
from .utils import now_millis

from .purchase_generator import PurchaseGenerator
from .user_generator import UserGenerator
from .utils import format_airbyte_time, generate_estimate, now_millis, read_json


class Products(Stream, IncrementalMixin):
    primary_key = "id"
    cursor_field = "updated_at"

    def __init__(self, count: int, seed: int, parallelism: int, records_per_slice: int, always_updated: bool, **kwargs):
        super().__init__(**kwargs)
        self.count = count
        self.seed = seed
        self.records_per_slice = records_per_slice
        self.always_updated = always_updated
        self._state: MutableMapping[str, Any] = {}

    @property
    def state_checkpoint_interval(self) -> int | None:
        return self.records_per_slice

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._state = dict(value)

    def load_products(self) -> list[dict]:
        dirname = os.path.dirname(os.path.realpath(__file__))
        return read_json(os.path.join(dirname, "record_data", "products.json"))

    def read_records(
        self,
        sync_mode: str,
        cursor_field: list[str] | None = None,
        stream_slice: Mapping[str, Any] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[StreamData]:
        if stream_state and "updated_at" in stream_state and not self.always_updated:
            return

        products = self.load_products()
        updated_at = ""

        median_record_byte_size = 180
        rows_to_emit = len(products)
        trace_message = generate_estimate(self.name, rows_to_emit, median_record_byte_size)
        yield AirbyteMessage(type=Type.TRACE, trace=trace_message)

        for product in products:
            if product["id"] <= self.count:
                now = datetime.datetime.now()
                updated_at = format_airbyte_time(now)
                product["updated_at"] = updated_at
                record = AirbyteRecordMessage(
                    stream=self.name,
                    data=product,
                    emitted_at=now_millis()
                )
                yield AirbyteMessage(type=Type.RECORD, record=record)

                if self.state_checkpoint_interval and product["id"] % self.state_checkpoint_interval == 0:
                    yield AirbyteMessage(
                        type=Type.STATE,
                        state=AirbyteStateMessage(data={"seed": self.seed, "updated_at": updated_at})
                    )


class Users(Stream, IncrementalMixin):
    primary_key = "id"
    cursor_field = "updated_at"

    def __init__(self, count: int, seed: int, parallelism: int, records_per_slice: int, always_updated: bool, **kwargs):
        super().__init__(**kwargs)
        self.count = count
        self.seed = seed
        self.records_per_slice = records_per_slice
        self.parallelism = parallelism
        self.always_updated = always_updated
        self.generator = UserGenerator(self.name, self.seed)
        self._state: MutableMapping[str, Any] = {}

    @property
    def state_checkpoint_interval(self) -> int | None:
        return self.records_per_slice

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._state = dict(value)

    def read_records(
        self,
        sync_mode: str,
        cursor_field: list[str] | None = None,
        stream_slice: Mapping[str, Any] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[StreamData]:
        """
        This is a multi-process implementation of read_records.
        We make N workers (where N is the number of available CPUs) and spread out the CPU-bound work of generating records and serializing them to JSON
        """

        if stream_state and "updated_at" in stream_state and not self.always_updated:
            return

        updated_at = ""

        median_record_byte_size = 450
        trace_message = generate_estimate(self.name, self.count, median_record_byte_size)
        yield AirbyteMessage(type=Type.TRACE, trace=trace_message)

        loop_offset = 0
        with Pool(initializer=self.generator.prepare, processes=self.parallelism) as pool:
            while loop_offset < self.count:
                records_remaining_this_loop = min(self.records_per_slice, (self.count - loop_offset))
                if records_remaining_this_loop <= 0:
                    break

                try:
                    users = pool.map(self.generator.generate, range(loop_offset, loop_offset + records_remaining_this_loop))
                    for user in users:
                        if isinstance(user, AirbyteMessageWithCachedJSON) and user.type == Type.RECORD:
                            updated_at = user.record.data["updated_at"]
                            yield user
                            loop_offset += 1

                            if self.state_checkpoint_interval and loop_offset % self.state_checkpoint_interval == 0:
                                yield AirbyteMessage(
                                    type=Type.STATE,
                                    state=AirbyteStateMessage(data={"seed": self.seed, "updated_at": updated_at})
                                )
                except Exception as e:
                    error_msg = f"Error generating user records: {str(e)}"
                    raise AirbyteTracedException(
                        message=error_msg,
                        internal_message=error_msg,
                        failure_type=FailureType.system_error,
                        exception=e
                    ) from e

            if updated_at:
                yield AirbyteMessage(
                    type=Type.STATE,
                    state=AirbyteStateMessage(data={"seed": self.seed, "updated_at": updated_at})
                )


class Purchases(Stream, IncrementalMixin):
    primary_key = "id"
    cursor_field = "updated_at"

    def __init__(self, count: int, seed: int, parallelism: int, records_per_slice: int, always_updated: bool, **kwargs):
        super().__init__(**kwargs)
        self.count = count
        self.seed = seed
        self.records_per_slice = records_per_slice
        self.parallelism = parallelism
        self.always_updated = always_updated
        self.generator = PurchaseGenerator(self.name, self.seed)
        self._state: MutableMapping[str, Any] = {}

    @property
    def state_checkpoint_interval(self) -> int | None:
        return self.records_per_slice

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._state = dict(value)

    def read_records(
        self,
        sync_mode: str,
        cursor_field: list[str] | None = None,
        stream_slice: Mapping[str, Any] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[StreamData]:
        """
        This is a multi-process implementation of read_records.
        We make N workers (where N is the number of available CPUs) and spread out the CPU-bound work of generating records and serializing them to JSON
        """

        if stream_state and "updated_at" in stream_state and not self.always_updated:
            return

        updated_at = ""

        # a fuzzy guess, some users have purchases, some don't
        median_record_byte_size = 230
        trace_message = generate_estimate(self.name, int(self.count * 1.3), median_record_byte_size)
        yield AirbyteMessage(type=Type.TRACE, trace=trace_message)

        loop_offset = 0
        with Pool(initializer=self.generator.prepare, processes=self.parallelism) as pool:
            while loop_offset < self.count:
                records_remaining_this_loop = min(self.records_per_slice, (self.count - loop_offset))
                carts = pool.map(self.generator.generate, range(loop_offset, loop_offset + records_remaining_this_loop))
                for purchases in carts:
                    loop_offset += 1
                    for purchase in purchases:
                        if isinstance(purchase, AirbyteMessageWithCachedJSON) and purchase.type == Type.RECORD:
                            updated_at = purchase.record.data["updated_at"]
                            yield purchase

                    if updated_at and self.state_checkpoint_interval and loop_offset % self.state_checkpoint_interval == 0:
                        state_data = {"seed": self.seed, "updated_at": updated_at}
                        if loop_offset:
                            state_data["loop_offset"] = loop_offset
                        yield AirbyteMessage(
                            type=Type.STATE,
                            state=AirbyteStateMessage(data=state_data)
                        )

                if records_remaining_this_loop == 0:
                    break

            if updated_at:
                state_data = {"seed": self.seed, "updated_at": updated_at}
                if loop_offset:
                    state_data["loop_offset"] = loop_offset
                yield AirbyteMessage(
                    type=Type.STATE,
                    state=AirbyteStateMessage(data=state_data)
                )
