package io.airbyte.integrations.source.mssql

import io.airbyte.cdk.read.ConcurrencyResource
import io.airbyte.cdk.read.GlobalFeedBootstrap
import io.airbyte.cdk.read.cdc.*
import java.util.concurrent.atomic.AtomicReference

class MsSqlServerCdcPartitionCreator<T : Comparable<T>>(
    concurrencyResource: ConcurrencyResource,
    feedBootstrap: GlobalFeedBootstrap,
    creatorOps: CdcPartitionsCreatorDebeziumOperations<T>,
    readerOps: CdcPartitionReaderDebeziumOperations<T>,
    lowerBoundReference: AtomicReference<T>,
    upperBoundReference: AtomicReference<T>,
    resetReason: AtomicReference<String?>,
): CdcPartitionsCreator<T>(
    concurrencyResource,
    feedBootstrap,
    creatorOps,
    readerOps,
    lowerBoundReference,
    upperBoundReference,
    resetReason,
) {
    override fun createCdcPartitionReader(upperBound: T, debeziumProperties: Map<String, String>,
                                          startingOffset: DebeziumOffset,
                                          startingSchemaHistory: DebeziumSchemaHistory?,
                                          isInputStateSynthetic: Boolean) =
        MsSqlServerCdcPartitionReader(
            concurrencyResource,
            feedBootstrap.streamRecordConsumers(),
            readerOps,
            upperBound,
            debeziumProperties,
            startingOffset,
            startingSchemaHistory,
            isInputStateSynthetic
        )
}
