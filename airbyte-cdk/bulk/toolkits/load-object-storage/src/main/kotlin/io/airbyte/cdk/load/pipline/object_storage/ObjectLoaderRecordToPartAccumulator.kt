/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.pipline.object_storage

import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.file.object_storage.BufferedFormattingWriter
import io.airbyte.cdk.load.file.object_storage.BufferedFormattingWriterFactory
import io.airbyte.cdk.load.file.object_storage.ObjectStorageClient
import io.airbyte.cdk.load.file.object_storage.Part
import io.airbyte.cdk.load.file.object_storage.PartFactory
import io.airbyte.cdk.load.file.object_storage.PathFactory
import io.airbyte.cdk.load.message.DestinationRecordAirbyteValue
import io.airbyte.cdk.load.message.StreamKey
import io.airbyte.cdk.load.pipeline.BatchAccumulator
import io.airbyte.cdk.load.state.DestinationStateManager
import io.airbyte.cdk.load.state.object_storage.ObjectStorageDestinationState
import io.airbyte.cdk.load.write.object_storage.ObjectLoader
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import java.io.OutputStream

@Singleton
@Requires(bean = ObjectLoader::class)
class ObjectLoaderRecordToPartAccumulator<T : OutputStream>(
    private val pathFactory: PathFactory,
    private val catalog: DestinationCatalog,
    private val writerFactory: BufferedFormattingWriterFactory<T>,
    private val client: ObjectStorageClient<*>,
    private val loader: ObjectLoader,
    // TODO: This doesn't need to be "DestinationState", just a couple of utility classes
    val stateMananger: DestinationStateManager<ObjectStorageDestinationState>,
    @Value("\${airbyte.destination.core.record-batch-size-override:null}")
    val batchSizeOverride: Long? = null,
) :
    BatchAccumulator<
        ObjectLoaderRecordToPartAccumulator.State<T>, StreamKey, DestinationRecordAirbyteValue, Part
    > {
    private val log = KotlinLogging.logger {}

    private val objectSizeBytes = loader.objectSizeBytes
    private val partSizeBytes = loader.partSizeBytes

    data class State<T : OutputStream>(
        val stream: DestinationStream,
        val writer: BufferedFormattingWriter<T>,
        val partFactory: PartFactory
    ) : AutoCloseable {
        override fun close() {
            writer.close()
        }
    }

    private suspend fun newState(stream: DestinationStream): State<T> {
        // Determine unique file name.
        val pathOnly = pathFactory.getFinalDirectory(stream)
        val state = stateMananger.getState(stream)
        val fileNo = state.getPartIdCounter(pathOnly).incrementAndGet()
        val fileName = state.ensureUnique(pathFactory.getPathToFile(stream, fileNo))

        // Initialize the part factory and writer.
        val partFactory = PartFactory(fileName, fileNo)
        log.info { "Starting part generation for $fileName (${stream.descriptor})" }
        return State(stream, writerFactory.create(stream), partFactory)
    }

    private fun makePart(state: State<T>, forceFinish: Boolean = false): Part {
        state.writer.flush()
        val newSize = state.partFactory.totalSize + state.writer.bufferSize
        val isFinal =
            forceFinish ||
                newSize >= objectSizeBytes ||
                batchSizeOverride != null // HACK: This is a hack to force a flush
        val bytes =
            if (isFinal) {
                state.writer.finish()
            } else {
                state.writer.takeBytes()
            }
        val part = state.partFactory.nextPart(bytes, isFinal)
        log.info { "Creating part $part" }
        return part
    }

    override suspend fun start(key: StreamKey, part: Int): State<T> {
        val stream = catalog.getStream(key.stream)
        return newState(stream)
    }

    override suspend fun accept(
        input: DestinationRecordAirbyteValue,
        state: State<T>
    ): Pair<State<T>?, Part?> {
        state.writer.accept(input)
        if (state.writer.bufferSize >= partSizeBytes || batchSizeOverride != null) {
            val part = makePart(state)
            val nextState =
                if (part.isFinal) {
                    null
                } else {
                    state
                }
            return Pair(nextState, part)
        }
        return Pair(state, null)
    }

    override suspend fun finish(state: State<T>): Part {
        return makePart(state, true)
    }
}
