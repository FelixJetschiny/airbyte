/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.pipeline.object_storage

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.file.object_storage.Part
import io.airbyte.cdk.load.message.StreamKey
import io.airbyte.cdk.load.pipline.object_storage.ObjectKey
import io.airbyte.cdk.load.pipline.object_storage.ObjectLoaderPartPartitioner
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test

class ObjectLoaderPartPartitionerTest {
    @Test
    fun `partitioner distributes keys round-robin`() {
        val keys = (0 until 12).map(Int::toString)
        val partitioner = ObjectLoaderPartPartitioner()
        val stream = DestinationStream.Descriptor("test", "stream")
        val numParts = 3
        var lastPartition: Int? = null
        (0 until 12).forEach {
            val partition = partitioner.getPart(ObjectKey(stream, it.toString()), numParts)
            lastPartition?.let { last -> assertEquals((last + 1) % numParts, partition) }
            lastPartition = partition
        }
    }

    @Test
    fun `partitioner uses object key as accumulator key`() {
        val keys = listOf("foo", "bar", "baz")
        val parts =
            keys.mapIndexed { index, key ->
                Part(
                    key = key,
                    fileNumber = index * 10L,
                    partIndex = index,
                    bytes = null,
                    isFinal = false
                )
            }
        val partitioner = ObjectLoaderPartPartitioner()
        val stream = DestinationStream.Descriptor("test", "stream")
        val outputKeys = parts.map { part -> partitioner.getOutputKey(StreamKey(stream), part) }
        val expectedKeys = keys.map { key -> ObjectKey(stream, key) }
        assertEquals(expectedKeys, outputKeys)
    }
}
