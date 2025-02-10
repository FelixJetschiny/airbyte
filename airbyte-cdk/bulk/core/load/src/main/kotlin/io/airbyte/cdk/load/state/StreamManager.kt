/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.state

import com.google.common.collect.Range
import com.google.common.collect.RangeSet
import com.google.common.collect.TreeRangeSet
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.message.Batch
import io.airbyte.cdk.load.message.BatchEnvelope
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CompletableDeferred

sealed interface StreamResult

data class StreamProcessingFailed(val streamException: Exception) : StreamResult

data object StreamProcessingSucceeded : StreamResult

/** Manages the state of a single stream. */
interface StreamManager {
    /**
     * Count incoming record and return the record's *index*. If [markEndOfStream] has been called,
     * this should throw an exception.
     */
    fun countRecordIn(): Long
    fun recordCount(): Long

    /**
     * Mark the end-of-stream, set the end of stream variant (complete or incomplete) and return the
     * record count. Expect this exactly once. Expect no further `countRecordIn`, and expect that
     * [markProcessingSucceeded] will always occur after this, while [markProcessingFailed] can
     * occur before or after.
     */
    fun markEndOfStream(receivedStreamCompleteMessage: Boolean): Long
    fun endOfStreamRead(): Boolean

    /** Whether we received a stream complete message for the managed stream. */
    fun isComplete(): Boolean

    /**
     * Mark a checkpoint in the stream and return the current index and the number of records since
     * the last one.
     *
     * NOTE: Single-writer. If in the future multiple threads set checkpoints, this method should be
     * synchronized.
     */
    fun markCheckpoint(): Pair<Long, Long>

    /** Record that the given batch's state has been reached for the associated range(s). */
    fun <B : Batch> updateBatchState(batch: BatchEnvelope<B>)

    /**
     * True if all are true:
     * * all records have been seen (ie, we've counted an end-of-stream)
     * * a [Batch.State.COMPLETE] batch range has been seen covering every record
     *
     * Does NOT require that the stream be closed.
     */
    fun isBatchProcessingComplete(): Boolean

    /**
     * True if all records in [0, index] have at least reached [Batch.State.PERSISTED]. This is
     * implicitly true if they have all reached [Batch.State.COMPLETE].
     */
    fun areRecordsPersistedUntil(index: Long): Boolean

    /**
     * Indicates destination processing of the stream succeeded, regardless of complete/incomplete
     * status. This should only be called after all records and end of stream messages have been
     * read.
     */
    fun markProcessingSucceeded()

    /**
     * Indicates destination processing of the stream failed. Returns false if task was already
     * complete
     */
    fun markProcessingFailed(causedBy: Exception): Boolean

    /** Suspend until the stream completes, returning the result. */
    suspend fun awaitStreamResult(): StreamResult

    /** True if the stream processing has not yet been marked as successful or failed. */
    fun isActive(): Boolean

    /**
     * If checkpointing by index is enabled, returns the next checkpoint index that will be used.
     */
    fun nextCheckpointIndex(): Long

    /** If checkpointing by index is enabled, add counts for the given index. */
    fun addCountsForIndex(state: Batch.State, index: Long, count: Long)
}

class DefaultStreamManager(val stream: DestinationStream, private val checkpointByIndex: Boolean) :
    StreamManager {
    private val streamResult = CompletableDeferred<StreamResult>()

    data class CachedRanges(val state: Batch.State, val ranges: RangeSet<Long>)
    private val cachedRangesById = ConcurrentHashMap<String, CachedRanges>()

    private val log = KotlinLogging.logger {}

    private val recordCount = AtomicLong(0)
    private val lastCheckpoint = AtomicLong(0L)

    private val markedEndOfStream = AtomicBoolean(false)
    private val receivedComplete = AtomicBoolean(false)

    private val rangesState: ConcurrentHashMap<Batch.State, RangeSet<Long>> = ConcurrentHashMap()

    init {
        Batch.State.entries.forEach { rangesState[it] = TreeRangeSet.create() }
    }

    override fun countRecordIn(): Long {
        if (markedEndOfStream.get()) {
            throw IllegalStateException("Stream is closed for reading")
        }

        return recordCount.getAndIncrement()
    }

    override fun recordCount(): Long {
        return recordCount.get()
    }

    override fun markEndOfStream(receivedStreamCompleteMessage: Boolean): Long {
        if (markedEndOfStream.getAndSet(true)) {
            throw IllegalStateException("Stream is closed for reading")
        }
        receivedComplete.getAndSet(receivedStreamCompleteMessage)

        return recordCount.get()
    }

    override fun endOfStreamRead(): Boolean {
        return markedEndOfStream.get()
    }

    override fun isComplete(): Boolean {
        return receivedComplete.get()
    }

    data class IndexCounts(val consumed: Long, val persistedCount: Long, val completeCount: Long)
    private val indexStates = ConcurrentHashMap<Long, IndexCounts>()

    override fun markCheckpoint(): Pair<Long, Long> {
        val index = recordCount.get()
        val lastCheckpoint = lastCheckpoint.getAndSet(index)

        if (checkpointByIndex) {
            val segmentSize = index - lastCheckpoint
            indexStates[nextCheckpointIndex()] = IndexCounts(segmentSize, 0, 0)
        }

        return Pair(index, index - lastCheckpoint)
    }

    override fun nextCheckpointIndex(): Long {
        return indexStates.keys.max() + 1L
    }

    override fun addCountsForIndex(state: Batch.State, index: Long, count: Long) {
        if (!checkpointByIndex) {
            throw IllegalStateException(
                "Legacy `addCountsForIndex` should not be called unless checkpointing by index"
            )
        }
        val states =
            indexStates[index]
                ?: throw IllegalStateException("Index $index has not been checkpointed")

        indexStates[index] =
            states.let { counts ->
                when (state) {
                    Batch.State.PERSISTED ->
                        counts.copy(persistedCount = counts.persistedCount + count)
                    Batch.State.COMPLETE ->
                        counts.copy(completeCount = counts.completeCount + count)
                    else -> counts
                }
            }
    }

    override fun <B : Batch> updateBatchState(batch: BatchEnvelope<B>) {
        if (checkpointByIndex) {
            throw IllegalStateException(
                "Legacy `updateBatchState` should not be called when checkpointing by index"
            )
        }
        rangesState[batch.batch.state]
            ?: throw IllegalArgumentException("Invalid batch state: ${batch.batch.state}")

        val stateRangesToAdd = mutableListOf(batch.batch.state to batch.ranges)

        // If the batch is part of a group, update all ranges associated with its groupId
        // to the most advanced state. Otherwise, just use the ranges provided.
        batch.batch.groupId?.let { groupId ->
                val cachedRangesMaybe = cachedRangesById[groupId]
                val cachedSet = cachedRangesMaybe?.ranges?.asRanges() ?: emptySet()
                val newRanges = TreeRangeSet.create(cachedSet + batch.ranges.asRanges()).merged()
                val newCachedRanges = CachedRanges(state = batch.batch.state, ranges = newRanges)
                cachedRangesById[groupId] = newCachedRanges
                if (cachedRangesMaybe != null && cachedRangesMaybe.state != batch.batch.state) {
                    stateRangesToAdd.add(batch.batch.state to newRanges)
                    stateRangesToAdd.add(cachedRangesMaybe.state to batch.ranges)
                }
                cachedRangesMaybe
            }

        stateRangesToAdd.forEach { (stateToSet, rangesToUpdate) ->
            when (stateToSet) {
                Batch.State.COMPLETE -> {
                    // A COMPLETED state implies PERSISTED, so also mark PERSISTED.
                    addAndMarge(Batch.State.PERSISTED, rangesToUpdate)
                    addAndMarge(Batch.State.COMPLETE, rangesToUpdate)
                }
                else -> {
                    // For all other states, just mark the state.
                    addAndMarge(stateToSet, rangesToUpdate)
                }
            }
        }

        log.info {
            "Added ${batch.batch.state}->${batch.ranges} (groupId=${batch.batch.groupId}) to ${stream.descriptor.namespace}.${stream.descriptor.name}=>${rangesState[batch.batch.state]}"
        }
    }

    private fun RangeSet<Long>.merged(): RangeSet<Long> {
        val newRanges = this.asRanges().toMutableSet()
        this.asRanges().forEach { oldRange ->
            newRanges
                .find { newRange ->
                    oldRange.upperEndpoint() + 1 == newRange.lowerEndpoint() ||
                        newRange.upperEndpoint() + 1 == oldRange.lowerEndpoint()
                }
                ?.let { newRange ->
                    newRanges.remove(oldRange)
                    newRanges.remove(newRange)
                    val lower = minOf(oldRange.lowerEndpoint(), newRange.lowerEndpoint())
                    val upper = maxOf(oldRange.upperEndpoint(), newRange.upperEndpoint())
                    newRanges.add(Range.closed(lower, upper))
                }
        }
        return TreeRangeSet.create(newRanges)
    }

    private fun addAndMarge(state: Batch.State, ranges: RangeSet<Long>) {
        rangesState[state] =
            (rangesState[state]?.let {
                    it.addAll(ranges)
                    it
                }
                    ?: ranges)
                .merged()
    }

    /** True if all records in `[0, index)` have reached the given state. */
    private fun isProcessingCompleteForState(index: Long, state: Batch.State): Boolean {
        val completeRanges = rangesState[state]!!

        // Force the ranges to overlap at their endpoints, in order to work around
        // the behavior of `.encloses`, which otherwise would not consider adjacent ranges as
        // contiguous.
        // This ensures that a state message received at eg, index 10 (after messages 0..9 have
        // been received), will pass `{'[0..5]','[6..9]'}.encloses('[0..10)')`.
        val expanded =
            completeRanges.asRanges().map { it.span(Range.singleton(it.upperEndpoint() + 1)) }
        val expandedSet = TreeRangeSet.create(expanded)

        if (index == 0L && recordCount.get() == 0L) {
            return true
        }

        return expandedSet.encloses(Range.closedOpen(0L, index))
    }

    override fun isBatchProcessingComplete(): Boolean {
        /* If the stream hasn't been fully read, it can't be done. */
        if (!markedEndOfStream.get()) {
            return false
        }

        /* A closed empty stream is always complete. */
        if (recordCount.get() == 0L) {
            return true
        }

        if (checkpointByIndex) {
            val indexCounts = indexStates[recordCount.get()] ?: return false
            return indexCounts.persistedCount == indexCounts.completeCount
        }

        return isProcessingCompleteForState(recordCount.get(), Batch.State.COMPLETE)
    }

    override fun areRecordsPersistedUntil(index: Long): Boolean {
        if (checkpointByIndex) {
            val indexCounts = indexStates[index] ?: return false
            return indexCounts.persistedCount == indexCounts.consumed
        }

        return isProcessingCompleteForState(index, Batch.State.PERSISTED)
    }

    override fun markProcessingSucceeded() {
        if (!markedEndOfStream.get()) {
            throw IllegalStateException("Stream is not closed for reading")
        }
        streamResult.complete(StreamProcessingSucceeded)
    }

    override fun markProcessingFailed(causedBy: Exception): Boolean {
        return streamResult.complete(StreamProcessingFailed(causedBy))
    }

    override suspend fun awaitStreamResult(): StreamResult {
        return streamResult.await()
    }

    override fun isActive(): Boolean {
        return streamResult.isActive
    }
}

interface StreamManagerFactory {
    fun create(stream: DestinationStream): StreamManager
}

@Singleton
@Secondary
class DefaultStreamManagerFactory(
    @Value("\${airbyte.destination.core.checkpoint-by-index") val checkpointByIndex: Boolean
) : StreamManagerFactory {
    override fun create(stream: DestinationStream): StreamManager {
        return DefaultStreamManager(stream, checkpointByIndex)
    }
}
