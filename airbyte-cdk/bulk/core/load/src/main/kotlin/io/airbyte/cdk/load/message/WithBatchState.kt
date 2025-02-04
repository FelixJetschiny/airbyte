/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.message

/**
 * Used internally by the CDK to implement RecordLoaders. It is added to outputs of batch
 * accumulators that can ack or complete record batches. This is done *when stitching the dev
 * interface to the pipeline, so the dev does not have to think about internal state.
 */
interface WithBatchState {
    val state: Batch.State
}
