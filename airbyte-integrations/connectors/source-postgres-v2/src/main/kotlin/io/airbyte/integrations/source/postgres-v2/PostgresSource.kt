/* Copyright (c) 2024 Airbyte, Inc., all rights reserved. */
package io.airbyte.integrations.source.postgres-v2

import io.airbyte.cdk.AirbyteSourceRunner

object PostgresSource {
    @JvmStatic
    fun main(args: Array<String>) {
        AirbyteSourceRunner.run(*args)
    }
}
