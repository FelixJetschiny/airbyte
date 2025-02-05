/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.mssql.v2

import io.airbyte.cdk.command.ConfigurationSpecification
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.write.BasicPerformanceTest
import io.airbyte.cdk.load.write.DataValidator
import io.airbyte.integrations.destination.mssql.v2.config.DataSourceFactory
import io.airbyte.integrations.destination.mssql.v2.config.MSSQLConfiguration
import io.airbyte.integrations.destination.mssql.v2.config.MSSQLConfigurationFactory
import io.airbyte.integrations.destination.mssql.v2.config.MSSQLSpecification
import java.nio.file.Files
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

class MSSQLDataValidator : DataValidator {
    override fun count(spec: ConfigurationSpecification, stream: DestinationStream): Long? {
        val config = getConfiguration(spec = spec as MSSQLSpecification, stream = stream)
        val sqlBuilder = MSSQLQueryBuilder(config, stream)
        val dataSource = DataSourceFactory().dataSource(config)

        return dataSource.connection.use { connection ->
            COUNT_FROM.toQuery(sqlBuilder.outputSchema, sqlBuilder.tableName).executeQuery(
                connection
            ) { rs ->
                while (rs.next()) {
                    return@executeQuery rs.getLong(1)
                }
                return@executeQuery null
            }
        }
    }

    private fun getConfiguration(
        spec: ConfigurationSpecification,
        stream: DestinationStream
    ): MSSQLConfiguration {
        /*
         * Replace the host, port and schema to match what is exposed
         * by the container and generated by the test suite in the case of the schema name
         */
        val configOverrides =
            mutableMapOf("host" to MSSQLContainerHelper.getHost()).apply {
                MSSQLContainerHelper.getPort()?.let { port -> put("port", port.toString()) }
                stream.descriptor.namespace?.let { schema -> put("schema", schema) }
            }
        return MSSQLConfigurationFactory()
            .makeWithOverrides(spec = spec as MSSQLSpecification, overrides = configOverrides)
    }
}

class MSSQLPerformanceTest :
    BasicPerformanceTest(
        configContents = Files.readString(MSSQLTestConfigUtil.getConfigPath("check/valid.json")),
        configSpecClass = MSSQLSpecification::class.java,
        configUpdater = MSSQLConfigUpdater(),
        dataValidator = MSSQLDataValidator(),
        defaultRecordsToInsert = 10000,
    ) {
    @Test
    override fun testInsertRecords() {
        testInsertRecords(recordsToInsert = 100000) {}
    }
    @Test
    override fun testInsertRecordsWithDedup() {
        testInsertRecordsWithDedup { perfSummary ->
            perfSummary.map { streamSummary ->
                assertEquals(streamSummary.expectedRecordCount, streamSummary.recordCount)
            }
        }
    }

    companion object {
        @JvmStatic
        @BeforeAll
        fun beforeAll() {
            MSSQLContainerHelper.start()
        }
    }
}
