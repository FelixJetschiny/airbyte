/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.mssql.v2.config

import io.airbyte.cdk.load.command.azureBlobStorage.AzureBlobStorageConfiguration
import io.airbyte.cdk.load.command.azureBlobStorage.AzureBlobStorageConfigurationProvider
import io.airbyte.cdk.load.command.object_storage.MSSQLCSVFormatConfiguration
import io.airbyte.cdk.load.command.object_storage.ObjectStorageCompressionConfiguration
import io.airbyte.cdk.load.command.object_storage.ObjectStorageCompressionConfigurationProvider
import io.airbyte.cdk.load.command.object_storage.ObjectStorageFormatConfiguration
import io.airbyte.cdk.load.command.object_storage.ObjectStorageFormatConfigurationProvider
import io.airbyte.cdk.load.command.object_storage.ObjectStoragePathConfiguration
import io.airbyte.cdk.load.command.object_storage.ObjectStoragePathConfigurationProvider
import io.airbyte.cdk.load.file.NoopProcessor
import io.micronaut.context.annotation.Requires
import io.micronaut.context.condition.Condition
import io.micronaut.context.condition.ConditionContext
import jakarta.inject.Singleton
import java.io.ByteArrayOutputStream

class MSSQLIsConfiguredForBulkLoad : Condition {
    override fun matches(context: ConditionContext<*>): Boolean {
        val config = context.beanContext.getBean(MSSQLConfiguration::class.java)
        return config.mssqlLoadTypeConfiguration.loadTypeConfiguration is BulkLoadConfiguration
    }
}

@Singleton
@Requires(condition = MSSQLIsConfiguredForBulkLoad::class)
class MSSQLBulkLoadConfiguration(
    private val config: MSSQLConfiguration,
) :
    ObjectStoragePathConfigurationProvider,
    ObjectStorageFormatConfigurationProvider,
    ObjectStorageCompressionConfigurationProvider<ByteArrayOutputStream>,
    AzureBlobStorageConfigurationProvider {

    // Cast is guaranteed to succeed by the `Requires` guard.
    private val bulkLoadConfig =
        config.mssqlLoadTypeConfiguration.loadTypeConfiguration as BulkLoadConfiguration

    val dataSource: String = bulkLoadConfig.bulkLoadDataSource
    override val objectStoragePathConfiguration =
        ObjectStoragePathConfiguration(
            prefix = "blob",
            pathPattern = "\${NAMESPACE}/\${STREAM_NAME}/\${YEAR}/\${MONTH}/\${DAY}/\${EPOCH}/",
            fileNamePattern = "{part_number}{format_extension}",
        )
    override val objectStorageFormatConfiguration: ObjectStorageFormatConfiguration =
        MSSQLCSVFormatConfiguration(
            validateValuesPreLoad = bulkLoadConfig.validateValuesPreLoad == true
        )
    override val objectStorageCompressionConfiguration:
        ObjectStorageCompressionConfiguration<ByteArrayOutputStream> =
        ObjectStorageCompressionConfiguration(NoopProcessor)
    override val azureBlobStorageConfiguration: AzureBlobStorageConfiguration =
        AzureBlobStorageConfiguration(
            accountName = bulkLoadConfig.accountName,
            containerName = bulkLoadConfig.containerName,
            sharedAccessSignature = bulkLoadConfig.sharedAccessSignature,
        )
}
