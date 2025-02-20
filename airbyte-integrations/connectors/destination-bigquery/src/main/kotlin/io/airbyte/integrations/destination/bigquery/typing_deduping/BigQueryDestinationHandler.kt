/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.integrations.destination.bigquery.typing_deduping

import com.google.cloud.bigquery.*
import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.Streams
import io.airbyte.cdk.integrations.base.AirbyteExceptionHandler
import io.airbyte.cdk.integrations.base.JavaBaseConstants
import io.airbyte.cdk.integrations.util.ConnectorExceptionUtil
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.commons.json.Jsons
import io.airbyte.integrations.base.destination.operation.AbstractStreamOperation
import io.airbyte.integrations.base.destination.typing_deduping.*
import io.airbyte.integrations.base.destination.typing_deduping.CollectionUtils.containsAllIgnoreCase
import io.airbyte.integrations.base.destination.typing_deduping.CollectionUtils.containsIgnoreCase
import io.airbyte.integrations.base.destination.typing_deduping.CollectionUtils.matchingKey
import io.airbyte.integrations.destination.bigquery.BigQueryDestination
import io.airbyte.integrations.destination.bigquery.BigQueryUtils
import io.airbyte.integrations.destination.bigquery.migrators.BigQueryDestinationState
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.FileOutputStream
import java.io.PrintWriter
import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.function.Consumer
import java.util.function.Function
import java.util.function.Predicate
import java.util.stream.Collectors
import java.util.stream.Stream
import kotlin.math.min
import org.apache.commons.text.StringSubstitutor
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class BigQueryDestinationHandler(private val bq: BigQuery, private val datasetLocation: String) :
    DestinationHandler<BigQueryDestinationState> {
    fun findExistingTable(id: StreamId): Optional<TableDefinition> {
        val table = bq.getTable(id.finalNamespace, id.finalName)
        return Optional.ofNullable(table).map { obj: Table -> obj.getDefinition() }
    }

    fun isFinalTableEmpty(id: StreamId): Boolean {
        return BigInteger.ZERO == bq.getTable(TableId.of(id.finalNamespace, id.finalName)).numRows
    }

    @Throws(Exception::class)
    fun getInitialRawTableState(id: StreamId, suffix: String): InitialRawTableStatus {
        bq.getTable(TableId.of(id.rawNamespace, id.rawName + suffix))
            ?: // Table doesn't exist. There are no unprocessed records, and no timestamp.
        return InitialRawTableStatus(false, false, Optional.empty())

        val unloadedRecordTimestamp =
            bq.query(
                    QueryJobConfiguration.newBuilder(
                            StringSubstitutor(
                                    java.util.Map.of<String, String>(
                                        "raw_table",
                                        id.rawTableId(BigQuerySqlGenerator.Companion.QUOTE, suffix)
                                    )
                                )
                                .replace( // bigquery timestamps have microsecond precision
                                    """
            SELECT TIMESTAMP_SUB(MIN(_airbyte_extracted_at), INTERVAL 1 MICROSECOND)
            FROM ${'$'}{raw_table}
            WHERE _airbyte_loaded_at IS NULL

            """.trimIndent()
                                )
                        )
                        .build()
                )
                .iterateAll()
                .iterator()
                .next()
                .first()
        // If this value is null, then there are no records with null loaded_at.
        // If it's not null, then we can return immediately - we've found some unprocessed records
        // and their
        // timestamp.
        if (!unloadedRecordTimestamp.isNull) {
            return InitialRawTableStatus(
                true,
                true,
                Optional.of(unloadedRecordTimestamp.timestampInstant)
            )
        }

        val loadedRecordTimestamp =
            bq.query(
                    QueryJobConfiguration.newBuilder(
                            StringSubstitutor(
                                    java.util.Map.of<String, String>(
                                        "raw_table",
                                        id.rawTableId(BigQuerySqlGenerator.Companion.QUOTE, suffix)
                                    )
                                )
                                .replace(
                                    """
            SELECT MAX(_airbyte_extracted_at)
            FROM ${'$'}{raw_table}

            """.trimIndent()
                                )
                        )
                        .build()
                )
                .iterateAll()
                .iterator()
                .next()
                .first()
        // We know (from the previous query) that all records have been processed by T+D already.
        // So we just need to get the timestamp of the most recent record.
        return if (loadedRecordTimestamp.isNull) {
            // Null timestamp because the table is empty. T+D can process the entire raw table
            // during this sync.
            InitialRawTableStatus(true, false, Optional.empty())
        } else {
            // The raw table already has some records. T+D can skip all records with timestamp <=
            // this value.
            InitialRawTableStatus(true, false, Optional.of(loadedRecordTimestamp.timestampInstant))
        }
    }

    @Throws(InterruptedException::class)
    override fun execute(sql: Sql) {
        val transactions = sql.asSqlStrings("BEGIN TRANSACTION", "COMMIT TRANSACTION")
        if (transactions.isEmpty()) {
            return
        }
        val queryId = UUID.randomUUID()
        val statement = java.lang.String.join("\n", transactions)
        LOGGER.debug("Executing sql {}: {}", queryId, statement)

        /*
         * If you run a query like CREATE SCHEMA ... OPTIONS(location=foo); CREATE TABLE ...;, bigquery
         * doesn't do a good job of inferring the query location. Pass it in explicitly.
         */
        var job =
            bq.create(
                JobInfo.of(
                    JobId.newBuilder().setLocation(datasetLocation).build(),
                    QueryJobConfiguration.newBuilder(statement).build()
                )
            )
        AirbyteExceptionHandler.addStringForDeinterpolation(job.etag)
        // job.waitFor() gets stuck forever in some failure cases, so manually poll the job instead.
        while (JobStatus.State.DONE != job.status.state) {
            Thread.sleep(1000L)
            job = job.reload()
        }
        if (job.status.error != null) {
            throw BigQueryException(
                Streams.concat(Stream.of(job.status.error), job.status.executionErrors.stream())
                    .toList()
            )
        }

        val statistics = job.getStatistics<JobStatistics.QueryStatistics>()
        LOGGER.info(
            "Root-level job {} completed in {} ms; processed {} bytes; billed for {} bytes",
            queryId,
            statistics.endTime - statistics.startTime,
            statistics.totalBytesProcessed,
            statistics.totalBytesBilled
        )

        // SQL transactions can spawn child jobs, which are billed individually. Log their stats
        // too.
        if (statistics.numChildJobs != null) {
            // There isn't (afaict) anything resembling job.getChildJobs(), so we have to ask bq for
            // them
            bq.listJobs(BigQuery.JobListOption.parentJobId(job.jobId.job))
                .streamAll()
                .sorted(
                    Comparator.comparing { childJob: Job ->
                        childJob.getStatistics<JobStatistics>().endTime
                    }
                )
                .forEach { childJob: Job ->
                    val configuration = childJob.getConfiguration<JobConfiguration>()
                    if (configuration is QueryJobConfiguration) {
                        val childQueryStats =
                            childJob.getStatistics<JobStatistics.QueryStatistics>()
                        var truncatedQuery: String =
                            configuration.query
                                .replace("\n".toRegex(), " ")
                                .replace(" +".toRegex(), " ")
                                .substring(
                                    0,
                                    min(100.0, configuration.query.length.toDouble()).toInt()
                                )
                        if (truncatedQuery != configuration.query) {
                            truncatedQuery += "..."
                        }
                        LOGGER.info(
                            "Child sql {} completed in {} ms; processed {} bytes; billed for {} bytes",
                            truncatedQuery,
                            childQueryStats.endTime - childQueryStats.startTime,
                            childQueryStats.totalBytesProcessed,
                            childQueryStats.totalBytesBilled
                        )
                    } else {
                        // other job types are extract/copy/load
                        // we're probably not using them, but handle just in case?
                        val childJobStats = childJob.getStatistics<JobStatistics>()
                        LOGGER.info(
                            "Non-query child job ({}) completed in {} ms",
                            configuration.type,
                            childJobStats.endTime - childJobStats.startTime
                        )
                    }
                }
        }
    }

    @Throws(Exception::class)
    override fun gatherInitialState(
        streamConfigs: List<StreamConfig>
    ): List<DestinationInitialStatus<BigQueryDestinationState>> {
        val initialStates: MutableList<DestinationInitialStatus<BigQueryDestinationState>> =
            ArrayList()
        for (streamConfig in streamConfigs) {
            val id = streamConfig.id
            val finalTable = findExistingTable(id)
            val rawTableState = getInitialRawTableState(id, "")
            val tempRawTableState =
                getInitialRawTableState(id, AbstractStreamOperation.TMP_TABLE_SUFFIX)
            initialStates.add(
                DestinationInitialStatus(
                    streamConfig,
                    finalTable.isPresent,
                    rawTableState,
                    tempRawTableState,
                    finalTable.isPresent &&
                        !existingSchemaMatchesStreamConfig(streamConfig, finalTable.get()),
                    finalTable.isEmpty ||
                        isFinalTableEmpty(
                            id
                        ), // Return a default state blob since we don't actually track state.
                    BigQueryDestinationState(false),
                    // for now, just use 0. this means we will always use a temp final table.
                    // platform has a workaround for this, so it's OK.
                    // TODO only fetch this on truncate syncs
                    // TODO once we have destination state, use that instead of a query
                    finalTableGenerationId = 0,
                    // temp table is always empty until we commit, so always return null
                    finalTempTableGenerationId = null,
                )
            )
        }
        return initialStates
    }

    @Throws(Exception::class)
    override fun commitDestinationStates(
        destinationStates: Map<StreamId, BigQueryDestinationState>
    ) {
        // Intentionally do nothing. Bigquery doesn't actually support destination states.
    }

    @Throws(TableNotMigratedException::class)
    private fun existingSchemaMatchesStreamConfig(
        stream: StreamConfig,
        existingTable: TableDefinition
    ): Boolean {
        val alterTableReport = buildAlterTableReport(stream, existingTable)
        var tableClusteringMatches = false
        var tablePartitioningMatches = false
        if (existingTable is StandardTableDefinition) {
            tableClusteringMatches = clusteringMatches(stream, existingTable)
            tablePartitioningMatches = partitioningMatches(existingTable)
        }
        LOGGER.info(
            "Alter Table Report {} {} {}; Clustering {}; Partitioning {}",
            alterTableReport.columnsToAdd,
            alterTableReport.columnsToRemove,
            alterTableReport.columnsToChangeType,
            tableClusteringMatches,
            tablePartitioningMatches
        )

        return alterTableReport.isNoOp && tableClusteringMatches && tablePartitioningMatches
    }

    fun buildAlterTableReport(
        stream: StreamConfig,
        existingTable: TableDefinition
    ): AlterTableReport {
        val pks = getPks(stream)

        val streamSchema: Map<String, StandardSQLTypeName> =
            stream.columns.entries.associate {
                it.key.name to BigQuerySqlGenerator.toDialectType(it.value)
            }

        val existingSchema =
            existingTable.schema!!
                .fields
                .stream()
                .collect(
                    Collectors.toMap(
                        Function { field: Field -> field.name },
                        Function { field: Field -> field.type.standardType }
                    )
                )

        // Columns in the StreamConfig that don't exist in the TableDefinition
        val columnsToAdd =
            streamSchema.keys
                .stream()
                .filter { name: String -> !containsIgnoreCase(existingSchema.keys, name) }
                .collect(Collectors.toSet())

        // Columns in the current schema that are no longer in the StreamConfig
        val columnsToRemove =
            existingSchema.keys
                .stream()
                .filter { name: String ->
                    !containsIgnoreCase(streamSchema.keys, name) &&
                        !containsIgnoreCase(JavaBaseConstants.V2_FINAL_TABLE_METADATA_COLUMNS, name)
                }
                .collect(Collectors.toSet<String>())

        // Columns that are typed differently than the StreamConfig
        val columnsToChangeType =
            Stream.concat(
                    streamSchema.keys
                        .stream() // If it's not in the existing schema, it should already be in the
                        // columnsToAdd Set
                        .filter { name: String ->
                            matchingKey(
                                    existingSchema.keys,
                                    name
                                ) // if it does exist, only include it in this set if the type (the
                                // value in each respective map)
                                // is different between the stream and existing schemas
                                .map { key: String ->
                                    existingSchema[key] != streamSchema[name]
                                } // if there is no matching key, then don't include it because it
                                // is probably already in columnsToAdd
                                .orElse(false)
                        }, // OR columns that used to have a non-null constraint and shouldn't
                    // (https://github.com/airbytehq/airbyte/pull/31082)

                    existingTable.schema!!
                        .fields
                        .stream()
                        .filter { field: Field -> pks.contains(field.name) }
                        .filter { field: Field -> field.mode == Field.Mode.REQUIRED }
                        .map { obj: Field -> obj.name }
                )
                .collect(Collectors.toSet())

        val isDestinationV2Format = schemaContainAllFinalTableV2AirbyteColumns(existingSchema.keys)

        return AlterTableReport(
            columnsToAdd,
            columnsToRemove,
            columnsToChangeType,
            isDestinationV2Format
        )
    }

    override fun createNamespaces(schemas: Set<String>) {
        schemas.forEach(Consumer { dataset: String -> this.createDataset(dataset) })
    }

    private fun createDataset(dataset: String) {
        LOGGER.info("Creating dataset if not present {}", dataset)
        try {
            BigQueryUtils.getOrCreateDataset(bq, dataset, datasetLocation)
        } catch (e: BigQueryException) {
            if (ConnectorExceptionUtil.HTTP_AUTHENTICATION_ERROR_CODES.contains(e.code)) {
                throw ConfigErrorException(e.message!!, e)
            } else {
                throw e
            }
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(BigQueryDestinationHandler::class.java)

        @JvmStatic
        @VisibleForTesting
        fun clusteringMatches(
            stream: StreamConfig,
            existingTable: StandardTableDefinition
        ): Boolean {
            return (existingTable.clustering != null &&
                containsAllIgnoreCase(
                    HashSet<String>(existingTable.clustering!!.fields),
                    BigQuerySqlGenerator.Companion.clusteringColumns(stream)
                ))
        }

        @JvmStatic
        @VisibleForTesting
        fun partitioningMatches(existingTable: StandardTableDefinition): Boolean {
            return existingTable.timePartitioning != null &&
                existingTable.timePartitioning!!
                    .field
                    .equals("_airbyte_extracted_at", ignoreCase = true) &&
                TimePartitioning.Type.DAY == existingTable.timePartitioning!!.type
        }

        /**
         * Checks the schema to determine whether the table contains all expected final table
         * airbyte columns
         *
         * @param columnNames the column names of the schema to check
         * @return whether all the [JavaBaseConstants.V2_FINAL_TABLE_METADATA_COLUMNS] are present
         */
        @VisibleForTesting
        @JvmStatic
        fun schemaContainAllFinalTableV2AirbyteColumns(columnNames: Collection<String>?): Boolean {
            return JavaBaseConstants.V2_FINAL_TABLE_METADATA_COLUMNS.stream()
                .allMatch(
                    Predicate<String> { column: String? ->
                        containsIgnoreCase(columnNames!!, column!!)
                    }
                )
        }

        private fun getPks(stream: StreamConfig): Set<String> {
            return stream.primaryKey.map(ColumnId::name).toSet()
        }
    }
}

private val logger = KotlinLogging.logger {}
/**
 * Assumes that the old_raw_table_5mb_part1/part2 + new_input_table_5mb_part1/part2 tables already exist.
 * Will drop+recreate the old_final_table_5mb / new_final_table_5mb tables as needed.
 */
fun main() {
    val size = "50gb"
    val runOldRawTablesFast = true
    val runOldRawTablesSlow = true
    val runNewTableNaive = true
    val runNewTableOptimized = true

    val config = Jsons.deserialize(Files.readString(Path.of("/Users/edgao/code/airbyte/airbyte-integrations/connectors/destination-bigquery/secrets/credentials-1s1t-gcs.json")))
    val bq = BigQueryDestination.getBigQuery(config)
    val generator =
        BigQuerySqlGenerator(
            projectId = "dataline-integration-testing",
            datasetLocation = "us-east1"
        )
    val destHandler = BigQueryDestinationHandler(bq, "us-east1")

    fun resetOldTypingDeduping() {
        // reset the raw tables (i.e. unset loaded_at)
        destHandler.execute(Sql.separately(
            """
                UPDATE `dataline-integration-testing`.`no_raw_tables_experiment`.`old_raw_table_${size}_part1`
                SET `_airbyte_loaded_at` = NULL
                WHERE true
            """.trimIndent(),
            """
                UPDATE `dataline-integration-testing`.`no_raw_tables_experiment`.`old_raw_table_${size}_part2`
                SET `_airbyte_loaded_at` = NULL
                WHERE true
            """.trimIndent(),
        ))
        // drop+recreate `old_final_table_${size}`
        // part=0 here b/c the part number doesn't matter (we don't need the raw tables yet)
        destHandler.execute(generator.createTable(getStreamConfig(size, 0), suffix = "", force = true))
    }

    if (runOldRawTablesFast) {
        resetOldTypingDeduping()
        logger.info { "Executing old-style fast T+D for $size dataset, part 1 (upsert to empty table)" }
        destHandler.execute(
            generator.updateTable(
                getStreamConfig(size, part = 1),
                finalSuffix = "",
                minRawTimestamp = Optional.empty(),
                useExpensiveSaferCasting = false,
            )
        )
        logger.info { "Executing old-style fast T+D for $size dataset, part 2 (upsert to populated table)" }
        destHandler.execute(
            generator.updateTable(
                getStreamConfig(size, part = 2),
                finalSuffix = "",
                minRawTimestamp = Optional.empty(),
                useExpensiveSaferCasting = false,
            )
        )
    }

    if (runOldRawTablesSlow) {
        resetOldTypingDeduping()
        logger.info { "Executing old-style slow T+D for $size dataset, part 1 (upsert to empty table)" }
        destHandler.execute(
            generator.updateTable(
                getStreamConfig(size, part = 1),
                finalSuffix = "",
                minRawTimestamp = Optional.empty(),
                useExpensiveSaferCasting = true,
            )
        )
        logger.info { "Executing old-style slow T+D for $size dataset, part 2 (upsert to populated table)" }
        destHandler.execute(
            generator.updateTable(
                getStreamConfig(size, part = 2),
                finalSuffix = "",
                minRawTimestamp = Optional.empty(),
                useExpensiveSaferCasting = true,
            )
        )
    }

    PrintWriter(FileOutputStream("/Users/edgao/code/airbyte/raw_table_experiments/generated_files/bigquery_newstyle.sql")).use { out ->
        out.println("-- naive create table --------------------------------")
        out.printSql(getNewStyleCreateFinalTableQuery(size, optimized = false))

        repeat(10) { out.println() }
        out.println("""-- "naive" dedup query -------------------------------""")
        out.println(getNewStyleDedupingQuery(size, 1, optimized = false))

        repeat(10) { out.println() }
        out.println("-- optimized create table --------------------------------")
        out.printSql(getNewStyleCreateFinalTableQuery(size, optimized = true))

        repeat(10) { out.println() }
        out.println("""-- "optimized" dedup query -------------------------------""")
        out.println(getNewStyleDedupingQuery(size, 1, optimized = true))
    }

    if (runNewTableNaive) {
        destHandler.execute(getNewStyleCreateFinalTableQuery(size, optimized = false))
        logger.info { "Executing new-style naive deduping for $size dataset, part 1 (upsert to empty table)" }
        destHandler.execute(Sql.of(getNewStyleDedupingQuery(size, 1, optimized = false)))
        logger.info { "Executing new-style naive deduping for $size dataset, part 2 (upsert to populated table)" }
        destHandler.execute(Sql.of(getNewStyleDedupingQuery(size, 2, optimized = false)))
    }

    if (runNewTableOptimized) {
        destHandler.execute(getNewStyleCreateFinalTableQuery(size, optimized = true))
        logger.info { "Executing new-style optimized deduping for $size dataset, part 1 (upsert to empty table)" }
        destHandler.execute(Sql.of(getNewStyleDedupingQuery(size, 1, optimized = true)))
        logger.info { "Executing new-style optimized deduping for $size dataset, part 2 (upsert to populated table)" }
        destHandler.execute(Sql.of(getNewStyleDedupingQuery(size, 2, optimized = true)))
    }
}

fun getNewStyleCreateFinalTableQuery(size: String, optimized: Boolean): Sql {
    return Sql.separately(
        "DROP TABLE IF EXISTS `dataline-integration-testing`.`no_raw_tables_experiment`.`new_final_table_${size}`",
        """
            CREATE OR REPLACE TABLE `dataline-integration-testing`.`no_raw_tables_experiment`.`new_final_table_${size}` (
              _airbyte_raw_id STRING NOT NULL,
              _airbyte_extracted_at TIMESTAMP NOT NULL,
              _airbyte_meta JSON NOT NULL,
              _airbyte_generation_id INTEGER,
              ${ if (optimized) { "_airbyte_partition_key INTEGER," } else { "" } }
              `primary_key` INT64,
              `cursor` DATETIME,
              `string` STRING,
              `bool` BOOL,
              `integer` INT64,
              `float` NUMERIC,
              `date` DATE,
              `ts_with_tz` TIMESTAMP,
              `ts_without_tz` DATETIME,
              `time_with_tz` STRING,
              `time_no_tz` TIME,
              `array` JSON,
              `json_object` JSON
            )
            ${ 
                if (optimized) {
                    "PARTITION BY (RANGE_BUCKET(_airbyte_partition_key, GENERATE_ARRAY(0, 10000, 10)))"
                } else { 
                    "PARTITION BY (DATE_TRUNC(_airbyte_extracted_at, DAY))" 
                } 
            }
            CLUSTER BY `primary_key`, `_airbyte_extracted_at`;
        """.trimIndent()
    )
}

fun getNewStyleDedupingQuery(size: String, part: Int, optimized: Boolean): String {
    return """
        MERGE `dataline-integration-testing`.`no_raw_tables_experiment`.`new_final_table_${size}` target_table
        USING (
          WITH new_records AS (
            SELECT *
            FROM `dataline-integration-testing`.`no_raw_tables_experiment`.`new_input_table_${size}_part${part}`
          ), numbered_rows AS (
            SELECT *, row_number() OVER (
              PARTITION BY `primary_key` ORDER BY `cursor` DESC NULLS LAST, `_airbyte_extracted_at` DESC
            ) AS row_number
            FROM new_records
          )
          SELECT
            `primary_key`,
            `cursor`,
            `string`,
            `bool`,
            `integer`,
            `float`,
            `date`,
            `ts_with_tz`,
            `ts_without_tz`,
            `time_with_tz`,
            `time_no_tz`,
            `array`,
            `json_object`,
            _airbyte_meta,
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_generation_id
            ${if (optimized) {""", mod(`primary_key`, 10000) as _airbyte_partition_key"""} else { "" } }
          FROM numbered_rows
          WHERE row_number = 1
        ) new_record
        ON (target_table.`primary_key` = new_record.`primary_key` OR (target_table.`primary_key` IS NULL AND new_record.`primary_key` IS NULL))
        WHEN MATCHED AND (
          target_table.`cursor` < new_record.`cursor`
          OR (target_table.`cursor` = new_record.`cursor` AND target_table._airbyte_extracted_at < new_record._airbyte_extracted_at)
          OR (target_table.`cursor` IS NULL AND new_record.`cursor` IS NULL AND target_table._airbyte_extracted_at < new_record._airbyte_extracted_at)
          OR (target_table.`cursor` IS NULL AND new_record.`cursor` IS NOT NULL)
        )
        THEN UPDATE SET
          `primary_key` = new_record.`primary_key`,
          `cursor` = new_record.`cursor`,
          `string` = new_record.`string`,
          `bool` = new_record.`bool`,
          `integer` = new_record.`integer`,
          `float` = new_record.`float`,
          `date` = new_record.`date`,
          `ts_with_tz` = new_record.`ts_with_tz`,
          `ts_without_tz` = new_record.`ts_without_tz`,
          `time_with_tz` = new_record.`time_with_tz`,
          `time_no_tz` = new_record.`time_no_tz`,
          `array` = new_record.`array`,
          `json_object` = new_record.`json_object`,
          _airbyte_meta = new_record._airbyte_meta,
          _airbyte_raw_id = new_record._airbyte_raw_id,
          _airbyte_extracted_at = new_record._airbyte_extracted_at,
          _airbyte_generation_id = new_record._airbyte_generation_id
          ${if (optimized) {""", _airbyte_partition_key = new_record._airbyte_partition_key"""} else { "" } }
        WHEN NOT MATCHED THEN INSERT (
          `primary_key`,
          `cursor`,
          `string`,
          `bool`,
          `integer`,
          `float`,
          `date`,
          `ts_with_tz`,
          `ts_without_tz`,
          `time_with_tz`,
          `time_no_tz`,
          `array`,
          `json_object`,
          _airbyte_meta,
          _airbyte_raw_id,
          _airbyte_extracted_at,
          _airbyte_generation_id
          ${if (optimized) {""", _airbyte_partition_key"""} else { "" } }
        ) VALUES (
          new_record.`primary_key`,
          new_record.`cursor`,
          new_record.`string`,
          new_record.`bool`,
          new_record.`integer`,
          new_record.`float`,
          new_record.`date`,
          new_record.`ts_with_tz`,
          new_record.`ts_without_tz`,
          new_record.`time_with_tz`,
          new_record.`time_no_tz`,
          new_record.`array`,
          new_record.`json_object`,
          new_record._airbyte_meta,
          new_record._airbyte_raw_id,
          new_record._airbyte_extracted_at,
          new_record._airbyte_generation_id
          ${if (optimized) {""", new_record._airbyte_partition_key"""} else { "" } }
        );
    """.trimIndent()
}
