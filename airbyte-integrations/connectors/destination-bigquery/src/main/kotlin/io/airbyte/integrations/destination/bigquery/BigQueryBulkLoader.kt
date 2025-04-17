/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.bigquery

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.FormatOptions
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.LoadJobConfiguration
import com.google.cloud.bigquery.TableId
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.file.s3.S3KotlinClient
import io.airbyte.cdk.load.file.s3.S3Object
import io.airbyte.cdk.load.message.StreamKey
import io.airbyte.cdk.load.orchestration.ColumnNameMapping
import io.airbyte.cdk.load.orchestration.TableName
import io.airbyte.cdk.load.orchestration.TableNames
import io.airbyte.cdk.load.write.db.BulkLoader
import io.airbyte.cdk.load.write.db.BulkLoaderFactory
import io.airbyte.integrations.destination.bigquery.formatter.BigQueryRecordFormatter
import io.airbyte.integrations.destination.bigquery.spec.BigqueryConfiguration
import io.airbyte.integrations.destination.bigquery.spec.GcsFilePostProcessing
import io.airbyte.integrations.destination.bigquery.spec.GcsStagingSpecification

class BigQueryBulkLoader(
    private val storageClient: S3KotlinClient,
    private val bigQueryClient: BigQuery,
    private val bigQueryConfiguration: BigqueryConfiguration,
    private val rawTableName: TableName,
) : BulkLoader<S3Object> {
    override suspend fun load(remoteObject: S3Object) {
        val rawTableId = TableId.of(rawTableName.namespace, rawTableName.name)
        val gcsUri = "gs://${remoteObject.keyWithBucketName}"

        val configuration =
            LoadJobConfiguration.builder(rawTableId, gcsUri)
                .setFormatOptions(FormatOptions.csv())
                .setSchema(BigQueryRecordFormatter.SCHEMA_V2)
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                .setJobTimeoutMs(600000L) // 10 min timeout
                .build()

        val loadJob = bigQueryClient.create(JobInfo.of(configuration))

        try {
            BigQueryUtils.waitForJobFinish(loadJob)
        } catch (e: Exception) {
            throw RuntimeException(
                "Failed to load CSV data from $gcsUri to table ${rawTableId.dataset}.${rawTableId.table}: ${e.message}",
                e
            )
        }

        val loadingMethodPostProcessing =
            (bigQueryConfiguration.loadingMethod as GcsStagingSpecification).filePostProcessing
        if (loadingMethodPostProcessing == GcsFilePostProcessing.DELETE) {
            storageClient.delete(remoteObject)
        }
    }

    override fun close() {
        /* Do nothing */
    }
}

// @Singleton
class BigQueryBulkLoaderFactory(
    private val names: Map<DestinationStream.Descriptor, Pair<TableNames, ColumnNameMapping>>,
    private val storageClient: S3KotlinClient,
    private val bigQueryClient: BigQuery,
    private val bigQueryConfiguration: BigqueryConfiguration
) : BulkLoaderFactory<StreamKey, S3Object> {
    override val maxNumConcurrentLoads: Int = 1
    override fun create(key: StreamKey, partition: Int): BulkLoader<S3Object> {
        return BigQueryBulkLoader(
            storageClient,
            bigQueryClient,
            bigQueryConfiguration,
            names[key.stream]!!.first.rawTableName!!,
        )
    }
}
