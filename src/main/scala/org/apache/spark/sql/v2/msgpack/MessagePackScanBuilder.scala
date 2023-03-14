package org.apache.spark.sql.v2.msgpack

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MessagePackScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap
) extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {

  override def build(): Scan =
    MessagePackScan(sparkSession, fileIndex, dataSchema, readDataSchema(), readPartitionSchema(), options)

}
