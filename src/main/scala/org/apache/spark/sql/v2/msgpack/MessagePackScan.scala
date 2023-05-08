package org.apache.spark.sql.v2.msgpack

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.msgpack.MessagePackOptions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._

case class MessagePackScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty
) extends FileScan {

  // Since msgpack is straight binary, setting this to false in order to ensure entire files are read as a single unit.
  // Setting this this to true causes inconsistent results when loads are spread across multiple cores.
  override def isSplitable(path: Path): Boolean = false

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val messagePackOptions = new MessagePackOptions(caseSensitiveMap)
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    MessagePackPartitionReaderFactory(
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      messagePackOptions,
      broadcastedConf
    )
  }

}
