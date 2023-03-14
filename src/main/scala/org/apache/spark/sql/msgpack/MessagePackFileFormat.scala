package org.apache.spark.sql.msgpack

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration
import org.msgpack.core.MessagePack

import java.net.URI
import scala.collection.JavaConverters.mapAsJavaMapConverter

class MessagePackFileFormat extends FileFormat with DataSourceRegister with Logging with Serializable {

  // Since msgpack is straight binary, setting this to false in order to ensure entire files are read as a single unit.
  // Setting this this to true causes inconsistent results when loads are spread across multiple cores.
  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path
  ): Boolean = false

  override def shortName(): String = "messagepack"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]
  ): Option[StructType] = {
    MessagePackSchema.inferFromFiles(sparkSession, files, new CaseInsensitiveStringMap(options.asJava))
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType
  ): OutputWriterFactory = {
    new MessagePackOutputWriterFactory()
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration
  ): (PartitionedFile) => Iterator[InternalRow] = {

    // serializable configurations.
    val broadcastedConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val conf = broadcastedConf.value.value

    // the [requiredSchema] contains a struct that is representative of the query expression.
    // i.e. it reflects the user specified schema, not the full schema.
    val deserializer = new MessagePackDeserializer(
      requiredSchema,
      dataSchema,
      new MessagePackOptions(new CaseInsensitiveStringMap(options.asJava))
    )

    // deserialize over each file.
    (file: PartitionedFile) => {
      val path = new Path(new URI(file.filePath))
      val is = MessagePackUtil.createInputStream(path, conf)
      deserializer.deserialize(MessagePack.newDefaultUnpacker(is))
    }
  }

}
