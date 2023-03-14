package org.apache.spark.sql.v2.msgpack

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.v2.FileWrite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.msgpack.MessagePackOutputWriterFactory
import org.apache.spark.sql.types.{DataType, StructType}

case class MessagePackWriteBuilder(
    paths: Seq[String],
    formatName: String,
    supportsDataType: DataType => Boolean,
    info: LogicalWriteInfo
) extends FileWrite {
  override def prepareWrite(
      sqlConf: SQLConf,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType
  ): OutputWriterFactory = {
    // TODO: handle options?
    new MessagePackOutputWriterFactory()
  }

}
