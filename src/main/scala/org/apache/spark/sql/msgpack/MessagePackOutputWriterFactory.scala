package org.apache.spark.sql.msgpack

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.{
  CodecStreams,
  OutputWriter,
  OutputWriterFactory
}
import org.apache.spark.sql.types.StructType

class MessagePackOutputWriterFactory extends OutputWriterFactory {

  override def getFileExtension(context: TaskAttemptContext): String =
    ".mp" + CodecStreams.getCompressionExtension(context)

  override def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext
  ): OutputWriter = {
    new MessagePackOutputWriter(path, context, dataSchema)
  }
}
