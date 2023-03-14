package org.apache.spark.sql.msgpack

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.{DataType, StructType}
import org.msgpack.core.MessagePack
import org.msgpack.value.MapValue

class MessagePackOutputWriter(
    val path: String,
    context: TaskAttemptContext,
    schema: StructType
) extends OutputWriter {

  private val packer = MessagePack.newDefaultBufferPacker()

  private val serializer = new MessagePackSerializer(
    schema.asInstanceOf[DataType]
  )

  override def write(row: InternalRow): Unit = {
    val record = serializer.serialize(row).asInstanceOf[MapValue]
    packer.packValue(record)
  }

  override def close(): Unit = {
    // TODO: switch to CodecStreams.createOutputStream
    val p = new Path(path)
    val os = p.getFileSystem(context.getConfiguration).create(p)
    // TODO: consider using toMessageBuffer for better performance.
    os.write(packer.toByteArray)
    os.close()
    //packer.close()
  }

}
