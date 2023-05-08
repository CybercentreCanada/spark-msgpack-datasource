package org.apache.spark.sql.v2.msgpack

import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, PartitionReaderFromIterator}
import org.apache.spark.sql.msgpack.{MessagePackDeserializer, MessagePackOptions, MessagePackUtil}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.net.URI

case class MessagePackPartitionReaderFactory(
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    options: MessagePackOptions,
    broadcastedConf: Broadcast[SerializableConfiguration]
) extends FilePartitionReaderFactory
    with Logging {

  override def buildReader(
      partitionedFile: PartitionedFile
  ): PartitionReader[InternalRow] = {
    val conf = broadcastedConf.value.value
//    val path = new Path(new URI(partitionedFile.filePath))
    val path = partitionedFile.filePath.toPath
    val is = MessagePackUtil.createInputStream(path, conf)
    new PartitionReaderFromIterator[InternalRow](
      new MessagePackDeserializer(readDataSchema, dataSchema, options).deserialize(is)
    )
  }
}
