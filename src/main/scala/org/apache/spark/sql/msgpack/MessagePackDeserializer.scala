package org.apache.spark.sql.msgpack

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.msgpack.converters.ValueDeserializer
import org.apache.spark.sql.types.StructType
import org.msgpack.core.{MessagePack, MessageUnpacker}
import org.msgpack.value.MapValue

import java.io.InputStream

class MessagePackDeserializer(readSchema: StructType, dataSchema: StructType, options: MessagePackOptions)
    extends Serializable {

  def this(readSchema: StructType, options: MessagePackOptions = new MessagePackOptions()) =
    this(readSchema, readSchema, options)

  def deserialize(bytes: Array[Byte]): Iterator[InternalRow] = {
    deserialize(MessagePack.newDefaultUnpacker(bytes))
  }

  def deserialize(is: InputStream): Iterator[InternalRow] = {
    deserialize(MessagePack.newDefaultUnpacker(is))
  }

  def deserialize(unpacker: MessageUnpacker): Iterator[InternalRow] = {
    if (dataSchema.isEmpty) {
      Iterator.empty
    } else {
      val mapIterator = new MessagePackIterator((unpacker))
      new Iterator[InternalRow] {
        override def hasNext: Boolean = mapIterator.hasNext
        override def next(): InternalRow = rootConverter(mapIterator.next())
      }
    }
  }

  private def rootConverter(map: MapValue): InternalRow = {
    if (readSchema.isEmpty) InternalRow.empty else ValueDeserializer.toRow(map, readSchema, options)
  }
}
