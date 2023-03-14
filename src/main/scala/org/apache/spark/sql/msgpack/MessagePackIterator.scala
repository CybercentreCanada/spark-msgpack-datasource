package org.apache.spark.sql.msgpack

import org.msgpack.core.{MessagePack, MessageUnpacker}
import org.msgpack.value.{MapValue, ValueType}

import java.io.{Closeable, InputStream}

class MessagePackIterator(unpacker: MessageUnpacker) extends Iterator[MapValue] with Serializable with Closeable {

  def this(is: InputStream) = this(MessagePack.newDefaultUnpacker(is))

  def this(bytes: Array[Byte]) = this(MessagePack.newDefaultUnpacker(bytes))

  private var position = -1

  override def hasNext: Boolean = {
    unpacker.hasNext
  }

  override def next(): MapValue = {
    position += 1
    unpacker.getNextFormat.getValueType match {
      case ValueType.MAP =>
        unpacker.unpackValue().asMapValue()
      case valueType =>
        throw new IllegalArgumentException(
          String.format("Invalid ValueType[%s] at root position[%s].  Must be MAP.", valueType.name(), "" + position)
        )
    }
  }

  override def close(): Unit = {
    unpacker.close()
  }
}
