package org.apache.spark.sql.msgpack.extensions

import org.apache.spark.sql.types.DataType

trait ExtensionDeserializer extends Serializable {

  def extensionType(): Int

  def sqlType(): DataType

  def deserialize(data: Array[Byte]): Any

}
