package org.apache.spark.sql.msgpack.extensions

import org.apache.spark.sql.types.{DataType, DataTypes}

class DefaultDeserializer(extType: Int) extends ExtensionDeserializer {

  override def extensionType(): Int = extType

  override def sqlType(): DataType = DataTypes.BinaryType

  override def deserialize(data: Array[Byte]): Any = data
}
