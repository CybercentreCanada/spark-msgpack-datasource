package org.apache.spark.sql.msgpack

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.msgpack.converters.ValueSerializer
import org.apache.spark.sql.types._
import org.msgpack.value.{Value, ValueFactory}

class MessagePackSerializer(schema: DataType) extends Logging with Serializable {

  def serialize(catalystData: Any): Value = {
    schema match {
      case _: StructType =>
        ValueSerializer.convert(catalystData.asInstanceOf[InternalRow], schema.asInstanceOf[StructType])
      case _ => ValueFactory.newNil()
    }
  }

}
