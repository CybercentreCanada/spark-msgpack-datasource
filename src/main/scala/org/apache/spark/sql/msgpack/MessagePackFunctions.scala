package org.apache.spark.sql.msgpack

import org.apache.spark.sql.Column
import org.apache.spark.sql.msgpack.expressions.FromMsgPack
import org.apache.spark.sql.types.{DataType, StructType}

object MessagePackFunctions {

  def from_msgpack(col: Column, schema: StructType): Column = {
    Column(FromMsgPack(col.expr, schema))
  }

  def from_msgpack(col: Column, schemaJsonStr: String): Column = {
    from_msgpack(col, DataType.fromJson(schemaJsonStr).asInstanceOf[StructType])
  }

}
