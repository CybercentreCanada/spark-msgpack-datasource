package org.apache.spark.sql.msgpack

import org.apache.spark.sql.Column
import org.apache.spark.sql.msgpack.expressions.FromMsgPack
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.column

object MessagePackFunctions {

  def from_msgpack(col: Column, schema: StructType): Column = {
    val expr = MessagePackUtil.extractExpression(col)
    MessagePackUtil.expressionToColumn(FromMsgPack(expr, schema))
    // columnFromExpression(FromMsgPack(expr, schema))
  }


  def from_msgpack(col: Column, schemaJsonStr: String): Column = {
    from_msgpack(col, DataType.fromJson(schemaJsonStr).asInstanceOf[StructType])
  }

  

}
