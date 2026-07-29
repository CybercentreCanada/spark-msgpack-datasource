package org.apache.spark.sql.msgpack

import org.apache.spark.sql.Column
import org.apache.spark.sql.classic.ExpressionUtils.{column, expression}
import org.apache.spark.sql.msgpack.expressions.FromMsgPack
import org.apache.spark.sql.types.{DataType, StructType}

object MessagePackFunctions {

  def from_msgpack(col: Column, schema: StructType): Column = {
    // Spark 4 decoupled `Column` from Catalyst (for Spark Connect), so the old
    // `Column(expr)` / `col.expr` bridge is gone. `ExpressionUtils.column` /
    // `.expression` (in the internal `org.apache.spark.sql.classic` module) is
    // the supported bridge between a custom Catalyst `Expression` and a `Column`.
    column(FromMsgPack(expression(col), schema))
  }

  def from_msgpack(col: Column, schemaJsonStr: String): Column = {
    from_msgpack(col, DataType.fromJson(schemaJsonStr).asInstanceOf[StructType])
  }

}
