package org.apache.spark.sql.msgpack

import org.apache.spark.sql.Column
import org.apache.spark.sql.msgpack.expressions.FromMsgPack
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.classic.ColumnConversions.toRichColumn
import org.apache.spark.sql.functions.column

import org.apache.spark.sql.{Column, functions => F}

object MessagePackFunctions {

  def from_msgpack(col: Column, schema: StructType): Column = {
    from_msgpack(col, schema.json)
  }

  def from_msgpack(col: Column, schemaJsonStr: String): Column = {
    F.expr(s"from_msgpack(${col.expr.sql}, '${schemaJsonStr}')")

  }

}
