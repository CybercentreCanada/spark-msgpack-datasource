package org.apache.spark.sql.msgpack.expressions

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  ExpressionInfo,
  ImplicitCastInputTypes,
  Literal,
  UnaryExpression
}
import org.apache.spark.sql.msgpack.MessagePackDeserializer
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

case class FromMsgPack(expr: Expression, schema: StructType)
    extends UnaryExpression
    with CodegenFallback
    with ImplicitCastInputTypes {

  private val deserializer = new MessagePackDeserializer(schema)

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = schema

  override def child: Expression = expr

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(expr = newChild)

  override protected def nullSafeEval(input: Any): Any = {
    val raw = input.asInstanceOf[Array[Byte]]
    val iterator = deserializer.deserialize(raw)
    if (iterator.hasNext) iterator.next else null
  }
}

object FromMsgPack {
  val DESCRIPTOR: (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = (
    new FunctionIdentifier("from_msgpack"),
    new ExpressionInfo(
      classOf[FromMsgPack].getSimpleName,
      "",
      "from_msgpack",
      "Deserialize a single msgpack map into a spark struct.",
      "",
      "",
      "",
      "", // Group
      "", // Since
      "", // Deprecated
      "scala_udf" // Source
    ),
    (children: Seq[Expression]) => {
      val expr = children.head
      val schemaStr = children.seq(1).asInstanceOf[Literal].value.asInstanceOf[UTF8String]
      val schemaType = Try(StructType.fromDDL(schemaStr.toString))
        .getOrElse(DataType.fromJson(schemaStr.toString).asInstanceOf[StructType])
      FromMsgPack(expr, schemaType)
    }
  )
}
