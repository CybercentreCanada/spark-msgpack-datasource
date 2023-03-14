package org.apache.spark.sql.msgpack.converters

import org.apache.spark.sql.msgpack.{MessagePackCoercion, MessagePackServiceLoader}
import org.apache.spark.sql.msgpack.extensions.ExtensionDeserializers
import org.apache.spark.sql.msgpack.visitor.{ValueVisitor, ValueVisitorContext}
import org.apache.spark.sql.types._
import org.msgpack.value._

import scala.collection.mutable.ListBuffer

object TypeDeserializer extends ValueVisitor {

  private val extDeserializers: ExtensionDeserializers = MessagePackServiceLoader.getExtensionDeserializers

  override protected def visit(value: MapValue, context: ValueVisitorContext): Any = {
    val keys = ListBuffer[String]()
    val fields = Array.newBuilder[StructField]
    val iterator = value.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next
      val key = entry.getKey.toString
      if (!keys.contains(key)) {
        keys += key
        fields += StructField(key, visit(entry.getValue, context).asInstanceOf[DataType], nullable = true)
      }
    }
    StructType(MessagePackCoercion.sort(fields.result()));
  }

  override protected def visit(value: ArrayValue, context: ValueVisitorContext): Any = {
    val fields = Array.newBuilder[DataType]
    val iterator = value.iterator();
    while (iterator.hasNext) {
      fields += visit(iterator.next, context).asInstanceOf[DataType]
    }
    ArrayType(MessagePackCoercion.coerce(fields.result()))
  }

  override protected def visit(value: StringValue, context: ValueVisitorContext): Any = {
    StringType
  }

  override protected def visit(value: IntegerValue, context: ValueVisitorContext): Any = {
    LongType
  }

  override protected def visit(value: FloatValue, context: ValueVisitorContext): Any = {
    DoubleType
  }

  override protected def visit(value: BooleanValue, context: ValueVisitorContext): Any = {
    BooleanType
  }

  override protected def visit(value: BinaryValue, context: ValueVisitorContext): Any = {
    BinaryType
  }

  override protected def visit(value: NilValue, context: ValueVisitorContext): Any = {
    NullType
  }

  override protected def visit(value: ExtensionValue, context: ValueVisitorContext): Any = {
    value match {
      case v if extDeserializers.has(v.getType) =>
        extDeserializers.get(v.getType).get().sqlType()
      case v if v.isTimestampValue =>
        TimestampType
      case _ =>
        BinaryType
    }
  }
}
