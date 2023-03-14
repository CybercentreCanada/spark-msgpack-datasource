package org.apache.spark.sql.msgpack.converters

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.msgpack.extensions.ExtensionDeserializers
import org.apache.spark.sql.msgpack.visitor.{ValueVisitor, ValueVisitorContext, ValueVisitorException}
import org.apache.spark.sql.msgpack.{MessagePackOptions, MessagePackServiceLoader, MessagePackUtil}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.msgpack.value._

import scala.util.{Failure, Success, Try}

object ValueDeserializer extends ValueVisitor with Logging {

  private val extDeserializers: ExtensionDeserializers =
    MessagePackServiceLoader.getExtensionDeserializers

  def toRow(value: Value, schema: StructType, options: MessagePackOptions): InternalRow = {
    visit(value, new ValueVisitorContext(schema, options)).asInstanceOf[InternalRow]
  }

  override protected def visit(value: MapValue, context: ValueVisitorContext): Any = {
    context.dataType match {
      case null | _: MapType =>
        convertMap(value, context)
      case _: StructType =>
        convertStruct(value, context)
      case StringType =>
        UTF8String.fromString(value.toString)
      case BinaryType =>
        value.asRawValue().asByteArray()
      case _ =>
        nullOnLenientElseThrow(value, context)
    }
  }

  override protected def visit(value: ArrayValue, context: ValueVisitorContext): Any = {
    context.dataType match {
      case null | _: ArrayType =>
        convertArray(value, context)
      case StringType =>
        UTF8String.fromString(value.toString)
      case BinaryType =>
        value.asRawValue().asByteArray()
      case _ =>
        nullOnLenientElseThrow(value, context)
    }
  }

  override protected def visit(value: StringValue, context: ValueVisitorContext): Any = {
    context.dataType match {
      case null | StringType =>
        UTF8String.fromString(value.toString)
      case LongType =>
        resolve(() => value.toString.toLong, value, context)
      case DoubleType =>
        resolve(() => value.toString.toDouble, value, context)
      case IntegerType =>
        resolve(() => value.toString.toInt, value, context)
      case ShortType =>
        resolve(() => value.toString.toShort, value, context)
      case _: DecimalType =>
        resolve(() => Decimal(value.toString), value, context)
      case BinaryType =>
        value.asRawValue().asByteArray()
      case _ =>
        nullOnLenientElseThrow(value, context)
    }
  }

  override protected def visit(value: IntegerValue, context: ValueVisitorContext): Any = {
    context.dataType match {
      case null | IntegerType =>
        resolve(() => value.toInt, value, context)
      case ShortType =>
        resolve(() => value.toShort, value, context)
      case LongType =>
        resolve(() => value.toLong, value, context)
      case FloatType =>
        resolve(() => value.toFloat, value, context)
      case DoubleType =>
        resolve(() => value.toDouble, value, context)
      case _: DecimalType =>
        resolve(() => Decimal(value.toInt), value, context)
      case StringType =>
        UTF8String.fromString(value.toString)
      case BinaryType =>
        value.asRawValue().asByteArray()
      case _ =>
        nullOnLenientElseThrow(value, context)
    }
  }

  override protected def visit(value: FloatValue, context: ValueVisitorContext): Any = {
    context.dataType match {
      case null | FloatType =>
        resolve(() => value.toFloat, value, context)
      case LongType =>
        resolve(() => value.toLong, value, context)
      case DoubleType =>
        resolve(() => value.toDouble, value, context)
      case _: DecimalType =>
        resolve(() => Decimal(value.toFloat), value, context)
      case StringType =>
        UTF8String.fromString(value.toString)
      case BinaryType =>
        value.asRawValue().asByteArray()
      case _ =>
        nullOnLenientElseThrow(value, context)
    }
  }

  override protected def visit(value: BooleanValue, context: ValueVisitorContext): Any = {
    context.dataType match {
      case null | BooleanType =>
        value.getBoolean
      case StringType =>
        UTF8String.fromString(value.toString)
      case BinaryType =>
        value.asRawValue().asByteArray()
      case _ =>
        nullOnLenientElseThrow(value, context)
    }
  }

  override protected def visit(value: BinaryValue, context: ValueVisitorContext): Any = {
    context.dataType match {
      case null | BinaryType =>
        value.asByteArray()
      case ByteType =>
        value.asByteArray()(0)
      case StringType =>
        UTF8String.fromString(value.toString)
      case _ =>
        nullOnLenientElseThrow(value, context)
    }
  }

  override protected def visit(value: NilValue, context: ValueVisitorContext): Any = {
    null
  }

  override protected def visit(value: ExtensionValue, context: ValueVisitorContext): Any = {
    if (extDeserializers != null && extDeserializers.has(value.getType)) {
      val extDeserializer = extDeserializers.get(value.getType).get()
      return extDeserializer.deserialize(value.getData)
    } else if (value.isTimestampValue) {
      val instant = value.asExtensionValue.asTimestampValue.toInstant
      val secs = instant.getEpochSecond
      val nanos = instant.getNano
      return (secs * 1000000) + (nanos / 1000)
    }
    value.getData
  }

  private def convertStruct(value: MapValue, context: ValueVisitorContext = new ValueVisitorContext()): InternalRow = {
    val dataType = context.dataType.asInstanceOf[StructType]
    val row = new SpecificInternalRow(dataType.fields.map(_.dataType).toIndexedSeq)
    val iterator = value.entrySet.iterator
    while (iterator.hasNext) {
      val entry = iterator.next
      val key = entry.getKey.toString
      val fieldPosition = dataType.getFieldIndex(key).getOrElse(-1)
      if (fieldPosition > -1) {
        context.pathIn(key)
        context.dataType = dataType.fields(fieldPosition).dataType
        row.update(fieldPosition, visit(entry.getValue, context))
        context.pathOut()
      }
    }
    context.dataType = null
    row
  }

  private def convertMap(value: MapValue, context: ValueVisitorContext): MapData = {
    val mapType = if (context.dataType != null) context.dataType.asInstanceOf[MapType] else null
    val keyType = if (mapType != null) mapType.keyType else null
    val valueType = if (mapType != null) mapType.valueType else null
    val keys = new GenericArrayData(new Array[Any](value.size()))
    val values = new GenericArrayData(new Array[Any](value.size()))
    var position = 0
    val iterator = value.entrySet.iterator
    while (iterator.hasNext) {
      val entry = iterator.next
      val _key = entry.getKey
      val _value = entry.getValue
      context.pathIn(_key.toString)
      context.dataType = keyType
      keys.update(position, visit(_key))
      context.dataType = valueType
      values.update(position, visit(_value))
      context.pathOut()
      position += 1
    }
    context.dataType = null
    new ArrayBasedMapData(keys, values)
  }

  private def convertArray(value: ArrayValue, context: ValueVisitorContext): ArrayData = {
    val arrayType = if (context.dataType != null) context.dataType.asInstanceOf[ArrayType] else null
    val elementType = if (arrayType != null) arrayType.elementType else null
    val data = new GenericArrayData(new Array[Any](value.size()))
    val iterator = value.iterator()
    var position = 0
    context.pathIn(-1)
    while (iterator.hasNext) {
      val nextValue = iterator.next
      context.dataType = elementType
      context.pathUpdate(position)
      data.update(position, visit(nextValue, context))
      position += 1
    }
    context.dataType = null
    context.pathOut()
    data
  }

  private def resolve(
      resolver: () => Any,
      value: Value,
      context: ValueVisitorContext = new ValueVisitorContext()
  ): Any =
    Try(resolver()) match {
      case Success(v) => v
      case Failure(_) => nullOnLenientElseThrow(value, context)
    }

  private def nullOnLenientElseThrow(value: Value, context: ValueVisitorContext): Any = {
    if (context.options.deserializerLenient) {
      if (context.options.deserializerTracePath) {
        log.error(MessagePackUtil.tracePathError(value, context))
      }
      return null
    }
    throw new ValueVisitorException(value, context)
  }

}
