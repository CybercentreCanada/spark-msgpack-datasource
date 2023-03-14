package org.apache.spark.sql.msgpack.converters

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.msgpack.MessagePackUtil
import org.apache.spark.sql.types._
import org.msgpack.value.{Value, ValueFactory}

object ValueSerializer {

  def convert(value: Any, dataType: DataType): Value = {
    dataType match {
      case _: StructType =>
        convertStruct(value.asInstanceOf[InternalRow], dataType.asInstanceOf[StructType])
      case _: MapType =>
        convertMap(value.asInstanceOf[MapData], dataType.asInstanceOf[MapType])
      case _: ArrayType =>
        convertArray(value.asInstanceOf[ArrayData], dataType.asInstanceOf[ArrayType])
      case StringType =>
        nullSafe(value, () => ValueFactory.newString(value.toString))
      case ShortType =>
        val v = value.asInstanceOf[Short]
        nullSafe(value, () => ValueFactory.newInteger(v))
      case IntegerType =>
        val v = value.asInstanceOf[Int]
        nullSafe(value, () => ValueFactory.newInteger(v))
      case LongType =>
        val v = value.asInstanceOf[Long]
        nullSafe(value, () => ValueFactory.newInteger(v))
      case DoubleType =>
        val v = value.asInstanceOf[Double]
        nullSafe(value, () => ValueFactory.newFloat(v))
      case FloatType =>
        val v = value.asInstanceOf[Float]
        nullSafe(value, () => ValueFactory.newFloat(v))
      case _: DecimalType =>
        // we serialize decimals into strings.
        val v = value.asInstanceOf[Decimal]
        nullSafe(value, () => ValueFactory.newString(v.toString()))
      case BooleanType =>
        val v = value.asInstanceOf[Boolean]
        nullSafe(value, () => ValueFactory.newBoolean(v))
      case DateType =>
        // we getting days here.
        nullSafe(
          value,
          () => {
            val secs: Int = MessagePackUtil.daysToSecs(value.asInstanceOf[Int])
            MessagePackUtil.generateTimestampExtension32(secs)
          }
        )
      case TimestampType =>
        // we getting microseconds here.
        nullSafe(
          value,
          () => {
            val v = value.asInstanceOf[Long]
            val (secs, nanos) = MessagePackUtil.microsToSecsAndNanos(v)
            MessagePackUtil.generateTimestampExtension64(secs, nanos)
          }
        )
      case ByteType =>
        // we storing single byte in a byte array of length 1.
        nullSafe(
          value,
          () => {
            val v: Byte = value.asInstanceOf[Byte]
            ValueFactory.newBinary(Array[Byte](v), true)
          }
        )
      case BinaryType =>
        nullSafe(
          value,
          () => {
            val v: Array[Byte] = value.asInstanceOf[Array[Byte]]
            ValueFactory.newBinary(v, true)
          }
        )
      case NullType =>
        ValueFactory.newNil()
      case _ =>
        // TODO: maybe ignore and log warning?
        throw new IllegalArgumentException(
          "Unsupported ValueType: %s".format(dataType.typeName)
        )
    }
  }

  private def convertStruct(data: InternalRow, struct: StructType): Value = {
    nullSafe(
      data,
      () => {
        val map = ValueFactory.newMapBuilder()
        for (i <- struct.fields.indices) {
          val field = struct.fields(i)
          val value = data.get(i, field.dataType)
          map.put(
            ValueFactory.newString(field.name),
            convert(value, field.dataType)
          )
        }
        map.build()

      }
    )
  }

  private def convertMap(data: MapData, mapType: MapType): Value = {

    nullSafe(
      data,
      () => {
        val map = ValueFactory.newMapBuilder()

        data.foreach(
          mapType.keyType,
          mapType.valueType,
          (key, value) => {
            map.put(
              convert(key, mapType.keyType),
              convert(value, mapType.valueType)
            )
          }
        )
        map.build()
      }
    )
  }

  private def convertArray(data: ArrayData, arrayType: ArrayType): Value = {
    nullSafe(
      data,
      () => {
        val values = new Array[Value](data.numElements())
        data.foreach(
          arrayType.elementType,
          (position, value) => {
            values.update(position, convert(value, arrayType.elementType))
          }
        )
        ValueFactory.newArray(values, true)
      }
    )
  }

  private def nullSafe(value: Any, toValue: () => Value): Value = {
    if (value != null) {
      return toValue()
    }
    //println("returning null")
    ValueFactory.newNil()
  }

}
