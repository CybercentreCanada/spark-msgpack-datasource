package org.apache.spark.sql.msgpack.suite.converters

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.msgpack.MessagePackUtil
import org.apache.spark.sql.msgpack.converters.ValueDeserializer
import org.apache.spark.sql.msgpack.visitor.ValueVisitorContext
import org.apache.spark.sql.types._
import org.msgpack.value.ValueFactory
import org.scalatest.funsuite.AnyFunSuite

class ValueDeserializerSuite extends AnyFunSuite {

  test("Convert StringValue to StringType") {
    val str = "test"
    val strValue = ValueDeserializer.visit(ValueFactory.newString(str))
    assert(strValue === MessagePackUtil.toUTF8(str))
  }

  test("Convert IntegerValue to ShortType") {
    val int = 123
    val intValue = ValueDeserializer.visit(ValueFactory.newInteger(int), new ValueVisitorContext(ShortType))
    assert(intValue.isInstanceOf[Short])
    assert(intValue === int)
  }

  test("Convert to IntegerType") {
    val int = 123
    val intValue = ValueDeserializer.visit(ValueFactory.newInteger(int))
    assert(intValue.isInstanceOf[Int])
    assert(intValue === int)
  }

  test("Convert String to DecimalType") {
    val decimalStr = "123456.456789"
    val decimalValue =
      ValueDeserializer.visit(ValueFactory.newString(decimalStr), new ValueVisitorContext(DecimalType(12, 6)))
    assert(decimalValue.isInstanceOf[Decimal])
    assert(decimalValue === Decimal(decimalStr))
  }

  test("Convert Float to LongType") {
    val long = 1345L
    val longValue = ValueDeserializer.visit(ValueFactory.newFloat(long.toFloat), new ValueVisitorContext(LongType))
    assert(longValue.isInstanceOf[Long])
    assert(longValue === long)
  }

  test("Convert Float to DoubleType") {
    val double = 1345.123
    val doubleValue = ValueDeserializer.visit(ValueFactory.newFloat(double), new ValueVisitorContext(DoubleType))
    assert(doubleValue.isInstanceOf[Double])
    assert(doubleValue === double)
  }

  test("Convert to FloatType") {
    val short = 123
    val int = 1234
    val long = 123456L
    val double = 123.456
    val float = 123.456f
    val shortValue = ValueDeserializer.visit(ValueFactory.newFloat(short.toFloat))
    val intValue = ValueDeserializer.visit(ValueFactory.newFloat(int.toFloat))
    val longValue = ValueDeserializer.visit(ValueFactory.newFloat(long.toFloat))
    val doubleValue = ValueDeserializer.visit(ValueFactory.newFloat(double))
    val floatValue = ValueDeserializer.visit(ValueFactory.newFloat(float))
    assert(shortValue.isInstanceOf[Float])
    assert(intValue.isInstanceOf[Float])
    assert(longValue.isInstanceOf[Float])
    assert(doubleValue.isInstanceOf[Float])
    assert(floatValue.isInstanceOf[Float])
    assert(shortValue === short.toFloat)
    assert(intValue === int.toFloat)
    assert(longValue === long.toFloat)
    assert(doubleValue === double.toFloat)
    assert(floatValue === float)
  }

  test("Convert to BooleanType") {
    val bValue = ValueDeserializer.visit(ValueFactory.newBoolean(true))
    assert(bValue === true)
  }

  test("Convert Binary to ByteType") {
    val binary = "b".getBytes()
    val byteValue = ValueDeserializer.visit(ValueFactory.newBinary(binary), new ValueVisitorContext(ByteType))
    assert(byteValue === binary(0))
  }

  test("Convert to BinaryType") {
    val binary = "git'er bindary".getBytes
    val binaryValue = ValueDeserializer.visit(ValueFactory.newBinary(binary))
    assert(binaryValue === binary)
  }

  test("Convert msgpack array of string into an ArrayData[UTF8Strings].") {
    val msgPackValue = ValueFactory.newArray(
      ValueFactory.newString("1"),
      ValueFactory.newString("2"),
      ValueFactory.newString("3")
    )
    val sparkValue = ValueDeserializer.visit(msgPackValue)
    assert(sparkValue.isInstanceOf[ArrayData])
    val sparkArray = sparkValue.asInstanceOf[ArrayData]
    assert(sparkArray.numElements() === 3)
    assert(sparkArray.getUTF8String(0) == MessagePackUtil.toUTF8("1"))
    assert(sparkArray.getUTF8String(1) == MessagePackUtil.toUTF8("2"))
    assert(sparkArray.getUTF8String(2) == MessagePackUtil.toUTF8("3"))
  }

  test("Convert msgpack array of integer into a Seq of Int") {
    val msgPackValue = ValueFactory.newArray(
      ValueFactory.newInteger(1),
      ValueFactory.newInteger(2),
      ValueFactory.newInteger(3)
    )
    val sparkValue = ValueDeserializer.visit(msgPackValue)
    assert(sparkValue.isInstanceOf[ArrayData])
    val sparkArray = sparkValue.asInstanceOf[ArrayData]
    assert(sparkArray.numElements() === 3)
    assert(sparkArray.getInt(0) == 1)
    assert(sparkArray.getInt(1) == 2)
    assert(sparkArray.getInt(2) == 3)
  }

  test("Convert msgpack array of float into a Seq of Float") {
    val msgPackValue = ValueFactory.newArray(
      ValueFactory.newFloat(1.1f),
      ValueFactory.newFloat(2.2f),
      ValueFactory.newFloat(3.3f)
    )
    val sparkValue = ValueDeserializer.visit(msgPackValue)
    assert(sparkValue.isInstanceOf[ArrayData])
    val sparkArray = sparkValue.asInstanceOf[ArrayData]
    assert(sparkArray.numElements() === 3)
    assert(sparkArray.getFloat(0) == 1.1f)
    assert(sparkArray.getFloat(1) == 2.2f)
    assert(sparkArray.getFloat(2) == 3.3f)

  }

  test("Convert msgpack array of mixed type into a Seq of mixed type") {
    val msgPackValue = ValueFactory.newArray(
      ValueFactory.newFloat(1.1f),
      ValueFactory.newString("value2"),
      ValueFactory.newBinary("value3".getBytes())
    )
    val sparkValue = ValueDeserializer.visit(msgPackValue)
    assert(sparkValue.isInstanceOf[ArrayData])
    val sparkArray = sparkValue.asInstanceOf[ArrayData]
    assert(sparkArray.numElements() === 3)
    assert(sparkArray.getFloat(0) === 1.1f)
    assert(sparkArray.getUTF8String(1) === MessagePackUtil.toUTF8("value2"))
    assert(sparkArray.getBinary(2) === "value3".getBytes())
  }

  test("Convert msgpack Map of Strings to Map of UTF8String") {
    val builder = ValueFactory
      .newMapBuilder()
      .put(
        ValueFactory.newString("key1"),
        ValueFactory.newString("value1")
      )
      .put(
        ValueFactory.newString("key2"),
        ValueFactory.newString("value2")
      )
      .put(
        ValueFactory.newString("key3"),
        ValueFactory.newString("value3")
      )
      .put(
        ValueFactory.newString("key4"),
        ValueFactory.newString("value4")
      )

    val mapValue = ValueDeserializer.visit(builder.build())
    assert(mapValue.isInstanceOf[MapData])
    val sparkMapKeys = mapValue.asInstanceOf[MapData].keyArray()
    val sparkMapValues = mapValue.asInstanceOf[MapData].valueArray()
    assert(sparkMapKeys.getUTF8String(0) === MessagePackUtil.toUTF8("key1"))
    assert(sparkMapKeys.getUTF8String(1) === MessagePackUtil.toUTF8("key2"))
    assert(sparkMapKeys.getUTF8String(2) === MessagePackUtil.toUTF8("key3"))
    assert(sparkMapKeys.getUTF8String(3) === MessagePackUtil.toUTF8("key4"))

    assert(sparkMapValues.getUTF8String(0) === MessagePackUtil.toUTF8("value1"))
    assert(sparkMapValues.getUTF8String(1) === MessagePackUtil.toUTF8("value2"))
    assert(sparkMapValues.getUTF8String(2) === MessagePackUtil.toUTF8("value3"))
    assert(sparkMapValues.getUTF8String(3) === MessagePackUtil.toUTF8("value4"))
  }

  test("Convert msgpack Map[String,Value] to StructType as per Schema") {
    val struct = new StructType(
      Array[StructField](
        StructField("key1", StringType, nullable = true),
        StructField("key2", IntegerType, nullable = true),
        StructField("key3", StringType, nullable = true),
        StructField("key5", LongType, nullable = true)
      )
    )

    val builder = ValueFactory.newMapBuilder()
    builder.put(
      ValueFactory.newString("key1"),
      ValueFactory.newString("value1")
    )
    builder.put(ValueFactory.newString("key2"), ValueFactory.newString("123"))
    builder.put(
      ValueFactory.newString("key3"),
      ValueFactory.newString("value3")
    )
    builder.put(
      ValueFactory.newString("key4"),
      ValueFactory.newString("not in schema")
    )
    builder.put(ValueFactory.newString("key5"), ValueFactory.newFloat(789456L))

    val row = ValueDeserializer.visit(builder.build(), new ValueVisitorContext(struct))
    assert(row.isInstanceOf[InternalRow])
    val internalRow = row.asInstanceOf[InternalRow]
    assert(internalRow.numFields === 4)
    assert(internalRow.getString(0) === "value1")
    assert(internalRow.getInt(1) === 123)
    assert(internalRow.getString(2) === "value3")
    assert(internalRow.getLong(3) === 789456L)
  }

}
