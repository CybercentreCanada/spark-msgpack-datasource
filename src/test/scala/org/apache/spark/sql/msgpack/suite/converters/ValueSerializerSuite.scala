package org.apache.spark.sql.msgpack.suite.converters

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapBuilder, GenericArrayData}
import org.apache.spark.sql.msgpack.MessagePackUtil.toUTF8
import org.apache.spark.sql.msgpack.converters.ValueSerializer.convert
import org.apache.spark.sql.msgpack.test.utils.MessagePackSuiteUtil
import org.apache.spark.sql.types._
import org.msgpack.value.ValueFactory
import org.scalatest.funsuite.AnyFunSuite

class ValueSerializerSuite extends AnyFunSuite {

  test("Serialize a UTF8String into a StringValue") {
    assert(
      convert(toUTF8("test"), StringType) === ValueFactory.newString("test")
    )
  }

  test("Serialize a short number into an IntegerValue") {
    assert(convert(1.toShort, ShortType) === ValueFactory.newInteger(1))
  }

  test("Serialize a integer number into an IntegerValue") {
    assert(convert(1, IntegerType) === ValueFactory.newInteger(1))
  }

  test("Serialize a long number into an IntegerValue") {
    assert(convert(1L, LongType) === ValueFactory.newInteger(1))
  }

  test("Serialize a double number into an FloatValue") {
    assert(convert(123.456, DoubleType) === ValueFactory.newFloat(123.456))
  }

  test("Serialize a decimal number into an string") {
    assert(
      convert(Decimal("123567.123567"), DecimalType(12, 6)) === ValueFactory.newString("123567.123567")
    )
  }

  test("Serialize a boolean into an BooleanValue") {
    assert(convert(true, BooleanType) === ValueFactory.newBoolean(true))
  }

  test("Serialize a date into a 32bit/4byte ExtensionValue") {
    val date = MessagePackSuiteUtil.timestamp(2020, 0, 1)
    val days = (((date.getTime / 1000) / 60) / 60) / 24
    val value = convert(days.toInt, DateType)
    assert(value.isExtensionValue)
    assert(value.asExtensionValue().getData.length === 4)
  }

  test("Serialize a timestamp into a 64bit/8byte ExtensionValue") {
    val date = MessagePackSuiteUtil.now(111111)
    val value = convert(date.getTime, TimestampType)
    assert(value.isExtensionValue)
    assert(value.asExtensionValue().getData.length === 8)
  }

  test("Serialize a Byte into an BinaryValue") {
    val bites = "t".getBytes()
    assert(convert(bites(0), ByteType) === ValueFactory.newBinary(bites))
  }

  test("Serialize Bytes into an BinaryValue") {
    val bites = "t".getBytes()
    assert(convert(bites, BinaryType) === ValueFactory.newBinary(bites))
  }

  test("Serialize a MapData[StringType,StringType] into a msgpack MapValue") {
    val builder = new ArrayBasedMapBuilder(StringType, StringType)
    builder.put("key1", "value1")
    builder.put("key2", "value2")
    builder.put("key3", "value3")
    val value = convert(builder.build(), MapType(StringType, StringType))
    assert(value.isMapValue)
    assert(value.asMapValue().size() === 3)
  }

  test("Serialize an InternalRow int a msgpack MapValue") {
    val struct = new StructType(
      Array[StructField](
        StructField("k1", StringType, nullable = true),
        StructField("k2", IntegerType, nullable = true),
        StructField("k3", BinaryType, nullable = true)
      )
    )

    val row = new GenericInternalRow(3)
    row.update(0, "value1")
    row.update(1, 1)
    row.update(2, "bytes".getBytes)

    val value = convert(row, struct)
    assert(value.isMapValue)
    assert(value.asMapValue().size() === 3)
  }

  test("Serialize an array of integer into an ArrayType[IntegerType]") {
    val data = new GenericArrayData(new Array[Any](3))
    data.update(0, 0)
    data.update(1, 1)
    data.update(2, 2)

    val value = convert(data, ArrayType(IntegerType))
    assert(value.isArrayValue)
    assert(value.asArrayValue().size() === 3)
  }

}
