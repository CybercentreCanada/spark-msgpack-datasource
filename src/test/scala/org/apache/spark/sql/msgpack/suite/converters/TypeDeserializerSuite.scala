package org.apache.spark.sql.msgpack.suite.converters

import org.apache.spark.sql.msgpack.converters.TypeDeserializer
import org.apache.spark.sql.types._
import org.msgpack.value.ValueFactory
import org.scalatest.funsuite.AnyFunSuite

class TypeDeserializerSuite extends AnyFunSuite {

  test("TypeDeserializer: StringValue") {
    val dataType = TypeDeserializer.visit(ValueFactory.newString("test"))
    assert(dataType.isInstanceOf[StringType])
  }

  test("TypeDeserializer: IntegerValue") {
    val dataType = TypeDeserializer.visit(ValueFactory.newInteger(1))
    assert(dataType.isInstanceOf[LongType])
  }

  test("TypeDeserializer: FloatValue") {
    val dataType = TypeDeserializer.visit(ValueFactory.newFloat(1f))
    assert(dataType.isInstanceOf[DoubleType])
  }

  test("TypeDeserializer: BooleanValue") {
    val dataType = TypeDeserializer.visit(ValueFactory.newBoolean(true))
    assert(dataType.isInstanceOf[BooleanType])
  }

  test("TypeDeserializer: NilValue") {
    val dataType = TypeDeserializer.visit(ValueFactory.newNil())
    assert(dataType.isInstanceOf[NullType])
  }

  test("TypeDeserializer: BinaryValue") {
    val dataType = TypeDeserializer.visit(ValueFactory.newBinary("testing".getBytes))
    assert(dataType.isInstanceOf[BinaryType])
  }

  test("TypeDeserializer: ArrayValue[StringValue]") {
    val arrayValue = ValueFactory.newArray(
      ValueFactory.newString("v1"),
      ValueFactory.newString("v2")
    )
    val dataType = TypeDeserializer.visit(arrayValue);
    assert(dataType.isInstanceOf[ArrayType])
    assert(dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StringType])
  }

  test("TypeDeserializer: ArrayValue[IntegerValue, FloatValue]") {
    val arrayValue = ValueFactory.newArray(
      ValueFactory.newInteger(1),
      ValueFactory.newFloat(2f)
    )
    val dataType = TypeDeserializer.visit(arrayValue);
    assert(dataType.isInstanceOf[ArrayType])
    assert(dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[DoubleType])
  }

  test("TypeDeserializer: MapValue") {
    val mapValue = ValueFactory
      .newMapBuilder()
      .put(ValueFactory.newString("k1"), ValueFactory.newString("v1"))
      .put(ValueFactory.newString("k2"), ValueFactory.newInteger(1))
      .put(ValueFactory.newString("k3"), ValueFactory.newFloat(1f))
      .put(ValueFactory.newString("k5"), ValueFactory.newBinary("test".getBytes))
      .put(ValueFactory.newString("k6"), ValueFactory.newNil())
      .put(
        ValueFactory.newString("k7"),
        ValueFactory.newArray(ValueFactory.newInteger(1), ValueFactory.newFloat(2f))
      )
      .put(ValueFactory.newString("k4"), ValueFactory.newBoolean(true))
      .build()

    val dataType = TypeDeserializer.visit(mapValue).asInstanceOf[StructType]
    // Assert correct type and order of each fields.
    assert(dataType.fields(0).dataType.isInstanceOf[StringType])
    assert(dataType.fields(1).dataType.isInstanceOf[LongType])
    assert(dataType.fields(2).dataType.isInstanceOf[DoubleType])
    assert(dataType.fields(3).dataType.isInstanceOf[BooleanType])
    assert(dataType.fields(4).dataType.isInstanceOf[BinaryType])
    assert(dataType.fields(5).dataType.isInstanceOf[NullType])
    assert(dataType.fields(6).dataType.isInstanceOf[ArrayType])
    assert(dataType.fields(6).dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[DoubleType])
  }

}
