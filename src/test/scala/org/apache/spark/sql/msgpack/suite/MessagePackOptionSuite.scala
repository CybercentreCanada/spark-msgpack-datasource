package org.apache.spark.sql.msgpack.suite

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.msgpack.MessagePackOptions
import org.apache.spark.sql.msgpack.test.data.MessagePackData
import org.apache.spark.sql.msgpack.test.data.impl.InconsistentData
import org.apache.spark.sql.msgpack.visitor.ValueVisitorException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DoubleType, LongType}

class MessagePackOptionSuite extends QueryTest with SharedSparkSession {

  private val COERCABLE_DATA = new MessagePackData() {
    override def name(): String = "CoercableData"
    override def pack(): Unit =
      packer.packMapHeader(1).packString("one").packLong(1L).packMapHeader(1).packString("one").packDouble(2d)
  }

  test("deserialization.lenient: false -> yields exceptions") {
    val data = new InconsistentData();
    val df = spark.read
      .format("messagepack")
      .schema(data.schema())
      .load(data.write())
    val exception = intercept[SparkException] {
      df.show()
    }
    assert(exception.getCause.getCause.isInstanceOf[ValueVisitorException])
    assert(exception.getCause.getCause.getMessage === "msgpack[STRING] cannot be converted to spark[long]")
  }

  test("deserialization.lenient: true -> yields null.") {
    val data = new InconsistentData();
    val df = spark.read
      .format("messagepack")
      .schema(data.schema())
      .option(MessagePackOptions.DESERIALIZER_LENIENT, value = true)
      .option(MessagePackOptions.DESERIALIZER_TRACEPATH, value = true)
      .load(data.write())
    val secondRow = df.collect()(1)
    assert(secondRow.get(1) === null)
  }

  test("deserializer.trace_path: true") {
    val data = new InconsistentData();
    val df = spark.read
      .format("messagepack")
      .schema(data.schema())
      .option(MessagePackOptions.DESERIALIZER_TRACEPATH, value = true)
      .load(data.write())
    val exception = intercept[SparkException] {
      df.show()
    }
    assert(exception.getCause.getCause.isInstanceOf[ValueVisitorException])
    assert(exception.getCause.getCause.getMessage === "msgpack[STRING] cannot be converted to spark[long] @ f2")
  }

  test("schema.max_sample_rows: all") {
    val df = spark.read.format("messagepack").load(COERCABLE_DATA.write())
    assert(df.schema.fields(0).dataType.isInstanceOf[DoubleType])
  }

  test("schema.max_sample_rows: 1") {
    val df =
      spark.read.format("messagepack").option(MessagePackOptions.SCHEMA_MAXSAMPLEROWS, 1).load(COERCABLE_DATA.write())
    assert(df.schema.fields(0).dataType.isInstanceOf[LongType])
  }
}
