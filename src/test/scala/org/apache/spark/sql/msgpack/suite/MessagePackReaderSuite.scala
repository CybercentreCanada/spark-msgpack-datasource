package org.apache.spark.sql.msgpack.suite

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.msgpack.test.data.impl._
import org.apache.spark.sql.test.SharedSparkSession

class MessagePackReaderSuite extends QueryTest with SharedSparkSession {

  test("schema.inference: ensure IllegalArgumentException is thrown on NoMapRootData") {
    val message = intercept[IllegalArgumentException] {
      spark.read.format("messagepack").load(new NoMapRootData().write())
    }.getMessage
    assert(message === "Unable to infer schema.  No valid schema could be read from files.")
  }

  test("schema.inference: ensure IllegalArgumentException is thrown on MixedRootData") {
    val message = intercept[IllegalArgumentException] {
      spark.read.format("messagepack").load(new MixRootData().write())
    }.getMessage
    assert(message === "Unable to infer schema.  No valid schema could be read from files.")
  }

  test("schema.inference: ensure IllegalArgumentException is thrown on EmptyData") {
    val message = intercept[IllegalArgumentException] {
      spark.read.format("messagepack").load(new EmptyData().write())
    }.getMessage
    assert(message === "Unable to infer schema.  No valid schema could be read from files.")
  }

  test("schema.inference: ensure provided ComplexData schema matches inference.") {
    val data = new ComplexData()
    val df = spark.read.format("messagepack").load(data.write())
    assert(df.schema.equals(data.schema()))
  }

  test("Load and count OneRowData") {
    val df = spark.read
      .format("messagepack")
      .load(new OneRowData().write())
    assert(df.count() === 1)
  }

  test("schema: complex data") {
    val df = spark.read.format("messagepack").load(new ComplexData().write())
    assert(df.count() === 1)
    assert(df.schema.fields.length === 5)
  }


}
