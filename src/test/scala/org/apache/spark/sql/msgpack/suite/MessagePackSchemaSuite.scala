package org.apache.spark.sql.msgpack.suite

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.msgpack.MessagePackSchema
import org.apache.spark.sql.msgpack.test.data.impl._
import org.apache.spark.sql.test.SharedSparkSession

class MessagePackSchemaSuite extends QueryTest with SharedSparkSession {

  test("inferFromBinary") {
    val data = new ComplexData()
    val dataPath = data.write()
    val assert_df = spark.read.format("messagepack").load(dataPath)
    val raw_df = spark.read.format("binaryFile").load(dataPath)
    val inferredSchema = MessagePackSchema.inferFromBinary(
      raw_df.rdd.map(row => row.getAs[Array[Byte]]("content")).collect()
    )
    assert(assert_df.schema.equals(inferredSchema))
  }

}
