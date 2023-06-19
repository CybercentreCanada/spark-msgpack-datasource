package org.apache.spark.sql.msgpack.suite

import org.apache.spark.sql.msgpack.{MessagePackOptions, MessagePackSchema}
import org.apache.spark.sql.{Encoders, QueryTest, Row}
import org.apache.spark.sql.msgpack.test.data.impl._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

class MessagePackSchemaSuite extends QueryTest with SharedSparkSession {

  test("inferFromBinary") {
    val data = new ComplexData()
    val df_schema = StructType(Seq(StructField("data", BinaryType, nullable = true)))
    val rdd = spark.sparkContext.parallelize(Seq(Row(data.getBytes)))
    val df = spark.createDataFrame(rdd, df_schema)
    val ds = df.map((row: Row) => row.getAs[Array[Byte]]("data"), Encoders.BINARY)
    val schema = MessagePackSchema.inferFromBinary(ds)
    assert(data.schema().equals(schema))
  }

}
