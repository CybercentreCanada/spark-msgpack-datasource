package org.apache.spark.sql.msgpack.test.data.impl

import org.apache.spark.sql.msgpack.test.data.MessagePackData
import org.apache.spark.sql.types.{BinaryType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

class OneRowData extends MessagePackData {

  override def schema(): StructType =
    StructType(
      Seq(
        StructField("k1", StringType, nullable = true),
        StructField("k2", IntegerType, nullable = true),
        StructField("k3", DoubleType, nullable = true),
        StructField("k4", BinaryType, nullable = true),
        StructField("k5", StringType, nullable = true),
        StructField("k6", LongType, nullable = true),
        StructField("k7", DoubleType, nullable = true)
      )
    )

  override def pack(): Unit = {
    packer
      .packMapHeader(7)
      .packString("k1")
      .packString("v1")
      .packString("k2")
      .packInt(2)
      .packString("k3")
      .packFloat(3f)
      .packString("k4")
      .packBinaryHeader(2)
      .writePayload("v4".getBytes)
      .packString("k5")
      .packBoolean(true)
      .packString("k6")
      .packLong(6L)
      .packString("k7")
      .packDouble(7d)
  }
}
