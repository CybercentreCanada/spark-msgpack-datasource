package org.apache.spark.sql.msgpack.test.data.impl

import org.apache.spark.sql.msgpack.test.data.MessagePackData
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class InconsistentData extends MessagePackData {

  override def schema(): StructType =
    StructType(
      Seq(
        StructField("f1", StringType, nullable = true),
        StructField("f2", LongType, nullable = true)
      )
    )

  override def pack(): Unit =
    packer
      .packMapHeader(2)
      .packString("f1")
      .packString("one")
      .packString("f2")
      .packLong(2L)
      .packMapHeader(2)
      .packString("f1")
      .packString("two")
      .packString("f2")
      .packString("inconsistent.data")

}
