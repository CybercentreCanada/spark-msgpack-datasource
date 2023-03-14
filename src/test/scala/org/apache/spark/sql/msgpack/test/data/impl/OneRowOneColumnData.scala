package org.apache.spark.sql.msgpack.test.data.impl

import org.apache.spark.sql.msgpack.test.data.MessagePackData
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class OneRowOneColumnData extends MessagePackData {

  override def schema(): StructType = StructType(Seq(StructField("f1", StringType, nullable = true)))

  override def pack(): Unit = packer.packMapHeader(1).packString("f1").packString("v1")

}
