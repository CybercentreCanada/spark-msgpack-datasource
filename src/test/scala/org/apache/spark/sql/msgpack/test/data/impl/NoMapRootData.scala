package org.apache.spark.sql.msgpack.test.data.impl

import org.apache.spark.sql.msgpack.test.data.MessagePackData

class NoMapRootData extends MessagePackData {
  override def pack(): Unit = {
    packer
      .packInt(1)
      .packArrayHeader(2)
      .packBinaryHeader(3)
      .writePayload("123".getBytes)
      .packString("testing")
      .packLong(2L)

  }
}
