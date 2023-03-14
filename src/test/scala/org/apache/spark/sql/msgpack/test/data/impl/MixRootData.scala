package org.apache.spark.sql.msgpack.test.data.impl

import org.apache.spark.sql.msgpack.test.data.MessagePackData

class MixRootData extends MessagePackData {
  override def pack(): Unit = {
    packer
      .packMapHeader(2)
      .packString("k1")
      .packString("v1")
      .packString("k2")
      .packString("v2")
      .packInt(1)
      .packArrayHeader(2)
      .packBinaryHeader(3)
      .writePayload("123".getBytes)
      .packString("testing")
      .packLong(2L)
      .packMapHeader(2)
      .packString("k1")
      .packString("v1")
      .packString("k2")
      .packString("v2")

  }
}
