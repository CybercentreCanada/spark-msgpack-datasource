package org.apache.spark.sql.msgpack.test.data.impl

import org.apache.spark.sql.msgpack.test.data.MessagePackData

class EmptyData extends MessagePackData {
  override def pack(): Unit = {}
}
