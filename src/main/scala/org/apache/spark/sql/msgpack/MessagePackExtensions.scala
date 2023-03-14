package org.apache.spark.sql.msgpack

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.msgpack.expressions.FromMsgPack

class MessagePackExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectFunction(FromMsgPack.DESCRIPTOR)
  }
}
