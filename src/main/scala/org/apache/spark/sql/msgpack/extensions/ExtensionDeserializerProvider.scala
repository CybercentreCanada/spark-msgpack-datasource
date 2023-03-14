package org.apache.spark.sql.msgpack.extensions

import org.apache.spark.sql.sources.MessagePackExtensionDeserializerProvider

class ExtensionDeserializerProvider extends MessagePackExtensionDeserializerProvider {
  override def get(): ExtensionDeserializers = {
    val deserializers = new ExtensionDeserializers();
    for (i <- 0 to 127) {
      deserializers.set(new DefaultDeserializer(i))
    }
    deserializers
  }
}
