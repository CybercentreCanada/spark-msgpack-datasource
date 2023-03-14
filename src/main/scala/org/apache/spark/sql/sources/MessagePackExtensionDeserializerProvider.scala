package org.apache.spark.sql.sources

import org.apache.spark.sql.msgpack.extensions.ExtensionDeserializers

trait MessagePackExtensionDeserializerProvider {

  def get(): ExtensionDeserializers

}
