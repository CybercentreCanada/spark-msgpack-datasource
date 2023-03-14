package org.apache.spark.sql.msgpack

import org.apache.spark.sql.msgpack.extensions.{ExtensionDeserializerProvider, ExtensionDeserializers}
import org.apache.spark.sql.sources.MessagePackExtensionDeserializerProvider

import java.util.ServiceLoader

object MessagePackServiceLoader {

  private val defaultProvider = new ExtensionDeserializerProvider()

  def getExtensionDeserializers: ExtensionDeserializers = {
    val providers =
      ServiceLoader.load(classOf[MessagePackExtensionDeserializerProvider]);
    val iterator = providers.iterator()
    if (iterator.hasNext) {
      return iterator.next().get()
    }
    defaultProvider.get()
  }

}
