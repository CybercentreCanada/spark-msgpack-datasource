package org.apache.spark.sql.msgpack.extensions

import java.util
import java.util.Optional

class ExtensionDeserializers extends Serializable {

  private val deserializers = new util.ArrayList[ExtensionDeserializer]()

  def has(extType: Byte): Boolean = {
    val t: Int = extType;
    has(t)
  }

  def has(extType: Int): Boolean = {
    val t: Int = extType
    deserializers.stream().anyMatch(d => d.extensionType() == t)
  }

  def get(extType: Byte): Optional[ExtensionDeserializer] = {
    val t: Int = extType
    get(t)
  }

  def get(extType: Int): Optional[ExtensionDeserializer] = {
    deserializers.stream().filter(d => d.extensionType() == extType).findFirst()
  }

  def set(deserializer: ExtensionDeserializer): ExtensionDeserializers = {
    deserializers.add(deserializer)
    this
  }

}
