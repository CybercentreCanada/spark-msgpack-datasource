package org.apache.spark.sql.msgpack

import org.apache.spark.sql.catalyst.{DataSourceOptions, FileSourceOptions}
import org.apache.spark.sql.msgpack.MessagePackOptions.{
  DESERIALIZER_LENIENT,
  DESERIALIZER_TRACEPATH,
  SCHEMA_MAXSAMPLEFILES,
  SCHEMA_MAXSAMPLEROWS
}

import scala.collection.mutable

class MessagePackOptions(parameters: Map[String, String]) extends FileSourceOptions(parameters) {

  val schemaMaxSampleFiles: Int = parameters.get(SCHEMA_MAXSAMPLEFILES).map(_.toInt).getOrElse(10)

  val schemaMaxSampleRows: Int = parameters.get(SCHEMA_MAXSAMPLEROWS).map(_.toInt).getOrElse(10000)

  val deserializerTracePath: Boolean = parameters.get(DESERIALIZER_TRACEPATH).exists(_.toBoolean)

  val deserializerLenient: Boolean = parameters.get(DESERIALIZER_LENIENT).exists((_.toBoolean))
}

object MessagePackOptions extends DataSourceOptions {

  def apply(parameters: Map[String, String] = Map.empty): MessagePackOptions = new MessagePackOptions(parameters)

  val SCHEMA_MAXSAMPLEFILES: String = newOption("schema.max_sample_files")

  val SCHEMA_MAXSAMPLEROWS: String = newOption("schema.max_sample_rows")

  val DESERIALIZER_TRACEPATH: String = newOption("deserializer.trace_path")

  val DESERIALIZER_LENIENT: String = newOption("deserializer.lenient")

  def builder: Builder = new Builder()

  class Builder {

    private var conf = mutable.Map[String, String]();

    def setLenientDeserialization(value: Boolean): Builder = {
      conf.put(DESERIALIZER_LENIENT, value.toString)
      this
    }

    def setTracePath(value: Boolean): Builder = {
      conf.put(DESERIALIZER_TRACEPATH, value.toString)
      this
    }

    def setMaxSampleRows(value: Int): Builder = {
      conf.put(SCHEMA_MAXSAMPLEROWS, value.toString)
      this
    }

    def setMaxSampleFiles(value: Int): Builder = {
      conf.put(SCHEMA_MAXSAMPLEFILES, value.toString)
      this
    }

    def get = new MessagePackOptions(conf.map(kv => (kv._1, kv._2)).toMap)
  }

}
