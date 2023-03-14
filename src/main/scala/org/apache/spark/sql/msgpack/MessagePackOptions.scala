package org.apache.spark.sql.msgpack

import org.apache.spark.sql.msgpack.MessagePackOptions._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable

object MessagePackOptions {

  val SCHEMA_MAXSAMPLEFILES = "schema.max_sample_files"

  private val SCHEMA_MAXSAMPLEFILES_DEFAULT: Int = 10

  val SCHEMA_MAXSAMPLEROWS = "schema.max_sample_rows"

  private val SCHEMA_MAXSAMPLEROWS_DEFAULT = 10000

  val DESERIALIZER_TRACEPATH = "deserializer.trace_path"

  private val DESERIALIZER_TRACEPATH_DEFAULT = false

  val DESERIALIZER_LENIENT = "deserializer.lenient"

  private val DESERIALIZER_LENIENT_DEFAULT = false

  val builder = new Builder()

  class Builder {

    private var conf = mutable.Map[String, String]();

    def setConf(conf: CaseInsensitiveStringMap): Builder = {
      this.conf = conf.asCaseSensitiveMap().asScala
      this
    }

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

    def get = new MessagePackOptions(new CaseInsensitiveStringMap(conf.asJava))
  }
}

class MessagePackOptions(options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()) extends Serializable {
  val schemaMaxSampleFiles: Int =
    options.getInt(
      SCHEMA_MAXSAMPLEFILES,
      SCHEMA_MAXSAMPLEFILES_DEFAULT
    )

  val schemaMaxSampleRows: Int =
    options.getInt(
      SCHEMA_MAXSAMPLEROWS,
      SCHEMA_MAXSAMPLEROWS_DEFAULT
    )

  val deserializerTracePath: Boolean =
    options.getBoolean(
      DESERIALIZER_TRACEPATH,
      DESERIALIZER_TRACEPATH_DEFAULT
    )

  val deserializerLenient: Boolean = options.getBoolean(DESERIALIZER_LENIENT, DESERIALIZER_LENIENT_DEFAULT)
}
