package org.apache.spark.sql.msgpack

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.execution.datasources.CodecStreams
import org.apache.spark.sql.msgpack.visitor.ValueVisitorContext
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{SparkConf, TaskContext}
import org.msgpack.value.{ExtensionValue, Value, ValueFactory}

import java.io.InputStream
import java.nio.ByteBuffer

object MessagePackUtil extends Logging {

  def createInputStream(path: Path, config: Configuration): InputStream = {
    // This is to deal with LSP-251.  Since CodecStreams does not load JNI ZSTD Code, but will look for one
    //  that is install on OS, compatibility issues arise when both version don't match.
    // This is to ensure that we decompress zstd file the same way the MapOutputTracker class does it.
    if (path.getName.endsWith(".zst")) {
      val codec = CompressionCodec.createCodec(new SparkConf(true), "zstd")
      val fs = path.getFileSystem(config)
      val inputStream: InputStream = fs.open(path)
      Option(TaskContext.get())
        .foreach(_.addTaskCompletionListener[Unit](_ => inputStream.close()))
      codec.compressedInputStream(inputStream)
    }
    // Just fall back to regularly scheduled spark codec streams...
    else {
      CodecStreams.createInputStreamWithCloseResource(config, path)
    }
  }

  def daysToSecs(days: Int): Int = {
    days * 24 * 60 * 60
  }

  def microsToSecsAndNanos(micros: Long): (Long, Int) = {
    val secs = micros / 1000000
    val secs1000 = secs * 1000000
    val remainder = (micros % secs1000)
    val nanos = (remainder * 1000).toInt
    (secs, nanos)
  }

  def generateTimestampExtension32(secs: Int): ExtensionValue = {
    val bytes = generateTimestamp32(secs)
    ValueFactory.newExtension(-1, bytes)
  }

  def generateTimestampExtension64(secs: Long, nanos: Long): ExtensionValue = {
    ValueFactory.newExtension(-1, generateTimestamp64(secs, nanos))
  }

  def generateTimestampExtension96(secs: Long, nanos: Int): ExtensionValue = {
    ValueFactory.newExtension(-1, generateTimestamp96(secs, nanos))
  }

  def generateTimestamp32(secs: Int): Array[Byte] = {
    ByteBuffer.allocate(4).putInt(secs).array
  }

  def generateTimestamp64(secs: Long, nanos: Long): Array[Byte] = {
    ByteBuffer.allocate(8).putLong((nanos << 34) | secs).array
  }

  def generateTimestamp96(secs: Long, nanos: Int): Array[Byte] = {
    // BIG_ENDIAN -> store left-most bytes first.
    ByteBuffer.allocate(12).putInt(nanos).putLong(secs).array
  }

  def toString(str: Array[Byte]): String = {
    UTF8String.fromBytes(str).toString
  }

  def toUTF8(str: String): UTF8String = {
    UTF8String.fromString(str)
  }

  def tracePathError(value: Value, context: ValueVisitorContext): String =
    s"msgpack[${value.getValueType}] cannot be converted to spark[${context.dataType.typeName}]${if (context.options.deserializerTracePath)
      s" @ ${context.resolvePath}"
    else ""}"

}
