package org.apache.spark.sql.msgpack

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.msgpack.converters.TypeDeserializer
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils
import org.msgpack.core.{MessagePack, MessageUnpacker}
import scala.collection.JavaConverters._

import java.io.InputStream
import java.util
import scala.util.{Failure, Success, Try}

object MessagePackSchema extends Logging {

  private def infer(is: InputStream, options: MessagePackOptions = MessagePackOptions()): StructType =
    infer(MessagePack.newDefaultUnpacker(is), options)

  private def infer(data: Array[Byte], options: MessagePackOptions): StructType =
    infer(MessagePack.newDefaultUnpacker(data), options)

  private def infer(data: MessageUnpacker, options: MessagePackOptions): StructType = {
    var rowNum = 0
    val schemas = Array.newBuilder[DataType]
    val iterator = new MessagePackIterator(data)
    val skipMax = options.schemaMaxSampleRows == 0
    while (iterator.hasNext && (skipMax || rowNum < options.schemaMaxSampleRows)) {
      val nextValue = iterator.next();
      schemas += TypeDeserializer.visit(nextValue).asInstanceOf[StructType]
      rowNum += 1
    }
    MessagePackCoercion.coerce(schemas.result()).asInstanceOf[StructType]
  }

  def inferFromBinary(data: Array[Array[Byte]]): StructType = {
    val schemas = data.map { raw =>
      MessagePackSchema.infer(raw, MessagePackOptions())
    }
    MessagePackCoercion.coerce(schemas).asInstanceOf[StructType]
  }

  def inferFromBinary(data: util.ArrayList[Array[Byte]]): StructType = {
    inferFromBinary(data.asScala.toArray)
  }

  def inferFromFiles(
      sparkSession: SparkSession,
      files: Seq[FileStatus],
      options: Map[String, String]
  ): Option[StructType] = {

    val conf = sparkSession.sparkContext.hadoopConfiguration
    val msgPackOptions = new MessagePackOptions(options)
    val skipDrop = msgPackOptions.schemaMaxSampleFiles == 0
    val schemas = files
      .dropRight(
        if (skipDrop || files.length <= msgPackOptions.schemaMaxSampleFiles) 0
        else files.length - msgPackOptions.schemaMaxSampleFiles
      )
      .iterator
      .map(f =>
        Utils.tryWithResource {
          MessagePackUtil.createInputStream(f.getPath, conf)
        } { is =>
          Try(MessagePackSchema.infer(is, msgPackOptions)) match {
            case Success(s) => Some(s)
            case Failure(t) =>
              logError(s"Ignoring corrupt file ${f.getPath}")
              logError(t.getMessage)
              None
          }
        }
      )
      .collect {
        case Some(r) if r.nonEmpty => r
      }
      .reduceOption { (t1, t2) =>
        MessagePackCoercion.coerce(t1, t2).asInstanceOf[StructType]
      }

    schemas match {
      case Some(schema) => Some(schema)
      case None =>
        throw new IllegalArgumentException("Unable to infer schema.  No valid schema could be read from files.")
    }
  }

}
