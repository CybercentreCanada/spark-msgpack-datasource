package org.apache.spark.sql.v2.msgpack

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.msgpack.MessagePackSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class MessagePackTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat]
) extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): MessagePackScanBuilder =
    new MessagePackScanBuilder(sparkSession, fileIndex, dataSchema, options)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    MessagePackSchema.inferFromFiles(sparkSession, files, caseSensitiveMap)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteBuilder {
      override def build(): Write = MessagePackWriteBuilder(paths, formatName, supportsDataType, info)
    }
  }

  // Both JsonTable and AvroTable do this.
  // Not sure which DataType this doesn't support.
  override def supportsDataType(dataType: DataType): Boolean =
    dataType match {
      case _: AtomicType => true

      case st: StructType => st.forall { f => supportsDataType(f.dataType) }

      case ArrayType(elementType, _) => supportsDataType(elementType)

      case MapType(keyType, valueType, _) =>
        supportsDataType(keyType) && supportsDataType(valueType)

      case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

      case _: NullType => true

      case _ => false
    }

  override def formatName: String = "messagepack"
}
