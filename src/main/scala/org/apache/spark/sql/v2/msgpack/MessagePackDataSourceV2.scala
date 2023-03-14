package org.apache.spark.sql.v2.msgpack

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.msgpack.MessagePackFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MessagePackDataSourceV2 extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] =
    classOf[MessagePackFileFormat]

  override def shortName(): String = "messagepack"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    MessagePackTable(
      tableName,
      sparkSession,
      options,
      paths,
      None,
      fallbackFileFormat
    )
  }

  override def getTable(
      options: CaseInsensitiveStringMap,
      schema: StructType
  ): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    MessagePackTable(
      tableName,
      sparkSession,
      options,
      paths,
      Some(schema),
      fallbackFileFormat
    )
  }

}
